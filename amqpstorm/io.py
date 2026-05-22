"""AMQPStorm Connection.IO."""
from __future__ import annotations

import logging
import select
import socket
import threading
import time
from errno import EAGAIN
from errno import EINTR
from errno import EWOULDBLOCK
from typing import Any
from typing import Callable

from amqpstorm import compatibility
from amqpstorm.base import MAX_FRAME_SIZE
from amqpstorm.compatibility import ssl
from amqpstorm.exception import AMQPConnectionError

EMPTY_BUFFER = b''
LOGGER = logging.getLogger(__name__)
POLL_TIMEOUT = 1.0
POLL_TIMEOUT_MS = int(POLL_TIMEOUT * 1000)
INBOUND_BACKPRESSURE_SLEEP = 0.001


class BasePoller:
    def __init__(
        self,
        fileno: int,
        exceptions: list[Exception],
        check_backpressure: Callable[[], bool] | None = None,
    ) -> None:
        self._fileno = fileno
        self._exceptions = exceptions
        self._check_backpressure = check_backpressure

    @property
    def fileno(self) -> int:
        """Socket Fileno.

        :return:
        """
        return self._fileno

    @property
    def is_ready(self) -> bool:
        raise NotImplementedError

    def close(self) -> None:
        pass

    def _should_pause(self) -> bool:
        """Back-pressure gate run before each poll.

        :rtype: bool
        """
        if self._check_backpressure and self._check_backpressure():
            time.sleep(INBOUND_BACKPRESSURE_SLEEP)
            return True
        return False


class SelectPoller(BasePoller):
    """Socket Read Poller using select.select."""

    @property
    def is_ready(self) -> bool:
        """Is Socket Ready.

        :rtype: bool
        """
        if self._should_pause():
            return False
        try:
            ready, _, _ = select.select([self.fileno], [], [], POLL_TIMEOUT)
            return bool(ready)
        except OSError as why:
            if why.errno != EINTR:
                self._exceptions.append(AMQPConnectionError(why))
        return False


class Poller(BasePoller):
    """Socket Read Poller using select.poll."""

    def __init__(
        self,
        fileno: int,
        exceptions: list[Exception],
        check_backpressure: Callable[[], bool] | None = None,
    ) -> None:
        super().__init__(fileno, exceptions, check_backpressure)
        self.poller = select.poll()  # type: ignore[attr-defined]
        self.poller.register(
            self._fileno,
            select.POLLIN | select.POLLPRI,  # type: ignore[attr-defined]
        )

    @property
    def is_ready(self) -> bool:
        """Check if the socket is ready for reading.

        :rtype: bool
        """
        if self._should_pause():
            return False
        try:
            events = self.poller.poll(POLL_TIMEOUT_MS)
            for fd, event in events:
                if fd == self.fileno:
                    return True
        except OSError as why:
            if why.errno != EINTR:
                self._exceptions.append(AMQPConnectionError(why))
        return False

    def close(self) -> None:
        """Unregister the file descriptor."""
        try:
            self.poller.unregister(self.fileno)
        except OSError:
            pass


class IO:
    """Internal Input/Output handler."""

    def __init__(
        self,
        parameters: dict[str, Any],
        exceptions: list[Exception] | None = None,
        on_read_impl: Callable[[bytes], bytes] | None = None,
        check_backpressure: Callable[[], bool] | None = None,
    ) -> None:
        self._exceptions: list[Exception] = (
            exceptions if exceptions is not None else []
        )
        self._wr_lock = threading.Lock()
        self._rd_lock = threading.Lock()
        self._inbound_thread: threading.Thread | None = None
        self._on_read_impl = on_read_impl
        self._check_backpressure = check_backpressure
        self._running = threading.Event()
        self._parameters = parameters
        self.data_in: bytes = EMPTY_BUFFER
        self.poller: BasePoller | None = None
        self.socket: socket.socket | None = None
        self.use_ssl: bool = self._parameters['ssl']
        self.poller_type: str = self._parameters['poller']

    def close(self) -> None:
        """Close Socket.

        :return:
        """
        with self._wr_lock, self._rd_lock:
            self._running.clear()
            self._close_socket()

        if self._inbound_thread:
            self._inbound_thread.join(timeout=self._parameters['timeout'])

        self.socket = None
        self.poller = None
        self._inbound_thread = None

    def open(self) -> None:
        """Open Socket and establish a connection.

        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.
        :return:
        """
        with self._wr_lock, self._rd_lock:
            self.data_in = EMPTY_BUFFER
            self._running.set()
            sock_addresses = self._get_socket_addresses()
            self.socket = self._find_address_and_connect(sock_addresses)
            if self.poller_type == 'poll':
                self.poller = Poller(self.socket.fileno(), self._exceptions,
                                     self._check_backpressure)
            else:
                self.poller = SelectPoller(self.socket.fileno(),
                                           self._exceptions,
                                           self._check_backpressure)
            self._inbound_thread = self._create_inbound_thread()

    def write_to_socket(self, frame_data: bytes) -> None:
        """Write data to the socket.

        :param str frame_data:
        :return:
        """
        with self._wr_lock:
            frame_data_view = memoryview(frame_data)
            total_bytes_written = 0
            bytes_to_send = len(frame_data)
            while total_bytes_written < bytes_to_send:
                try:
                    if not self.socket:
                        raise OSError('connection/socket error')
                    bytes_written = (
                        self.socket.send(frame_data_view[total_bytes_written:])
                    )
                    if bytes_written == 0:
                        raise OSError('connection/socket error')
                    total_bytes_written += bytes_written
                except TimeoutError:
                    pass
                except OSError as why:
                    if why.errno in (EWOULDBLOCK, EAGAIN):
                        continue
                    self._exceptions.append(AMQPConnectionError(why))
                    return

    def _close_socket(self) -> None:
        """Shutdown and close the Socket.

        :return:
        """
        if not self.socket:
            return
        try:
            if self.poller:
                self.poller.close()
            if self.use_ssl:
                self.socket.unwrap()  # type: ignore[attr-defined]
            self.socket.shutdown(socket.SHUT_RDWR)
        except (OSError, ValueError):
            pass

        self.socket.close()

    def _get_socket_addresses(self) -> list[Any]:
        """Get Socket address information.

        :rtype: list
        """
        family = socket.AF_UNSPEC
        if not socket.has_ipv6:
            family = socket.AF_INET
        try:
            addresses = socket.getaddrinfo(self._parameters['hostname'],
                                           self._parameters['port'], family,
                                           socket.SOCK_STREAM)
        except socket.gaierror as why:
            raise AMQPConnectionError(why)
        return addresses

    def _find_address_and_connect(self, addresses: list[Any]) -> socket.socket:
        """Find and connect to the appropriate address.

        :param addresses:

        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :rtype: socket.socket
        """
        error_message = None
        for address in addresses:
            sock = self._create_socket(socket_family=address[0])
            try:
                sock.connect(address[4])
            except OSError as why:
                error_message = why.strerror
                continue
            return sock
        host = self._parameters['hostname']
        port = self._parameters['port']
        raise AMQPConnectionError(
            f'Could not connect to {host}:{port} error: {error_message}'
        )

    def _create_socket(self, socket_family: int) -> socket.socket:
        """Create Socket.

        :param int socket_family:
        :rtype: socket.socket
        """
        sock = socket.socket(socket_family, socket.SOCK_STREAM, 0)
        sock.settimeout(self._parameters['timeout'] or None)
        if self.use_ssl:
            if not compatibility.SSL_SUPPORTED:
                raise AMQPConnectionError(
                    'Python not compiled with support for TLSv1 or higher'
                )
            sock = self._ssl_wrap_socket(sock)
        return sock

    def _ssl_wrap_socket(self, sock: socket.socket) -> Any:
        """Wrap SSLSocket around the Socket.

        :param socket.socket sock:
        :rtype: SSLSocket
        """
        ssl_options = self._parameters.get('ssl_options', {})

        context = ssl_options.get('context')
        server_hostname = (
            ssl_options.get('server_hostname') or self._parameters['hostname']
        )
        if context is not None:
            return context.wrap_socket(
                sock, do_handshake_on_connect=True,
                server_hostname=server_hostname
            )

        context = ssl.create_default_context()

        verify_mode = ssl_options.get('verify_mode', ssl_options.get('cert_reqs'))
        if verify_mode is not None:
            new_mode = None
            if isinstance(verify_mode, ssl.VerifyMode):
                new_mode = verify_mode
            elif verify_mode.lower() == 'required':
                new_mode = ssl.CERT_REQUIRED
            elif verify_mode.lower() == 'optional':
                new_mode = ssl.CERT_OPTIONAL
            elif verify_mode.lower() == 'none':
                new_mode = ssl.CERT_NONE
            if new_mode is not None:
                if new_mode == ssl.CERT_NONE:
                    context.check_hostname = False
                context.verify_mode = new_mode

        check_hostname = ssl_options.get('check_hostname')
        if check_hostname is not None:
            if isinstance(check_hostname, bool):
                context.check_hostname = check_hostname
            else:
                context.check_hostname = str(check_hostname).strip().lower() in (
                    'true', '1', 'yes', 'y', 'on'
                )

        context.load_default_certs()
        ca_certs = ssl_options.get('ca_certs', ssl_options.get('cafile'))
        if ca_certs:
            context.load_verify_locations(cafile=ca_certs)
        certfile = ssl_options.get('certfile')
        keyfile = ssl_options.get('keyfile')
        if certfile or keyfile:
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)

        return context.wrap_socket(sock, do_handshake_on_connect=True,
                                   server_hostname=server_hostname)

    def _create_inbound_thread(self) -> threading.Thread:
        """Internal Thread that handles all incoming traffic.

        :rtype: threading.Thread
        """
        inbound_thread = threading.Thread(target=self._process_incoming_data,
                                          name=__name__)
        inbound_thread.daemon = True
        inbound_thread.start()
        return inbound_thread

    def _process_incoming_data(self) -> None:
        """Retrieve and process any incoming data.

        :return:
        """
        try:
            while self._running.is_set():
                if self.poller.is_ready:
                    self.data_in += self._receive()
                    self.data_in = self._on_read_impl(self.data_in)
        except Exception as why:
            self._exceptions.append(AMQPConnectionError(why))
            if self._running.is_set():
                LOGGER.warning(
                    'Stopping inbound thread due to %s', why,
                    exc_info=True,
                )
            self._running.clear()

    def _receive(self) -> bytes:
        """Receive any incoming socket data.

            If an error is thrown, handle it and return an empty string.

        :return: data_in
        :rtype: bytes
        """
        data_in = EMPTY_BUFFER
        try:
            data_in = self._read_from_socket()
            if not data_in:
                raise OSError('connection closed by server')
        except TimeoutError:
            pass
        except compatibility.SSLWantReadError:
            # NOTE(visobet): Retry if the non-blocking socket does not
            # have any meaningful data ready.
            pass
        except OSError as why:
            if why.errno not in (EWOULDBLOCK, EAGAIN):
                self._exceptions.append(AMQPConnectionError(why))
                if self._running.is_set():
                    LOGGER.warning(
                        'Stopping inbound thread due to %s', why,
                        exc_info=True,
                    )
                self._running.clear()
        return data_in

    def _read_from_socket(self) -> bytes:
        """Read data from the socket.

        :rtype: bytes
        """
        if not self.use_ssl:
            if not self.socket:
                raise OSError('connection/socket error')
            return self.socket.recv(MAX_FRAME_SIZE)

        with self._rd_lock:
            if not self.socket:
                raise OSError('connection/socket error')
            return self.socket.read(MAX_FRAME_SIZE)  # type: ignore[attr-defined]
