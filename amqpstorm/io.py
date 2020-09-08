"""AMQPStorm Connection.IO."""

import logging
import select
import socket
import threading
from errno import EAGAIN
from errno import EINTR
from errno import EWOULDBLOCK

from amqpstorm import compatibility
from amqpstorm.base import MAX_FRAME_SIZE
from amqpstorm.compatibility import ssl
from amqpstorm.exception import AMQPConnectionError

EMPTY_BUFFER = bytes()
LOGGER = logging.getLogger(__name__)
POLL_TIMEOUT = 1.0


class Poller(object):
    """Socket Read Poller."""

    def __init__(self, fileno, exceptions, timeout=5):
        self.select = select
        self._fileno = fileno
        self._exceptions = exceptions
        self.timeout = timeout

    @property
    def fileno(self):
        """Socket Fileno.

        :return:
        """
        return self._fileno

    @property
    def is_ready(self):
        """Is Socket Ready.

        :rtype: tuple
        """
        try:
            ready, _, _ = self.select.select([self.fileno], [], [],
                                             POLL_TIMEOUT)
            return bool(ready)
        except self.select.error as why:
            if why.args[0] != EINTR:
                self._exceptions.append(AMQPConnectionError(why))
        return False


class IO(object):
    """Internal Input/Output handler."""

    def __init__(self, parameters, exceptions=None, on_read_impl=None):
        self._exceptions = exceptions
        self._wr_lock = threading.Lock()
        self._rd_lock = threading.Lock()
        self._inbound_thread = None
        self._on_read_impl = on_read_impl
        self._running = threading.Event()
        self._parameters = parameters
        self.data_in = EMPTY_BUFFER
        self.poller = None
        self.socket = None
        self.use_ssl = self._parameters['ssl']

    def close(self):
        """Close Socket.

        :return:
        """
        self._wr_lock.acquire()
        self._rd_lock.acquire()
        try:
            self._running.clear()
            self._close_socket()
            self.socket = None
            self.poller = None
        finally:
            self._wr_lock.release()
            self._rd_lock.release()

        if self._inbound_thread:
            self._inbound_thread.join(timeout=self._parameters['timeout'])
        self._inbound_thread = None

    def open(self):
        """Open Socket and establish a connection.

        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.
        :return:
        """
        self._wr_lock.acquire()
        self._rd_lock.acquire()
        try:
            self.data_in = EMPTY_BUFFER
            self._running.set()
            sock_addresses = self._get_socket_addresses()
            self.socket = self._find_address_and_connect(sock_addresses)
            self.poller = Poller(self.socket.fileno(), self._exceptions,
                                 timeout=self._parameters['timeout'])
            self._inbound_thread = self._create_inbound_thread()
        finally:
            self._wr_lock.release()
            self._rd_lock.release()

    def write_to_socket(self, frame_data):
        """Write data to the socket.

        :param str frame_data:
        :return:
        """
        self._wr_lock.acquire()
        try:
            total_bytes_written = 0
            bytes_to_send = len(frame_data)
            while total_bytes_written < bytes_to_send:
                try:
                    if not self.socket:
                        raise socket.error('connection/socket error')
                    bytes_written = (
                        self.socket.send(frame_data[total_bytes_written:])
                    )
                    if bytes_written == 0:
                        raise socket.error('connection/socket error')
                    total_bytes_written += bytes_written
                except socket.timeout:
                    pass
                except socket.error as why:
                    if why.args[0] in (EWOULDBLOCK, EAGAIN):
                        continue
                    self._exceptions.append(AMQPConnectionError(why))
                    return
        finally:
            self._wr_lock.release()

    def _close_socket(self):
        """Shutdown and close the Socket.

        :return:
        """
        if not self.socket:
            return
        try:
            if self.use_ssl:
                self.socket.unwrap()
            self.socket.shutdown(socket.SHUT_RDWR)
        except (OSError, socket.error, ValueError):
            pass

        self.socket.close()

    def _get_socket_addresses(self):
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

    def _find_address_and_connect(self, addresses):
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
            except (IOError, OSError) as why:
                error_message = why.strerror
                continue
            return sock
        raise AMQPConnectionError(
            'Could not connect to %s:%d error: %s' % (
                self._parameters['hostname'], self._parameters['port'],
                error_message
            )
        )

    def _create_socket(self, socket_family):
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

    def _ssl_wrap_socket(self, sock):
        """Wrap SSLSocket around the Socket.

        :param socket.socket sock:
        :rtype: SSLSocket
        """
        context = self._parameters['ssl_options'].get('context')
        if context is not None:
            hostname = self._parameters['ssl_options'].get('server_hostname')
            return context.wrap_socket(
                sock, do_handshake_on_connect=True,
                server_hostname=hostname
            )
        hostname = self._parameters['hostname']
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        mode = self._parameters['ssl_options'].get('verify_mode', 'none')
        if mode.lower() == 'required':
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            context.verify_mode = ssl.CERT_NONE
        check = self._parameters['ssl_options'].get('check_hostname', False)
        context.check_hostname = check
        context.load_default_certs()
        return context.wrap_socket(sock, do_handshake_on_connect=True,
                                   server_hostname=hostname)

    def _create_inbound_thread(self):
        """Internal Thread that handles all incoming traffic.

        :rtype: threading.Thread
        """
        inbound_thread = threading.Thread(target=self._process_incoming_data,
                                          name=__name__)
        inbound_thread.daemon = True
        inbound_thread.start()
        return inbound_thread

    def _process_incoming_data(self):
        """Retrieve and process any incoming data.

        :return:
        """
        while self._running.is_set():
            if self.poller.is_ready:
                self.data_in += self._receive()
                self.data_in = self._on_read_impl(self.data_in)

    def _receive(self):
        """Receive any incoming socket data.

            If an error is thrown, handle it and return an empty string.

        :return: data_in
        :rtype: bytes
        """
        data_in = EMPTY_BUFFER
        try:
            data_in = self._read_from_socket()
        except socket.timeout:
            pass
        except compatibility.SSLWantReadError:
            # NOTE(visobet): Retry if the non-blocking socket does not
            # have any meaningful data ready.
            pass
        except (IOError, OSError) as why:
            if why.args[0] not in (EWOULDBLOCK, EAGAIN):
                self._exceptions.append(AMQPConnectionError(why))
                if self._running.is_set():
                    LOGGER.warning("Stopping inbound thread due to %s", why)
                self._running.clear()
        return data_in

    def _read_from_socket(self):
        """Read data from the socket.

        :rtype: bytes
        """
        if not self.use_ssl:
            if not self.socket:
                raise socket.error('connection/socket error')
            return self.socket.recv(MAX_FRAME_SIZE)

        with self._rd_lock:
            if not self.socket:
                raise socket.error('connection/socket error')
            return self.socket.read(MAX_FRAME_SIZE)
