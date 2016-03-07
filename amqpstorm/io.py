"""AMQP-Storm IO."""
__author__ = 'eandersson'

import select
import socket
import logging
import threading
from time import sleep
from errno import EINTR
from errno import EWOULDBLOCK

from amqpstorm import compatibility
from amqpstorm.base import Stateful
from amqpstorm.base import IDLE_WAIT
from amqpstorm.base import FRAME_MAX
from amqpstorm.exception import AMQPConnectionError

try:
    import ssl
except ImportError:
    ssl = None

EMPTY_BUFFER = bytes()
LOGGER = logging.getLogger(__name__)


class Poller(object):
    """Socket Read/Write Poller."""

    def __init__(self, fileno, timeout=30, on_error=None):
        self._fileno = fileno
        self.timeout = timeout
        self.on_error = on_error

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
            ready, _, _ = select.select([self.fileno], [], [],
                                            self.timeout)
            return bool(ready)
        except select.error as why:
            if why.args[0] != EINTR:
                self.on_error(why)
        return False, False


class IO(Stateful):
    def __init__(self, parameters, on_read=None, on_error=None):
        super(IO, self).__init__()
        self.lock = threading.Lock()
        self.socket = None
        self.poller = None
        self.inbound_thread = None
        self.buffer = EMPTY_BUFFER
        self.parameters = parameters
        self.on_read = on_read
        self.on_error = on_error

    def open(self):
        """Open Socket and establish a connection.

        :param str hostname:
        :param int port:
        :raises AMQPConnectionError: If a connection cannot be established on
                                     the specified address, raise an exception.
        :return:
        """
        self.buffer = EMPTY_BUFFER
        self.set_state(self.OPENING)
        sock_addresses = self._get_socket_addresses()
        self.socket = self._find_address_and_connect(sock_addresses)
        self.poller = Poller(self.socket.fileno(), on_error=self.on_error,
                             timeout=self.parameters['timeout'])
        self.inbound_thread = self._create_inbound_thread()
        self.set_state(self.OPEN)

    def close(self):
        """Close Socket.

        :return:
        """
        self.set_state(self.CLOSING)
        if not self.socket:
            return
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except socket.error:
            pass
        self.inbound_thread = None
        self.poller = None
        self.socket.close()
        self.socket = None
        self.set_state(self.CLOSED)

    def write_to_socket(self, frame_data):
        """Write data to the socket.

        :param str frame_data:
        :return:
        """
        total_bytes_written = 0
        bytes_to_send = len(frame_data)
        while total_bytes_written < bytes_to_send:
            try:
                bytes_written = \
                    self.socket.send(frame_data[total_bytes_written:])
                if bytes_written == 0:
                    raise socket.error('connection/socket error')
                total_bytes_written += bytes_written
            except socket.timeout:
                pass
            except socket.error as why:
                if why.args[0] == EWOULDBLOCK:
                    continue
                self.on_error(why)
                break
        return total_bytes_written

    def _get_socket_addresses(self):
        """Get Socket address information.

        :rtype: list
        """
        family = socket.AF_UNSPEC
        if not socket.has_ipv6:
            family = socket.AF_INET
        try:
            addresses = socket.getaddrinfo(self.parameters['hostname'],
                                           self.parameters['port'], family)
        except socket.gaierror as why:
            raise AMQPConnectionError(why)
        return addresses

    def _find_address_and_connect(self, addresses):
        """Find and connect to the appropriate address.

        :param addresses:
        :raises AMQPConnectionError: If no appropriate address can be found,
                                     raise an exception.
        :return:
        """
        for address in addresses:
            sock = self._create_socket(socket_family=address[0])
            try:
                sock.connect(address[4])
            except (socket.error, OSError):
                continue
            return sock
        raise AMQPConnectionError('Could not connect to %s:%d'
                                  % (self.parameters['hostname'],
                                     self.parameters['port']))

    def _create_socket(self, socket_family):
        """Create Socket.

        :param int family:
        :return:
        """
        sock = socket.socket(socket_family, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.settimeout(self.parameters['timeout'] or None)
        if self.parameters['ssl']:
            if not compatibility.SSL_SUPPORTED:
                raise AMQPConnectionError('Python not compiled with support '
                                          'for TLSv1 or higher')
            sock = self._ssl_wrap_socket(sock)
        return sock

    def _ssl_wrap_socket(self, sock):
        """Wrap SSLSocket around the socket.

        :param socket sock:
        :rtype: SSLSocket
        """
        if 'ssl_version' not in self.parameters['ssl_options']:
            self.parameters['ssl_options']['ssl_version'] = \
                compatibility.DEFAULT_SSL_VERSION
        return ssl.wrap_socket(sock, do_handshake_on_connect=True,
                               **self.parameters['ssl_options'])

    def _create_inbound_thread(self):
        """Internal Thread that handles all incoming traffic.

        :rtype: threading.Thread
        """
        inbound_thread = threading.Thread(target=self._process_incoming_data,
                                          name=__name__)
        inbound_thread.setDaemon(True)
        inbound_thread.start()
        return inbound_thread

    def _process_incoming_data(self):
        """Retrieve and process any incoming data.

        :return:
        """
        while not self.is_closed:
            if self.is_closing:
                break
            if self.poller and self.poller.is_ready:
                self.buffer += self._receive()
                self.buffer = self.on_read(self.buffer)
            sleep(IDLE_WAIT)

    def _receive(self):
        """Receive any incoming socket data.

            If an error is thrown, handle it and return an empty string.

        :return: buffer
        :rtype: str
        """
        result = EMPTY_BUFFER
        try:
            result = self.socket.recv(FRAME_MAX)
        except socket.timeout:
            pass
        except (socket.error, AttributeError) as why:
            self.on_error(why)
        return result
