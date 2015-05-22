"""AMQP-Storm IO."""
__author__ = 'eandersson'

import ssl
import select
import socket
import logging
import threading
from time import sleep
from errno import EINTR
from errno import EWOULDBLOCK

from pamqp import frame as pamqp_frame
from pamqp import specification as pamqp_spec

from amqpstorm.base import Stateful
from amqpstorm.base import IDLE_WAIT
from amqpstorm.base import FRAME_MAX
from amqpstorm.exception import AMQPConnectionError


EMPTY_BUFFER = bytes()
LOGGER = logging.getLogger(__name__)


class Poller(object):
    """Socket Read/Write Poller."""

    def __init__(self, fileno, timeout=10):
        self._fileno = fileno
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
            ready, write, _ = select.select([self.fileno], [self.fileno], [],
                                            self.timeout)
            return bool(ready), bool(write)
        except select.error as why:
            if why.args[0] != EINTR:
                raise


class IO(Stateful):
    socket = None
    poller = None
    buffer = EMPTY_BUFFER

    def __init__(self, parameters, on_read=None, on_error=None):
        super(IO, self).__init__()
        self.parameters = parameters
        self.on_read = on_read
        self.on_error = on_error

    def open(self, hostname, port):
        """Open Socket and establish a connection.

        :param str hostname:
        :param int port:
        :return:
        """
        self.set_state(self.OPENING)
        sock_address_tuple = self._get_socket_address(hostname, port)
        sock = self._create_socket(socket_family=sock_address_tuple[0])
        if self.parameters['ssl']:
            sock = self._ssl_wrap_socket(sock)
        try:
            sock.connect(sock_address_tuple[4])
        except (socket.error, ssl.SSLError) as why:
            raise AMQPConnectionError(why)
        self.socket = sock
        self.poller = Poller(self.socket.fileno())
        self._create_inbound_thread()
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
        self.socket.close()
        self.socket = None
        self.set_state(self.CLOSED)

    def write_frame(self, channel_id, frame_out):
        """Marshal and write an outgoing pamqp frame to the socket.

        :param int channel_id:
        :param pamqp_spec.Frame frame_out: Amqp frame.
        :return:
        """
        frame_data = pamqp_frame.marshal(frame_out, channel_id)
        self.write_to_socket(frame_data)

    def write_multiple_frames(self, channel_id, multiple_frames):
        """Marshal and write multiple outgoing pamqp frames to the socket.

        :param int channel_id:
        :param list multiple_frames: Amqp frames.
        :return:
        """
        frame_data = EMPTY_BUFFER
        for single_frame in multiple_frames:
            frame_data += pamqp_frame.marshal(single_frame, channel_id)
        self.write_to_socket(frame_data)

    def write_to_socket(self, frame_data):
        """Write data to the socket.

        :param str frame_data:
        :return:
        """
        while not self.poller.is_ready[1]:
            sleep(0.001)
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

    @staticmethod
    def _get_socket_address(hostname, port):
        """Get Socket address information.

        :param str hostname:
        :param int port:
        :rtype: tuple
        """
        try:
            addresses = socket.getaddrinfo(hostname, port)
        except socket.gaierror as why:
            raise AMQPConnectionError(why)
        result = None
        for address in addresses:
            if not address:
                continue
            result = address
            break
        return result

    def _create_socket(self, socket_family):
        """Create Socket.

        :param int family:
        :return:
        """
        sock = socket.socket(socket_family, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setblocking(0)
        sock.settimeout(self.parameters['timeout'] or None)
        return sock

    def _ssl_wrap_socket(self, sock):
        """Wrap SSLSocket around the socket.

        :param socket sock:
        :return:
        """
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
            if self.poller.is_ready[0]:
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
