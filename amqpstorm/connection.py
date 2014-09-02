"""AMQP-Storm Connection"""
__author__ = 'eandersson'

import ssl
import socket
import select
import logging
import threading
from time import sleep
from errno import EINTR
from errno import EWOULDBLOCK

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from pamqp import frame as pamqp_frame
from pamqp import header as pamqp_header
from pamqp import specification as pamqp_spec
from pamqp import exceptions as pamqp_exception

from amqpstorm import comaptibility
from amqpstorm.base import Stateful
from amqpstorm.base import IDLE_WAIT
from amqpstorm.base import FRAME_MAX
from amqpstorm.channel import Channel
from amqpstorm.channel0 import Channel0
from amqpstorm.exception import AMQPError
from amqpstorm.exception import AMQPConnectionError


EMPTY_BUFFER = bytes()
LOGGER = logging.getLogger(__name__)


class Poller(object):
    """Socket Read/Write Poller"""

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
        """Is Poller Ready.

        :rtype: tuple
        """
        try:
            ready, write, _ = select.select([self.fileno], [self.fileno], [],
                                            self.timeout)
            return bool(ready), bool(write)
        except select.error as why:
            if why.args[0] != EINTR:
                raise


class Connection(Stateful):
    """RabbitMQ Connection Class"""
    lock = threading.Lock()
    _buffer = EMPTY_BUFFER
    _channel0 = None
    _io_thread = None
    _poller = None
    _socket = None

    def __init__(self, hostname, username, password, port=5672, **kwargs):
        """Create a new instance of the Connection class.

        :param str hostname:
        :param str username:
        :param str password:
        :param int port:
        :param str virtual_host:
        :param int heartbeat: RabbitMQ Heartbeat interval
        :param int|float timeout: Socket timeout
        :param bool ssl: Enable SSL
        :param dict ssl_options: SSL Kwargs
        :return:
        """
        super(Connection, self).__init__()
        self.parameters = {
            'hostname': hostname,
            'username': username,
            'password': password,
            'port': port,
            'virtual_host': kwargs.get('virtual_host', '/'),
            'heartbeat': int(kwargs.get('heartbeat', 60)),
            'timeout': kwargs.get('timeout', 0),
            'ssl': kwargs.get('ssl', False),
            'ssl_options': kwargs.get('ssl_options', {})
        }
        self._channels = {}
        self._validate_parameters()
        self.open()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, _):
        if exception_value:
            msg = 'Closing connection due to an unhandled exception: {0}'
            LOGGER.error(msg.format(exception_type))
        self.close()

    @property
    def socket(self):
        """Returns an instance of the socket.

        :return:
        """
        return self._socket

    def open(self):
        """Open Connection."""
        LOGGER.debug('Connection Opening.')
        self._buffer = EMPTY_BUFFER
        self._exceptions = []
        self.set_state(self.OPENING)
        self._socket, error = self._open_socket(self.parameters['hostname'],
                                                self.parameters['port'])
        if error:
            raise AMQPConnectionError(error)
        self._poller = Poller(self._socket.fileno())
        self._channel0 = Channel0(self)
        self._send_handshake()
        self._io_thread = self._create_inbound_thread()
        while not self.is_open:
            self.check_for_errors()
            sleep(IDLE_WAIT)
        LOGGER.debug('Connection Opened.')

    def close(self):
        """Close connection."""
        if self.socket:
            self.set_state(self.CLOSING)
            self._close_channels()
            self._channel0.send_close_connection_frame()
        self.set_state(self.CLOSED)

    def channel(self):
        """Open Channel."""
        with self.lock:
            channel_id = len(self._channels) + 1
            channel = Channel(channel_id, self)
            self._channels[channel_id] = channel
            channel.open()
            while not channel.is_open and self.is_open:
                sleep(IDLE_WAIT)
        return self._channels[channel_id]

    def write_frame(self, channel_id, frame_out):
        """Marshal and write a outgoing frame to the socket.

        :param int channel_id:
        :param pamqp_spec.Frame frame_out: Amqp frame.
        :return:
        """
        frame_data = pamqp_frame.marshal(frame_out, channel_id)
        self._write_to_socket(frame_data)

    def write_frames(self, channel_id, frames_out):
        """Marshal and write any outgoing frames to the socket.

        :param int channel_id:
        :param list frames_out: Amqp frames.
        :return:
        """
        frame_data = EMPTY_BUFFER
        for single_frame in frames_out:
            frame_data += pamqp_frame.marshal(single_frame, channel_id)
        self._write_to_socket(frame_data)

    def check_for_errors(self):
        """Check connection for potential errors.

        :return:
        """
        if not self._socket:
            self._handle_socket_error('socket/connection closed')
        super(Connection, self).check_for_errors()

    def _validate_parameters(self):
        """Validate Connection Parameters.

        :return:
        """
        if not comaptibility.is_string(self.parameters['hostname']):
            raise AMQPError('hostname should be a string')
        elif not isinstance(self.parameters['port'], int):
            raise AMQPError('port should be an int')
        elif not comaptibility.is_string(self.parameters['username']):
            raise AMQPError('username should be a string')
        elif not comaptibility.is_string(self.parameters['password']):
            raise AMQPError('password should be a string')
        elif not comaptibility.is_string(self.parameters['virtual_host']):
            raise AMQPError('virtual_host should be a string')
        elif not isinstance(self.parameters['timeout'], (int, float)):
            raise AMQPError('timeout should be an int or float')

    def _open_socket(self, hostname, port, keep_alive=1, no_delay=0):
        """Open Socket and establish a connection.

        :param str hostname:
        :param int port:
        :param keep_alive:
        :param no_delay:
        :return:
        """
        try:
            addresses = socket.getaddrinfo(hostname, port)
        except socket.gaierror as why:
            return None, why
        sock_addr_tuple = None
        for sock_addr in addresses:
            if sock_addr:
                sock_addr_tuple = sock_addr
                break
        sock = socket.socket(sock_addr_tuple[0], socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, no_delay)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, keep_alive)
        sock.setblocking(0)
        sock.settimeout(self.parameters['timeout'] or None)

        if self.parameters['ssl']:
            sock = self._ssl_wrap_socket(sock)

        try:
            sock.connect(sock_addr_tuple[4])
        except (socket.error, ssl.SSLError) as why:
            LOGGER.error(why, exc_info=False)
            return None, why
        return sock, None

    def _ssl_wrap_socket(self, sock):
        """Wrap socket to add SSL.

        :param socket sock:
        :return:
        """
        return ssl.wrap_socket(sock, do_handshake_on_connect=True,
                               **self.parameters['ssl_options'])

    def _send_handshake(self):
        """Send RabbitMQ Handshake.

        :return:
        """
        self._write_to_socket(pamqp_header.ProtocolHeader().marshal())

    def _create_inbound_thread(self):
        """Internal Thread that handles all incoming traffic.

        :rtype: threading.Thread
        """
        io_thread = threading.Thread(target=self._process_incoming_data,
                                     name=__name__)
        io_thread.setDaemon(True)
        io_thread.start()
        return io_thread

    @property
    def _poll_is_ready(self):
        """Wrapper around ReadPoller.is_ready to ensure that
            error messages are handled properly.

        :type: bool, bool
        """
        try:
            return self._poller.is_ready
        except select.error as why:
            self.exceptions.append(AMQPConnectionError(why))
            return True, True

    def _process_incoming_data(self):
        """Retrieve and process any incoming data.

        :return:
        """
        while not self.is_closed:
            if self.is_closing:
                break
            if self._poll_is_ready[0]:
                self._buffer += self._receive()
                self._read_buffer()
            sleep(IDLE_WAIT)

    def _read_buffer(self):
        """Process the socket buffer, and direct the data to the correct
            channel.

        :return:
        """
        while self._buffer:
            self._buffer, channel_id, frame_in = self._handle_amqp_frame(
                self._buffer)

            if frame_in is None:
                break

            if channel_id == 0:
                self._channel0.on_frame(channel_id, frame_in)
            else:
                self._channels[channel_id].on_frame(frame_in)

    @staticmethod
    def _handle_amqp_frame(data_in):
        """Unmarshal any incoming RabbitMQ frames and return the result.

        :param data_in: socket data
        :return: buffer, channel_id, frame
        """
        if not data_in:
            return data_in, None, None
        try:
            byte_count, channel_id, frame_in = pamqp_frame.unmarshal(data_in)
            return data_in[byte_count:], channel_id, frame_in
        except pamqp_exception.UnmarshalingException:
            return data_in, None, None
        except pamqp_spec.AMQPFrameError as why:
            LOGGER.error('AMQPFrameError: %r', why, exc_info=True)
            return data_in, None, None

    def _close_channels(self):
        """Close any open channels.

        :return:
        """
        for channel_id in self._channels:
            self._channels[channel_id].close()

    def _handle_socket_error(self, why):
        """Handle any socket errors. If requested we will try to
            re-establish the connection.

        :param exception why:
        :return:
        """
        LOGGER.debug(why, exc_info=False)
        self._close_channels()
        if self.socket:
            self.socket.close()
        self.set_state(self.CLOSED)
        self._exceptions.append(AMQPConnectionError(why))

    def _receive(self):
        """Receive any incoming socket data.

            If an error is thrown, handle it and return an empty string.

        :return: buffer
        :rtype: str
        """
        result = EMPTY_BUFFER
        try:
            result = self._socket.recv(FRAME_MAX)
        except socket.timeout:
            pass
        except (socket.error, AttributeError) as why:
            self._handle_socket_error(why)
        return result

    def _write_to_socket(self, frame_data):
        """Write data to the socket.

        :param str frame_data:
        :return:
        """
        while not self._poll_is_ready[1]:
            sleep(0.001)
        to_send = len(frame_data)
        bytes_written = 0
        while bytes_written < to_send:
            try:
                result = self.socket.send(frame_data[bytes_written:])
                if result == 0:
                    raise AMQPConnectionError('connection/socket error')
                bytes_written += result
            except socket.timeout:
                pass
            except socket.error as why:
                if why.args[0] == EWOULDBLOCK:
                    continue
                self._handle_socket_error(why)
                break
        return bytes_written


class UriConnection(Connection):
    """Wrapper of the Connection class that takes the AMQP uri schema"""

    def __init__(self, uri):
        """Create a new instance of the Connection class using
            an AMQP Uri string.

            e.g.
                amqp://guest:guest@localhost:5672/%2F
                amqps://guest:guest@localhost:5671/%2F

        :param str uri: AMQP Connection string
        """
        parsed = urlparse.urlparse(uri)
        use_ssl = parsed.scheme == 'amqps'
        hostname = parsed.hostname or 'localhost'
        port = parsed.port or 5672
        username = parsed.username or 'guest'
        password = parsed.password or 'guest'
        virtual_host = urlparse.unquote(parsed.path[1:]) or '/'
        kwargs = urlparse.parse_qs(parsed.query)
        heartbeat = kwargs.get('heartbeat', [60])
        timeout = kwargs.get('timeout', [0])

        super(UriConnection, self).__init__(hostname, username,
                                            password, port,
                                            virtual_host=virtual_host,
                                            heartbeat=int(heartbeat[0]),
                                            timeout=int(timeout[0]),
                                            ssl=use_ssl)
