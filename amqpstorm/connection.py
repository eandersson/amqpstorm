"""AMQP-Storm Connection."""
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

from amqpstorm import compatibility
from amqpstorm.base import Stateful
from amqpstorm.base import IDLE_WAIT
from amqpstorm.base import FRAME_MAX
from amqpstorm.channel import Channel
from amqpstorm.channel0 import Channel0
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.exception import AMQPInvalidArgument


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


class Connection(Stateful):
    """RabbitMQ Connection Class."""
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
            'heartbeat': kwargs.get('heartbeat', 60),
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
            message = 'Closing connection due to an unhandled exception: {0!s}'
            LOGGER.warning(message.format(exception_type))
        self.close()

    @property
    def is_blocked(self):
        """Is the connection currently being blocked from publishing by
        the remote server.

        :rtype: bool
        """
        return self._channel0.is_blocked

    @property
    def server_properties(self):
        """Returns the RabbitMQ Server properties.

        :rtype: dict
        """
        return self._channel0.server_properties

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
        LOGGER.debug('Connection Closing.')
        if not self.is_closed and self.socket:
            self._close_channels()
            self.set_state(self.CLOSING)
            self._channel0.send_close_connection_frame()
        self._close_socket()
        self.set_state(self.CLOSED)
        LOGGER.debug('Connection Closed.')

    def channel(self, rpc_timeout=360):
        """Open Channel."""
        LOGGER.debug('Opening new Channel.')
        if not isinstance(rpc_timeout, int):
            raise AMQPInvalidArgument('rpc_timeout should be an integer')

        with self.lock:
            channel_id = len(self._channels) + 1
            channel = Channel(channel_id, self, rpc_timeout)
            self._channels[channel_id] = channel
            channel.open()
        LOGGER.debug('Channel #%s Opened.', channel_id)
        return self._channels[channel_id]

    def write_frame(self, channel_id, frame_out):
        """Marshal and write an outgoing pamqp frame to the socket.

        :param int channel_id:
        :param pamqp_spec.Frame frame_out: Amqp frame.
        :return:
        """
        frame_data = pamqp_frame.marshal(frame_out, channel_id)
        self._write_to_socket(frame_data)

    def write_frames(self, channel_id, frames_out):
        """Marshal and write any outgoing pamqp frames to the socket.

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
        if not compatibility.is_string(self.parameters['hostname']):
            raise AMQPInvalidArgument('hostname should be a string')
        if not isinstance(self.parameters['port'], int):
            raise AMQPInvalidArgument('port should be an integer')
        if not compatibility.is_string(self.parameters['username']):
            raise AMQPInvalidArgument('username should be a string')
        if not compatibility.is_string(self.parameters['password']):
            raise AMQPInvalidArgument('password should be a string')
        if not compatibility.is_string(self.parameters['virtual_host']):
            raise AMQPInvalidArgument('virtual_host should be a string')
        if not isinstance(self.parameters['timeout'], (int, float)):
            raise AMQPInvalidArgument('timeout should be an integer or float')
        if not isinstance(self.parameters['heartbeat'], int):
            raise AMQPInvalidArgument('heartbeat should be an integer')

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
        sock_address_tuple = None
        for sock_addr in addresses:
            if not sock_addr:
                continue
            sock_address_tuple = sock_addr
            break
        sock = socket.socket(sock_address_tuple[0], socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, no_delay)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, keep_alive)
        sock.setblocking(0)
        sock.settimeout(self.parameters['timeout'] or None)

        if self.parameters['ssl']:
            sock = self._ssl_wrap_socket(sock)

        try:
            sock.connect(sock_address_tuple[4])
        except (socket.error, ssl.SSLError) as why:
            LOGGER.error(why, exc_info=False)
            return None, why
        return sock, None

    def _ssl_wrap_socket(self, sock):
        """Wrap SSLSocket around the socket.

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

    def _process_incoming_data(self):
        """Retrieve and process any incoming data.

        :return:
        """
        while not self.is_closed:
            if self.is_closing:
                break
            if self._poller.is_ready[0]:
                self._buffer += self._receive()
                self._read_buffer()
            sleep(IDLE_WAIT)

    def _read_buffer(self):
        """Process the socket buffer, and direct the data to the correct
        channel.

        :return:
        """
        while self._buffer:
            self._buffer, channel_id, frame_in = \
                self._handle_amqp_frame(self._buffer)

            if frame_in is None:
                break

            if channel_id == 0:
                self._channel0.on_frame(frame_in)
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
            if not self._channels[channel_id].is_open:
                continue
            self._channels[channel_id].close()

    def _close_socket(self):
        """Close Socket.

        :return:
        """
        if not self._socket:
            return
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except socket.error:
            pass
        self._socket.close()
        self._socket = None

    def _handle_socket_error(self, why):
        """Handle any socket errors. If requested we will try to
        re-establish the connection.

        :param exception why:
        :return:
        """
        previous_state = self._state
        self.set_state(self.CLOSED)
        if previous_state != self.CLOSED:
            LOGGER.error(why, exc_info=False)
        self._close_socket()
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
        while not self._poller.is_ready[1]:
            sleep(0.001)
        total_bytes_written = 0
        bytes_to_send = len(frame_data)
        while total_bytes_written < bytes_to_send:
            try:
                bytes_written = \
                    self.socket.send(frame_data[total_bytes_written:])
                if bytes_written == 0:
                    why = AMQPConnectionError('connection/socket error')
                    self._handle_socket_error(why)
                    break
                total_bytes_written += bytes_written
            except socket.timeout:
                pass
            except socket.error as why:
                if why.args[0] == EWOULDBLOCK:
                    continue
                self._handle_socket_error(why)
                break
        return total_bytes_written


class UriConnection(Connection):
    """Wrapper of the Connection class that takes the AMQP uri schema."""

    def __init__(self, uri):
        """Create a new Connection instance using an AMQP Uri string.

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
