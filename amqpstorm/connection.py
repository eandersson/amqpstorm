"""AMQPStorm Connection."""

import logging
import time
from time import sleep

from pamqp import exceptions as pamqp_exception
from pamqp import frame as pamqp_frame
from pamqp import header as pamqp_header
from pamqp import specification

from amqpstorm import compatibility
from amqpstorm.base import IDLE_WAIT
from amqpstorm.base import Stateful
from amqpstorm.channel import Channel
from amqpstorm.channel0 import Channel0
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.exception import AMQPInvalidArgument
from amqpstorm.heartbeat import Heartbeat
from amqpstorm.io import EMPTY_BUFFER
from amqpstorm.io import IO

LOGGER = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL = 60
DEFAULT_SOCKET_TIMEOUT = 10
DEFAULT_VIRTUAL_HOST = '/'


class Connection(Stateful):
    """RabbitMQ Connection.

    e.g.
    ::

        import amqpstorm
        connection = amqpstorm.Connection('localhost', 'guest', 'guest')

    Using a SSL Context:
    ::

        import ssl
        import amqpstorm
        ssl_options = {
            'context': ssl.create_default_context(cafile='cacert.pem'),
            'server_hostname': 'rmq.eandersson.net',
            'check_hostname': True,        # New 2.8.0, default is False
            'verify_mode': 'required',     # New 2.8.0, default is 'none'
        }
        connection = amqpstorm.Connection(
            'rmq.eandersson.net', 'guest', 'guest', port=5671,
            ssl=True, ssl_options=ssl_options
        )

    :param str hostname: Hostname
    :param str username: Username
    :param str password: Password
    :param int port: Server port
    :param str virtual_host: Virtual host
    :param int heartbeat: RabbitMQ Heartbeat interval
    :param int,float timeout: Socket timeout
    :param bool ssl: Enable SSL
    :param dict ssl_options: SSL kwargs
    :param dict client_properties: None or dict of client properties
    :param bool lazy: Lazy initialize the connection

    :raises AMQPConnectionError: Raises if the connection
                                 encountered an error.
    """
    __slots__ = [
        'heartbeat', 'parameters', '_channel0', '_channels', '_io'
    ]

    def __init__(self, hostname, username, password, port=5672, **kwargs):
        super(Connection, self).__init__()
        self.parameters = {
            'hostname': hostname,
            'username': username,
            'password': password,
            'port': port,
            'virtual_host': kwargs.get('virtual_host', DEFAULT_VIRTUAL_HOST),
            'heartbeat': kwargs.get('heartbeat', DEFAULT_HEARTBEAT_INTERVAL),
            'timeout': kwargs.get('timeout', DEFAULT_SOCKET_TIMEOUT),
            'ssl': kwargs.get('ssl', False),
            'ssl_options': kwargs.get('ssl_options', {}),
            'client_properties': kwargs.get('client_properties', {})
        }
        self._validate_parameters()
        self._io = IO(self.parameters, exceptions=self._exceptions,
                      on_read_impl=self._read_buffer)
        self._channel0 = Channel0(self, self.parameters['client_properties'])
        self._channels = {}
        self._last_channel_id = None
        self.heartbeat = Heartbeat(self.parameters['heartbeat'],
                                   self._channel0.send_heartbeat)
        if not kwargs.get('lazy', False):
            self.open()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, _):
        if exception_type:
            message = 'Closing connection due to an unhandled exception: %s'
            LOGGER.warning(message, exception_value)
        self.close()

    @property
    def channels(self):
        """Returns a dictionary of the Channels currently available.

        :rtype: dict
        """
        return self._channels

    @property
    def fileno(self):
        """Returns the Socket File number.

        :rtype: integer,None
        """
        if not self._io.socket:
            return None
        return self._io.socket.fileno()

    @property
    def is_blocked(self):
        """Is the connection currently being blocked from publishing by
        the remote server.

        :rtype: bool
        """
        return self._channel0.is_blocked

    @property
    def max_allowed_channels(self):
        """Returns the maximum allowed channels for the connection.

        :rtype: int
        """
        return self._channel0.max_allowed_channels

    @property
    def max_frame_size(self):
        """Returns the maximum allowed frame size for the connection.

        :rtype: int
        """
        return self._channel0.max_frame_size

    @property
    def server_properties(self):
        """Returns the RabbitMQ Server Properties.

        :rtype: dict
        """
        return self._channel0.server_properties

    @property
    def socket(self):
        """Returns an instance of the Socket used by the Connection.

        :rtype: socket.socket
        """
        return self._io.socket

    def channel(self, rpc_timeout=60, lazy=False):
        """Open a Channel.

        :param int rpc_timeout: Timeout before we give up waiting for an RPC
                                response from the server.

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :rtype: amqpstorm.Channel
        """
        LOGGER.debug('Opening a new Channel')
        if not compatibility.is_integer(rpc_timeout):
            raise AMQPInvalidArgument('rpc_timeout should be an integer')
        elif self.is_closed:
            raise AMQPConnectionError('socket/connection closed')

        with self.lock:
            channel_id = self._get_next_available_channel_id()
            channel = Channel(channel_id, self, rpc_timeout,
                              on_close_impl=self._cleanup_channel)
            self._channels[channel_id] = channel
            if not lazy:
                channel.open()
        LOGGER.debug('Channel #%d Opened', channel_id)
        return self._channels[channel_id]

    def check_for_errors(self):
        """Check Connection for errors.

        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.
        :return:
        """
        if not self.exceptions:
            if not self.is_closed:
                return
            why = AMQPConnectionError('connection was closed')
            self.exceptions.append(why)
        self.set_state(self.CLOSED)
        self.close()
        raise self.exceptions[0]

    def close(self):
        """Close the Connection.

        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.
        :return:
        """
        LOGGER.debug('Connection Closing')
        if not self.is_closed:
            self.set_state(self.CLOSING)
        self.heartbeat.stop()
        try:
            if not self.is_closed and self.socket:
                self._channel0.send_close_connection()
                self._wait_for_connection_state(state=Stateful.CLOSED)
        except AMQPConnectionError:
            pass
        finally:
            self._close_remaining_channels()
            self._io.close()
            self.set_state(self.CLOSED)
        LOGGER.debug('Connection Closed')

    def open(self):
        """Open Connection.

        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.
        """
        LOGGER.debug('Connection Opening')
        self.set_state(self.OPENING)
        self._exceptions = []
        self._channels = {}
        self._last_channel_id = None
        self._io.open()
        self._send_handshake()
        self._wait_for_connection_state(state=Stateful.OPEN)
        self.heartbeat.start(self._exceptions)
        LOGGER.debug('Connection Opened')

    def write_frame(self, channel_id, frame_out):
        """Marshal and write an outgoing pamqp frame to the Socket.

        :param int channel_id: Channel ID.
        :param specification.Frame frame_out: Amqp frame.

        :return:
        """
        frame_data = pamqp_frame.marshal(frame_out, channel_id)
        self.heartbeat.register_write()
        self._io.write_to_socket(frame_data)

    def write_frames(self, channel_id, frames_out):
        """Marshal and write multiple outgoing pamqp frames to the Socket.

        :param int channel_id: Channel ID/
        :param list frames_out: Amqp frames.

        :return:
        """
        data_out = EMPTY_BUFFER
        for single_frame in frames_out:
            data_out += pamqp_frame.marshal(single_frame, channel_id)
        self.heartbeat.register_write()
        self._io.write_to_socket(data_out)

    def _close_remaining_channels(self):
        """Forcefully close all open channels.

        :return:
        """
        for channel_id in list(self._channels):
            self._channels[channel_id].set_state(Channel.CLOSED)
            self._channels[channel_id].close()
            self._cleanup_channel(channel_id)

    def _get_next_available_channel_id(self):
        """Returns the next available available channel id.

        :raises AMQPConnectionError: Raises if there is no available channel.
        :rtype: int
        """
        for index in compatibility.RANGE(self._last_channel_id or 1,
                                         self.max_allowed_channels + 1):
            if index in self._channels:
                continue
            self._last_channel_id = index
            return index

        if self._last_channel_id:
            self._last_channel_id = None
            return self._get_next_available_channel_id()

        raise AMQPConnectionError(
            'reached the maximum number of channels %d' %
            self.max_allowed_channels)

    def _handle_amqp_frame(self, data_in):
        """Unmarshal a single AMQP frame and return the result.

        :param data_in: socket data

        :return: data_in, channel_id, frame
        """
        if not data_in:
            return data_in, None, None
        try:
            byte_count, channel_id, frame_in = pamqp_frame.unmarshal(data_in)
            return data_in[byte_count:], channel_id, frame_in
        except pamqp_exception.UnmarshalingException:
            pass
        except specification.AMQPFrameError as why:
            LOGGER.error('AMQPFrameError: %r', why, exc_info=True)
        except ValueError as why:
            LOGGER.error(why, exc_info=True)
            self.exceptions.append(AMQPConnectionError(why))
        return data_in, None, None

    def _read_buffer(self, data_in):
        """Process the socket buffer, and direct the data to the appropriate
        channel.

        :rtype: bytes
        """
        while data_in:
            data_in, channel_id, frame_in = self._handle_amqp_frame(data_in)

            if frame_in is None:
                break

            self.heartbeat.register_read()
            if channel_id == 0:
                self._channel0.on_frame(frame_in)
            elif channel_id in self._channels:
                self._channels[channel_id].on_frame(frame_in)

        return data_in

    def _cleanup_channel(self, channel_id):
        """Remove the the channel from the list of available channels.

        :param int channel_id: Channel id

        :return:
        """
        with self.lock:
            if channel_id not in self._channels:
                return
            del self._channels[channel_id]

    def _send_handshake(self):
        """Send a RabbitMQ Handshake.

        :return:
        """
        self._io.write_to_socket(pamqp_header.ProtocolHeader().marshal())

    def _validate_parameters(self):
        """Validate Connection Parameters.

        :return:
        """
        if not compatibility.is_string(self.parameters['hostname']):
            raise AMQPInvalidArgument('hostname should be a string')
        elif not compatibility.is_integer(self.parameters['port']):
            raise AMQPInvalidArgument('port should be an integer')
        elif not compatibility.is_string(self.parameters['username']):
            raise AMQPInvalidArgument('username should be a string')
        elif not compatibility.is_string(self.parameters['password']):
            raise AMQPInvalidArgument('password should be a string')
        elif not compatibility.is_string(self.parameters['virtual_host']):
            raise AMQPInvalidArgument('virtual_host should be a string')
        elif not isinstance(self.parameters['timeout'], (int, float)):
            raise AMQPInvalidArgument('timeout should be an integer or float')
        elif not compatibility.is_integer(self.parameters['heartbeat']):
            raise AMQPInvalidArgument('heartbeat should be an integer')

    def _wait_for_connection_state(self, state=Stateful.OPEN, rpc_timeout=30):
        """Wait for a Connection state.

        :param int state: State that we expect

        :raises AMQPConnectionError: Raises if we are unable to establish
                                     a connection to RabbitMQ.

        :return:
        """
        start_time = time.time()
        while self.current_state != state:
            self.check_for_errors()
            if time.time() - start_time > rpc_timeout:
                raise AMQPConnectionError('Connection timed out')
            sleep(IDLE_WAIT)
