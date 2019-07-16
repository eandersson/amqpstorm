"""AMQPStorm Connection.Channel0."""

import logging
import platform

from pamqp import specification
from pamqp.heartbeat import Heartbeat

from amqpstorm import __version__
from amqpstorm.base import LOCALE
from amqpstorm.base import MAX_CHANNELS
from amqpstorm.base import MAX_FRAME_SIZE
from amqpstorm.base import Stateful
from amqpstorm.compatibility import try_utf8_decode
from amqpstorm.exception import AMQPConnectionError

LOGGER = logging.getLogger(__name__)


class Channel0(object):
    """Internal Channel0 handler."""

    def __init__(self, connection, client_properties=None):
        super(Channel0, self).__init__()
        self.is_blocked = False
        self.max_allowed_channels = MAX_CHANNELS
        self.max_frame_size = MAX_FRAME_SIZE
        self.server_properties = {}
        self._connection = connection
        self._heartbeat = connection.parameters['heartbeat']
        self._parameters = connection.parameters
        self._override_client_properties = client_properties

    def on_frame(self, frame_in):
        """Handle frames sent to Channel0.

        :param frame_in: Amqp frame.
        :return:
        """
        LOGGER.debug('Frame Received: %s', frame_in.name)
        if frame_in.name == 'Heartbeat':
            return
        elif frame_in.name == 'Connection.Close':
            self._close_connection(frame_in)
        elif frame_in.name == 'Connection.CloseOk':
            self._close_connection_ok()
        elif frame_in.name == 'Connection.Blocked':
            self._blocked_connection(frame_in)
        elif frame_in.name == 'Connection.Unblocked':
            self._unblocked_connection()
        elif frame_in.name == 'Connection.OpenOk':
            self._set_connection_state(Stateful.OPEN)
        elif frame_in.name == 'Connection.Start':
            self.server_properties = frame_in.server_properties
            self._send_start_ok(frame_in)
        elif frame_in.name == 'Connection.Tune':
            self._send_tune_ok(frame_in)
            self._send_open_connection()
        else:
            LOGGER.error('[Channel0] Unhandled Frame: %s', frame_in.name)

    def send_close_connection(self):
        """Send Connection Close frame.

        :return:
        """
        self._write_frame(specification.Connection.Close())

    def send_heartbeat(self):
        """Send Heartbeat frame.

        :return:
        """
        if not self._connection.is_open:
            return
        self._write_frame(Heartbeat())

    def _close_connection(self, frame_in):
        """Connection Close.

        :param specification.Connection.Close frame_in: Amqp frame.
        :return:
        """
        self._set_connection_state(Stateful.CLOSED)
        if frame_in.reply_code != 200:
            reply_text = try_utf8_decode(frame_in.reply_text)
            message = (
                'Connection was closed by remote server: %s' % reply_text
            )
            exception = AMQPConnectionError(message,
                                            reply_code=frame_in.reply_code)
            self._connection.exceptions.append(exception)

    def _close_connection_ok(self):
        """Connection CloseOk frame received.

        :return:
        """
        self._set_connection_state(Stateful.CLOSED)

    def _blocked_connection(self, frame_in):
        """Connection is Blocked.

        :param frame_in:
        :return:
        """
        self.is_blocked = True
        LOGGER.warning(
            'Connection is blocked by remote server: %s',
            try_utf8_decode(frame_in.reason)
        )

    def _negotiate(self, server_value, client_value):
        """Negotiate the highest supported value. Fall back on the
        client side value if zero.

        :param int server_value: Server Side value
        :param int client_value: Client Side value

        :rtype: int
        :return:
        """
        return min(server_value, client_value) or client_value

    def _unblocked_connection(self):
        """Connection is Unblocked.

        :return:
        """
        self.is_blocked = False
        LOGGER.info('Connection is no longer blocked by remote server')

    def _plain_credentials(self):
        """AMQP Plain Credentials.

        :rtype: str
        """
        return '\0%s\0%s' % (self._parameters['username'],
                             self._parameters['password'])

    def _send_start_ok(self, frame_in):
        """Send Start OK frame.

        :param specification.Connection.Start frame_in: Amqp frame.
        :return:
        """
        mechanisms = try_utf8_decode(frame_in.mechanisms)
        if 'EXTERNAL' in mechanisms:
            mechanism = 'EXTERNAL'
            credentials = '\0\0'
        elif 'PLAIN' in mechanisms:
            mechanism = 'PLAIN'
            credentials = self._plain_credentials()
        else:
            exception = AMQPConnectionError(
                'Unsupported Security Mechanism(s): %s' %
                frame_in.mechanisms
            )
            self._connection.exceptions.append(exception)
            return
        start_ok_frame = specification.Connection.StartOk(
            mechanism=mechanism,
            client_properties=self._client_properties(),
            response=credentials,
            locale=LOCALE
        )
        self._write_frame(start_ok_frame)

    def _send_tune_ok(self, frame_in):
        """Send Tune OK frame.

        :param specification.Connection.Tune frame_in: Tune frame.

        :return:
        """
        self.max_allowed_channels = self._negotiate(frame_in.channel_max,
                                                    MAX_CHANNELS)
        self.max_frame_size = self._negotiate(frame_in.frame_max,
                                              MAX_FRAME_SIZE)
        LOGGER.debug(
            'Negotiated max frame size %d, max channels %d',
            self.max_frame_size, self.max_allowed_channels
        )

        tune_ok_frame = specification.Connection.TuneOk(
            channel_max=self.max_allowed_channels,
            frame_max=self.max_frame_size,
            heartbeat=self._heartbeat)
        self._write_frame(tune_ok_frame)

    def _send_open_connection(self):
        """Send Open Connection frame.

        :return:
        """
        open_frame = specification.Connection.Open(
            virtual_host=self._parameters['virtual_host']
        )
        self._write_frame(open_frame)

    def _set_connection_state(self, state):
        """Set Connection state.

        :param state:
        :return:
        """
        self._connection.set_state(state)

    def _write_frame(self, frame_out):
        """Write a pamqp frame from Channel0.

        :param frame_out: Amqp frame.
        :return:
        """
        self._connection.write_frame(0, frame_out)
        LOGGER.debug('Frame Sent: %s', frame_out.name)

    def _client_properties(self):
        """AMQPStorm Client Properties.

        :rtype: dict
        """
        client_properties = {
            'product': 'AMQPStorm',
            'platform': 'Python %s (%s)' % (platform.python_version(),
                                            platform.python_implementation()),
            'capabilities': {
                'basic.nack': True,
                'connection.blocked': True,
                'publisher_confirms': True,
                'consumer_cancel_notify': True,
                'authentication_failure_close': True,
            },
            'information': 'See https://github.com/eandersson/amqpstorm',
            'version': __version__
        }
        if self._override_client_properties:
            client_properties.update(self._override_client_properties)
        return client_properties
