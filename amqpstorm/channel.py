"""AMQP-Storm Connection.Channel."""
__author__ = 'eandersson'

import logging
from time import sleep

from pamqp.header import ContentHeader
from pamqp import specification as pamqp_spec

from amqpstorm.base import Rpc
from amqpstorm.base import IDLE_WAIT
from amqpstorm.base import Stateful
from amqpstorm.base import BaseChannel
from amqpstorm.queue import Queue
from amqpstorm.basic import Basic
from amqpstorm import compatibility
from amqpstorm.message import Message
from amqpstorm.exchange import Exchange
from amqpstorm.exception import AMQPChannelError
from amqpstorm.exception import AMQPMessageError
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.exception import AMQPInvalidArgument


LOGGER = logging.getLogger(__name__)
CONTENT_FRAME = ['Basic.Deliver', 'ContentHeader', 'ContentBody']


class Channel(BaseChannel, Stateful):
    """RabbitMQ Channel Class."""

    def __init__(self, channel_id, connection, rpc_timeout):
        super(Channel, self).__init__(channel_id)
        self.rpc = Rpc(self, timeout=rpc_timeout)
        self._inbound = []
        self._connection = connection
        self.confirming_deliveries = False
        self.consumer_callback = None
        self.basic = Basic(self)
        self.queue = Queue(self)
        self.exchange = Exchange(self)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, _):
        if exception_value:
            message = 'Closing channel due to an unhandled exception: {0!s}'
            LOGGER.warning(message.format(exception_type))
        if not self.is_open:
            return
        self.close()

    def __int__(self):
        return self._channel_id

    def open(self):
        """Open Channel.

        :return:
        """
        self._inbound = []
        self._exceptions = []
        self.set_state(self.OPENING)
        self.rpc_request(pamqp_spec.Channel.Open())
        self.set_state(self.OPEN)

    def close(self, reply_code=0, reply_text=''):
        """Close Channel.

        :param int reply_code:
        :param str reply_text:
        :return:
        """
        LOGGER.debug('Channel #%s Closing.', self.channel_id)
        if not isinstance(reply_code, int):
            raise AMQPInvalidArgument('reply_code should be an integer')
        if not compatibility.is_string(reply_text):
            raise AMQPInvalidArgument('reply_text should be a string')

        if not self._connection.is_open or not self.is_open:
            self.remove_consumer_tag()
            self.set_state(self.CLOSED)
            return
        self.set_state(self.CLOSING)
        self.stop_consuming()
        self.rpc_request(pamqp_spec.Channel.Close(
            reply_code=reply_code,
            reply_text=reply_text))
        del self._inbound[:]
        self.set_state(self.CLOSED)
        LOGGER.debug('Channel #%s Closed.', self.channel_id)

    def confirm_deliveries(self):
        """Set the channel to confirm that each message has been
        successfully delivered.

        :return:
        """
        self.confirming_deliveries = True
        confirm_frame = pamqp_spec.Confirm.Select()
        return self.rpc_request(confirm_frame)

    def on_frame(self, frame_in):
        """Handle frame sent to this specific channel.

        :param pamqp.Frame frame_in: Amqp frame.
        :return:
        """
        rpc_request = self.rpc.on_frame(frame_in)
        if rpc_request:
            return

        if frame_in.name in CONTENT_FRAME:
            self._inbound.append(frame_in)
        elif frame_in.name == 'Basic.ConsumeOk':
            self.add_consumer_tag(frame_in['consumer_tag'])
        elif frame_in.name == 'Channel.Close':
            self._close_channel(frame_in)
        elif frame_in.name == 'Basic.Cancel':
            LOGGER.warning('Received Basic.Cancel on consumer_tag: {0!s}'
                           .format(frame_in.consumer_tag))
            self.remove_consumer_tag(frame_in.consumer_tag)
        elif frame_in.name == 'Basic.CancelOk':
            self.remove_consumer_tag(frame_in.consumer_tag)
        elif frame_in.name == 'Basic.Return':
            self._basic_return(frame_in)
        else:
            message = "Unhandled Frame: {0!s} -- {1!s}"
            LOGGER.error(message.format(frame_in.name,
                                        dict(frame_in)))

    def start_consuming(self):
        """Start consuming events.

        :return:
        """
        while self.consumer_tags and not self.is_closed:
            self.process_data_events()

    def stop_consuming(self):
        """Stop consuming events.

        :return:
        """
        if not self.consumer_tags:
            return
        for tag in self.consumer_tags:
            self.basic.cancel(tag)
        self.remove_consumer_tag()

    def process_data_events(self):
        """Consume inbound messages.

            This is only required when consuming messages. All other
            events are automatically handled in the background.

        :return:
        """
        if not self.consumer_callback:
            raise AMQPChannelError('no consumer_callback defined')
        self.check_for_errors()
        while self._inbound and not self.is_closed:
            message = self._fetch_message()
            if not message:
                break
            self.consumer_callback(*message.to_tuple())
        sleep(IDLE_WAIT)

    def write_frame(self, frame_out):
        """Write a pamqp frame from the current channel.

        :param pamqp_spec.Frame frame_out: Amqp frame.
        :return:
        """
        self.check_for_errors()
        self._connection.write_frame(self.channel_id, frame_out)

    def write_frames(self, frames_out):
        """Write multiple pamqp frames from the current channel.

        :param list frames_out: Amqp frames.
        :return:
        """
        self.check_for_errors()
        self._connection.write_frames(self.channel_id, frames_out)

    def check_for_errors(self):
        """Check for errors.

        :return:
        """
        self._connection.check_for_errors()
        if self._connection.is_closed:
            self.set_state(self.CLOSED)
            if not self.exceptions:
                why = AMQPConnectionError('connection was closed')
                self.exceptions.append(why)
        if self.is_closed:
            why = AMQPChannelError('channel was closed')
            self.exceptions.append(why)
        super(Channel, self).check_for_errors()

    def rpc_request(self, frame_out):
        """Perform a RPC Request.

        :param pamqp_spec.Frame frame_out: Amqp frame.
        :rtype: dict
        """
        with self.rpc.lock:
            uuid = self.rpc.register_request(frame_out.valid_responses)
            self.write_frame(frame_out)
            return self.rpc.get_request(uuid)

    def _close_channel(self, frame_in):
        """Close Channel.

        :param pamqp_spec.Channel.Close frame_in: Amqp frame.
        :return:
        """
        self.remove_consumer_tag()
        if frame_in.reply_code != 200:
            message = 'Channel {0!s} was closed by remote server: {1!s}'
            why = AMQPChannelError(message.format(self._channel_id,
                                                  frame_in.reply_text))
            self._exceptions.append(why)
        del self._inbound[:]
        self.set_state(self.CLOSED)

    def _basic_return(self, frame_in):
        """Handle Basic Return and treat it as an error.

        :param pamqp_spec.Return frame_in: Amqp frame.
        :return:
        """
        message = "Message not delivered: {0!s} ({1!s}) to queue" \
                  " '{2!s}' from exchange '{3!s}'" \
            .format(frame_in.reply_text,
                    frame_in.reply_code,
                    frame_in.routing_key,
                    frame_in.exchange)
        self.exceptions.append(AMQPMessageError(message))

    def _fetch_message(self):
        """Fetch a message from the inbound queue.

        :rtype: Message
        """
        with self.lock:
            if len(self._inbound) < 3:
                return None
            basic_deliver = self._inbound.pop(0)
            if not isinstance(basic_deliver, pamqp_spec.Basic.Deliver):
                LOGGER.warning('Received an out-of-order Basic.Deliver frame')
                return None
            content_header = self._inbound.pop(0)
            if not isinstance(content_header, ContentHeader):
                LOGGER.warning('Received an out-of-order ContentHeader frame')
                return None
            body = self._build_message_body(content_header.body_size)

        message = Message(body, self,
                          dict(basic_deliver),
                          dict(content_header.properties))
        return message

    def _build_message_body(self, body_size):
        """Build the Message body from the inbound queue.

        :rtype: str
        """
        body = bytes()
        while len(body) < body_size:
            if not self._inbound:
                sleep(IDLE_WAIT)
                continue
            body_piece = self._inbound.pop(0)
            if not body_piece:
                break
            body += body_piece.value
        return body
