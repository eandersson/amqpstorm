"""AMQP-Storm Connection.Channel."""

import logging
from time import sleep

from pamqp import specification as pamqp_spec

from pamqp.header import ContentHeader

from amqpstorm import compatibility
from amqpstorm.base import BaseChannel
from amqpstorm.base import IDLE_WAIT
from amqpstorm.base import Rpc
from amqpstorm.basic import Basic
from amqpstorm.exception import AMQPChannelError
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.exception import AMQPInvalidArgument
from amqpstorm.exception import AMQPMessageError
from amqpstorm.exchange import Exchange
from amqpstorm.message import Message
from amqpstorm.queue import Queue
from amqpstorm.compatibility import try_utf8_decode

LOGGER = logging.getLogger(__name__)
CONTENT_FRAME = ['Basic.Deliver', 'ContentHeader', 'ContentBody']


class Channel(BaseChannel):
    """Connection.channel"""

    def __init__(self, channel_id, connection, rpc_timeout):
        super(Channel, self).__init__(channel_id)
        self.rpc = Rpc(self, timeout=rpc_timeout)
        self.confirming_deliveries = False
        self.consumer_callback = None
        self._inbound = []
        self._connection = connection
        self._basic = None
        self._exchange = None
        self._queue = None

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, _):
        if exception_value:
            LOGGER.warning('Closing channel due to an unhandled exception: %s',
                           exception_type)
        if not self.is_open:
            return
        self.close()

    def __int__(self):
        return self._channel_id

    @property
    def basic(self):
        """RabbitMQ Basic Operations.

        :rtype: Basic
        """
        return self._lazy_load_handler('_basic', Basic)

    @property
    def exchange(self):
        """RabbitMQ Exchange Operations.

        :rtype: Exchange
        """
        return self._lazy_load_handler('_exchange', Exchange)

    @property
    def queue(self):
        """RabbitMQ Queue Operations.

        :rtype: Queue
        """
        return self._lazy_load_handler('_queue', Queue)

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

        :param int reply_code: Close reply code (e.g. 200)
        :param str reply_text: Close reply text

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :return:
        """
        LOGGER.debug('Channel #%d Closing', self.channel_id)
        if not compatibility.is_integer(reply_code):
            raise AMQPInvalidArgument('reply_code should be an integer')
        elif not compatibility.is_string(reply_text):
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
        LOGGER.debug('Channel #%d Closed', self.channel_id)

    def confirm_deliveries(self):
        """Set the channel to confirm that each message has been
        successfully delivered.

        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

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
        if self.rpc.on_frame(frame_in):
            return

        if frame_in.name in CONTENT_FRAME:
            self._inbound.append(frame_in)
        elif frame_in.name == 'Basic.ConsumeOk':
            self.add_consumer_tag(frame_in['consumer_tag'])
        elif frame_in.name == 'Channel.Close':
            self._close_channel(frame_in)
        elif frame_in.name == 'Basic.Cancel':
            LOGGER.warning('Received Basic.Cancel on consumer_tag: %s',
                           try_utf8_decode(frame_in.consumer_tag))
            self.remove_consumer_tag(frame_in.consumer_tag)
        elif frame_in.name == 'Basic.CancelOk':
            self.remove_consumer_tag(frame_in.consumer_tag)
        elif frame_in.name == 'Channel.Flow':
            self.write_frame(pamqp_spec.Channel.FlowOk(frame_in.active))
        elif frame_in.name == 'Basic.Return':
            self._basic_return(frame_in)
        else:
            LOGGER.error('[Channel%d] Unhandled Frame: %s -- %s',
                         self.channel_id, frame_in.name, dict(frame_in))

    def start_consuming(self, to_tuple=True):
        """Start consuming messages.

        :param bool to_tuple: Should incoming messages be converted to a
                              tuple before delivery.

        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :return:
        """
        while self.consumer_tags and not self.is_closed:
            self.process_data_events(to_tuple=to_tuple)

    def stop_consuming(self):
        """Stop consuming messages.

        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :return:
        """
        if not self.consumer_tags:
            return
        for tag in self.consumer_tags:
            self.basic.cancel(tag)
        self.remove_consumer_tag()

    def process_data_events(self, to_tuple=True):
        """Consume inbound messages.

            This is only required when consuming messages. All other
            events are automatically handled in the background.

        :param bool to_tuple: Should incoming messages be converted to a
                              tuple before delivery.

        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :return:
        """
        if not self.consumer_callback:
            raise AMQPChannelError('no consumer_callback defined')
        for message in self.build_inbound_messages(break_on_empty=True):
            if not to_tuple:
                # noinspection PyCallingNonCallable
                self.consumer_callback(message)
                continue
            # noinspection PyCallingNonCallable
            self.consumer_callback(*message.to_tuple())
        sleep(IDLE_WAIT)

    def build_inbound_messages(self, break_on_empty=False, to_tuple=False):
        """Build messages in the inbound queue.

        :param bool break_on_empty: Should we break the loop when there are
                                    no more messages to consume.
        :param bool to_tuple: Should incoming messages be converted to a
                              tuple before delivery.

        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :rtype: :py:class:`generator`
        """
        self.check_for_errors()
        while not self.is_closed:
            message = self._build_message()
            if not message:
                if break_on_empty:
                    break
                self.check_for_errors()
                sleep(IDLE_WAIT)
                continue
            if to_tuple:
                yield message.to_tuple()
                continue
            yield message

    def write_frame(self, frame_out):
        """Write a pamqp frame from the current channel.

        :param pamqp_spec.Frame frame_out: A single pamqp frame.
        :return:
        """
        self.check_for_errors()
        self._connection.write_frame(self.channel_id, frame_out)

    def write_frames(self, multiple_frames):
        """Write multiple pamqp frames from the current channel.

        :param list multiple_frames: A list of pamqp frames.
        :return:
        """
        self.check_for_errors()
        self._connection.write_frames(self.channel_id, multiple_frames)

    def check_for_errors(self):
        """Check connection and channel for errors.

        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.
        :return:
        """
        if self._connection.exceptions or self._connection.is_closed:
            self.set_state(self.CLOSED)
            why = AMQPConnectionError('connection was closed')
            if self._connection.exceptions:
                why = self._connection.exceptions[0]
            raise why

        if self.exceptions:
            exception = self.exceptions[0]
            if self.is_open:
                self.exceptions.pop(0)
            raise exception

        if self.is_closed:
            raise AMQPChannelError('channel was closed')

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
            reply_text = try_utf8_decode(frame_in.reply_text)
            message = 'Channel %d was closed by remote server: %s' % \
                      (self._channel_id, reply_text)
            exception = AMQPChannelError(message,
                                         reply_code=frame_in.reply_code)
            self.exceptions.append(exception)
        del self._inbound[:]
        self.set_state(self.CLOSED)

    def _basic_return(self, frame_in):
        """Handle Basic Return and treat it as an error.

        :param pamqp_spec.Return frame_in: Amqp frame.
        :return:
        """
        reply_text = try_utf8_decode(frame_in.reply_text)
        message = ("Message not delivered: %s (%s) to queue '%s' "
                   "from exchange '%s'" % (reply_text,
                                           frame_in.reply_code,
                                           frame_in.routing_key,
                                           frame_in.exchange))
        exception = AMQPMessageError(message,
                                     reply_code=frame_in.reply_code)
        self.exceptions.append(exception)

    def _build_message(self):
        """Fetch and build a complete Message from the inbound queue.

        :rtype: Message
        """
        with self.lock:
            if len(self._inbound) < 2:
                return None
            basic_deliver = self._inbound.pop(0)
            if not isinstance(basic_deliver, pamqp_spec.Basic.Deliver):
                LOGGER.warning('Received an out-of-order frame: %s was '
                               'expecting a Basic.Deliver frame',
                               type(basic_deliver))
                return None
            content_header = self._inbound.pop(0)
            if not isinstance(content_header, ContentHeader):
                LOGGER.warning('Received an out-of-order frame: %s was '
                               'expecting a ContentHeader frame',
                               type(content_header))
                return None
            body = self._build_message_body(content_header.body_size)
        message = Message(channel=self,
                          body=body,
                          method=dict(basic_deliver),
                          properties=dict(content_header.properties))
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

    def _lazy_load_handler(self, name, handler_class):
        """Lazy load operations (e.g. Queue, Exchange)

        :param name:
        :param Handler handler_class: Handler (e.g. Queue)
        :return:
        """
        handler = getattr(self, name)
        if not handler:
            handler = handler_class(self)
            setattr(self, name, handler)
        return handler
