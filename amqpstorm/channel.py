""" AMQP-Storm Connection.Channel. """
__author__ = 'eandersson'

import math
import logging
from time import sleep

from pamqp import specification as pamqp_spec

from amqpstorm.base import Rpc
from amqpstorm.base import IDLE_WAIT
from amqpstorm.base import BaseChannel
from amqpstorm.queue import Queue
from amqpstorm.basic import Basic
from amqpstorm.message import Message
from amqpstorm.exchange import Exchange
from amqpstorm.exception import AMQPChannelError
from amqpstorm.exception import AMQPConnectionError


LOGGER = logging.getLogger(__name__)
CONTENT_FRAME = ['Basic.Deliver', 'ContentHeader', 'ContentBody']


class Channel(BaseChannel):
    """ RabbitMQ Channel Class """

    def __init__(self, channel_id, connection):
        super(Channel, self).__init__(channel_id)
        self.rpc = Rpc(self)
        self._connection = connection
        self._inbound = []
        self.consumer_callback = None
        self.basic = Basic(self)
        self.queue = Queue(self)
        self.exchange = Exchange(self)
        self.open()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_value:
            msg = 'Closing channel due to an unhandled exception: {0}'
            LOGGER.error(msg.format(exception_type))
        self.close()

    def __int__(self):
        return self._channel_id

    @property
    def inbound(self):
        """ Internal Inbound AMQP message queue.

            This is exposed for advanced and internal use.

        :rtype: list
        """
        return self._inbound

    @property
    def messages_inbound(self):
        """ Number of messages queued for processing.
        :return:
        """
        return math.ceil(len(self._inbound) / 3)

    def open(self):
        """ Open Channel.

        :return:
        """
        self._inbound = []
        self._exceptions = []
        LOGGER.debug('Opening Channel: {0}'.format(self.channel_id))
        self.set_state(self.OPENING)
        self.write_frame(pamqp_spec.Channel.Open())

    def close(self, reply_code=0, reply_text=''):
        """ Close Channel.

        :param int reply_code:
        :param str reply_text:
        :return:
        """
        if not self.is_open:
            self.set_state(self.CLOSED)
            return
        self.set_state(self.CLOSING)
        self.write_frame(pamqp_spec.Channel.Close(
            reply_code=reply_code,
            reply_text=reply_text))

    def on_frame(self, frame_in):
        """ Handle frame sent to this specific channel.

        :param frame_in: Amqp frame
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
            self.remove_consumer_tag(frame_in.consumer_tag)
        elif frame_in.name == 'Basic.CancelOk':
            self.remove_consumer_tag(frame_in.consumer_tag)
        elif frame_in.name == 'Channel.OpenOk':
            self.set_state(self.OPEN)
        else:
            msg = "Unhandled Frame: {0} -- {1}"
            LOGGER.error(msg.format(frame_in.name,
                                    frame_in.__dict__))

    def start_consuming(self):
        """ Start consuming events.

        :return:
        """
        while self.consumer_tags and not self.is_closed:
            self.process_data_events()

    def stop_consuming(self):
        """ Stop consuming events.

        :return:
        """
        if not self.consumer_tags:
            return
        self.basic.cancel()
        self.remove_consumer_tag()

    def process_data_events(self):
        """ Consume events in inbound buffer.

        :return:
        """
        self.check_for_errors()
        while self._inbound and not self.is_closed:
            if not self.consumer_tags:
                break
            message = self._fetch_message()
            if not message:
                break
            if self.consumer_callback:
                self.consumer_callback(*message.to_tuple())
        sleep(IDLE_WAIT)

    def write_frame(self, frame_out):
        """ Write a frame from the current channel.

        :param frame_out:
        :return:
        """
        self.check_for_errors()
        self._connection.write_frame(self.channel_id, frame_out)

    def write_frames(self, frames_out):
        """ Write multiple frames from the current channel.

        :param frames_out:
        :return:
        """
        self.check_for_errors()
        self._connection.write_frames(self.channel_id, frames_out)

    def check_for_errors(self):
        """ Check for errors.

        :return:
        """
        self._connection.check_for_errors()
        if self._connection.is_closed:
            self.set_state(self.CLOSED)
            if not self.exceptions:
                why = AMQPConnectionError('connection was closed')
                self.exceptions.append(why)
        super(Channel, self).check_for_errors()

    def _close_channel(self, frame_in):
        """ Close Channel.

        :param frame_in:
        :return:
        """
        if frame_in.reply_code != 200:
            msg = 'Channel {0} was closed by remote server: {1}'
            why = AMQPChannelError(msg.format(self._channel_id,
                                              frame_in.reply_text))
            self._exceptions.append(why)
        self.remove_consumer_tag()
        self.set_state(self.CLOSED)

    def _fetch_message(self):
        """ Fetch a message from the inbound queue.
        :rtype: Message
        """
        with self.lock:
            if len(self._inbound) < 3:
                return None
            basic_deliver = self._inbound.pop(0)
            content_header = self._inbound.pop(0)
            body = self._build_messsage_body(content_header.body_size)

        message = Message(body, self,
                          basic_deliver.__dict__,
                          content_header.properties.__dict__)
        return message

    def _build_messsage_body(self, body_size):
        """ Build the Message body from the inbound queue. """
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
