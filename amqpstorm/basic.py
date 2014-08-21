""" AMQP-Storm Channel.Basic. """
__author__ = 'eandersson'

import math
import inspect
import logging

from pamqp import body as pamqp_body
from pamqp import header as pamqp_header
from pamqp import specification as pamqp_spec

from amqpstorm.base import FRAME_MAX
from amqpstorm.message import Message
from amqpstorm.exception import AMQPChannelError


LOGGER = logging.getLogger(__name__)


class Basic(object):
    """ Channel.Basic """

    def __init__(self, channel):
        self._channel = channel

    def qos(self, prefetch_size=0, prefetch_count=0, global_=False):
        """ Specify quality of service.

        :param int/long prefetch_size: Prefetch window in octets
        :param int prefetch_count: Prefetch window in messages
        :param bool global_: Apply to entire connection
        :return:
        """
        qos_frame = pamqp_spec.Basic.Qos(prefetch_count=prefetch_count,
                                         prefetch_size=prefetch_size,
                                         global_=global_)
        with self._channel.rpc.lock:
            uuid = self._channel.rpc.register_request(pamqp_spec.Basic.Qos)
            self._channel.write_frame(qos_frame)
            return self._channel.rpc.get_request(uuid)

    def get(self, queue='', no_ack=False):
        """ Get a single message.

        :param str queue:
        :param bool no_ack: No acknowledgement needed
        :rtype: dict or None
        """

        if self._channel.consumer_tags:
            LOGGER.warning('Unable to perform Basic.Get when consuming.')
            return None

        get_frame = pamqp_spec.Basic.Get(queue=queue,
                                         no_ack=no_ack)

        with self._channel.lock and self._channel.rpc.lock:
            uuid_get = self._channel.rpc.register_request(pamqp_spec.Basic.Get)
            uuid_header = self._channel.rpc.register_request('ContentHeader')
            uuid_body = self._channel.rpc.register_request('ContentBody')

            self._channel.write_frame(get_frame)
            get_frame = self._channel.rpc.get_request(uuid_get, True)
            if not isinstance(get_frame, pamqp_spec.Basic.GetOk):
                self._channel.rpc.remove_request(uuid_header)
                self._channel.rpc.remove_request(uuid_body)
                return None

            content_header = self._channel.rpc.get_request(uuid_header, True)
            content_body = self._channel.rpc.get_request(uuid_body, True)
            message = Message(self._channel,
                              get_frame,
                              content_header,
                              content_body).to_dict()

        return message

    def recover(self, requeue=False):
        """ Redeliver unacknowledged messages.

        :param bool requeue: Requeue the messages
        :return:
        """
        recover_frame = pamqp_spec.Basic.Recover(requeue=requeue)
        with self._channel.rpc.lock:
            uuid = self._channel.rpc.register_request(pamqp_spec.Basic.Recover)
            self._channel.write_frame(recover_frame)
            return self._channel.rpc.get_request(uuid)

    def consume(self, callback, queue='', consumer_tag='', exclusive=False,
                no_ack=False, no_local=False, arguments=None):
        """ Start a queue consumer.

        :param function callback:
        :param str queue:
        :param str consumer_tag:
        :param bool no_local: Do not deliver own messages
        :param bool no_ack: No acknowledgement needed
        :param bool exclusive: Request exclusive access
        :param dict arguments: Arguments for declaration
        :return:
        """
        if not inspect.isfunction(callback):
            raise AMQPChannelError('callback is not callable')

        self._channel.consumer_callback = callback
        method_frame = pamqp_spec.Basic.Consume(queue=queue,
                                                consumer_tag=consumer_tag,
                                                exclusive=exclusive,
                                                no_local=no_local,
                                                no_ack=no_ack,
                                                arguments=arguments)
        with self._channel.rpc.lock:
            uuid = self._channel.rpc.register_request(pamqp_spec.Basic.Consume)
            self._channel.write_frame(method_frame)
            frame_in = self._channel.rpc.get_request(uuid)
        consumer_tag = frame_in['consumer_tag']
        self._channel.add_consumer_tag(consumer_tag)
        return consumer_tag

    def cancel(self, consumer_tag=''):
        """ Cancel a queue consumer.

        :param str consumer_tag: Consumer tag
        :return:
        """
        cancel_frame = pamqp_spec.Basic.Cancel(consumer_tag=consumer_tag)
        with self._channel.rpc.lock:
            uuid = self._channel.rpc.register_request(pamqp_spec.Basic.Cancel)
            self._channel.write_frame(cancel_frame)
            result = self._channel.rpc.get_request(uuid)
        self._channel.remove_consumer_tag(consumer_tag)
        return result

    def publish(self, body, routing_key, exchange='', properties=None):
        """ Publish Message.

        :param str|unicode body:
        :param str routing_key:
        :param str exchange:
        :param dict properties:
        :return:
        """
        if not properties:
            properties = {}

        if isinstance(body, unicode):
            if 'content_encoding' not in properties:
                properties['content_encoding'] = 'utf-8'
            encoding = properties.get('content_encoding')
            body = body.encode(encoding)

        properties = pamqp_spec.Basic.Properties(**properties)
        method_frame = pamqp_spec.Basic.Publish(exchange=exchange,
                                                routing_key=routing_key)
        header_frame = pamqp_header.ContentHeader(body_size=len(body),
                                                  properties=properties)
        send_buffer = [method_frame, header_frame]
        self._create_content_body(body, send_buffer)
        self._channel.write_frames(send_buffer)

    def ack(self, delivery_tag=None, multiple=False):
        """ Acknowledge Message.

        :param int/long delivery_tag: Server-assigned delivery tag
        :param bool multiple: Acknowledge multiple messages
        :return:
        """
        ack_frame = pamqp_spec.Basic.Ack(delivery_tag=delivery_tag,
                                         multiple=multiple)
        self._channel.write_frame(ack_frame)

    def reject(self, delivery_tag=None, requeue=True):
        """ Reject Message.

        :param int/long delivery_tag: Server-assigned delivery tag
        :param bool requeue: Requeue the message
        :return:
        """
        reject_frame = pamqp_spec.Basic.Reject(delivery_tag=delivery_tag,
                                               requeue=requeue)
        self._channel.write_frame(reject_frame)

    def nack(self, delivery_tag=None, multiple=False, requeue=True):
        """ Negative Acknowledgement.

        :param int/long delivery_tag: Server-assigned delivery tag
        :param bool multiple:
        :param bool requeue:
        :return:
        """
        nack_frame = pamqp_spec.Basic.Nack(delivery_tag=delivery_tag,
                                           multiple=multiple,
                                           requeue=requeue)
        self._channel.write_frame(nack_frame)

    @staticmethod
    def _create_content_body(body, send_buffer):
        """ Split body based on the maximum frame size.

            This function is based on code from Rabbitpy.
            https://github.com/gmr/rabbitpy

        :param body:
        :param send_buffer:
        :return:
        """
        frames = int(math.ceil(len(body) / float(FRAME_MAX)))
        for offset in xrange(0, frames):
            start_frame = FRAME_MAX * offset
            end_frame = start_frame + FRAME_MAX
            if end_frame > len(body):
                end_frame = len(body)
            send_buffer.append(
                pamqp_body.ContentBody(body[start_frame:end_frame]))
