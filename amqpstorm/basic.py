"""AMQPStorm Channel.Basic."""

import logging
import math

from pamqp import body as pamqp_body
from pamqp import header as pamqp_header
from pamqp import specification

from amqpstorm import compatibility
from amqpstorm.base import BaseMessage
from amqpstorm.base import Handler
from amqpstorm.base import MAX_FRAME_SIZE
from amqpstorm.exception import AMQPChannelError
from amqpstorm.exception import AMQPInvalidArgument
from amqpstorm.message import Message

LOGGER = logging.getLogger(__name__)


class Basic(Handler):
    """RabbitMQ Basic Operations."""
    __slots__ = ['_max_frame_size']

    def __init__(self, channel, max_frame_size=None):
        super(Basic, self).__init__(channel)
        self._max_frame_size = max_frame_size or MAX_FRAME_SIZE

    def qos(self, prefetch_count=0, prefetch_size=0, global_=False):
        """Specify quality of service.

        :param int prefetch_count: Prefetch window in messages
        :param int/long prefetch_size: Prefetch window in octets
        :param bool global_: Apply to entire connection

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :rtype: dict
        """
        if not compatibility.is_integer(prefetch_count):
            raise AMQPInvalidArgument('prefetch_count should be an integer')
        elif not compatibility.is_integer(prefetch_size):
            raise AMQPInvalidArgument('prefetch_size should be an integer')
        elif not isinstance(global_, bool):
            raise AMQPInvalidArgument('global_ should be a boolean')
        qos_frame = specification.Basic.Qos(prefetch_count=prefetch_count,
                                            prefetch_size=prefetch_size,
                                            global_=global_)
        return self._channel.rpc_request(qos_frame)

    def get(self, queue='', no_ack=False, to_dict=False, auto_decode=True,
            message_impl=None):
        """Fetch a single message.

        :param str queue: Queue name
        :param bool no_ack: No acknowledgement needed
        :param bool to_dict: Should incoming messages be converted to a
                    dictionary before delivery.
        :param bool auto_decode: Auto-decode strings when possible.
        :param class message_impl: Message implementation based on BaseMessage
        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :returns: Returns a single message, as long as there is a message in
                  the queue. If no message is available, returns None.

        :rtype: amqpstorm.Message,dict,None
        """
        if not compatibility.is_string(queue):
            raise AMQPInvalidArgument('queue should be a string')
        elif not isinstance(no_ack, bool):
            raise AMQPInvalidArgument('no_ack should be a boolean')
        elif self._channel.consumer_tags:
            raise AMQPChannelError("Cannot call 'get' when channel is "
                                   "set to consume")
        if message_impl:
            if not issubclass(message_impl, BaseMessage):
                raise AMQPInvalidArgument(
                    'message_impl should be derived from BaseMessage'
                )
        else:
            message_impl = Message
        get_frame = specification.Basic.Get(queue=queue,
                                            no_ack=no_ack)
        with self._channel.lock and self._channel.rpc.lock:
            message = self._get_message(get_frame, auto_decode=auto_decode,
                                        message_impl=message_impl)
            if message and to_dict:
                return message.to_dict()
            return message

    def recover(self, requeue=False):
        """Redeliver unacknowledged messages.

        :param bool requeue: Re-queue the messages

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :rtype: dict
        """
        if not isinstance(requeue, bool):
            raise AMQPInvalidArgument('requeue should be a boolean')
        recover_frame = specification.Basic.Recover(requeue=requeue)
        return self._channel.rpc_request(recover_frame)

    def consume(self, callback=None, queue='', consumer_tag='',
                exclusive=False, no_ack=False, no_local=False, arguments=None):
        """Start a queue consumer.

        :param typing.Callable callback: Message callback
        :param str queue: Queue name
        :param str consumer_tag: Consumer tag
        :param bool no_local: Do not deliver own messages
        :param bool no_ack: No acknowledgement needed
        :param bool exclusive: Request exclusive access
        :param dict arguments: Consume key/value arguments

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :returns: Consumer tag
        :rtype: str
        """
        if not compatibility.is_string(queue):
            raise AMQPInvalidArgument('queue should be a string')
        elif not compatibility.is_string(consumer_tag):
            raise AMQPInvalidArgument('consumer_tag should be a string')
        elif not isinstance(exclusive, bool):
            raise AMQPInvalidArgument('exclusive should be a boolean')
        elif not isinstance(no_ack, bool):
            raise AMQPInvalidArgument('no_ack should be a boolean')
        elif not isinstance(no_local, bool):
            raise AMQPInvalidArgument('no_local should be a boolean')
        elif arguments is not None and not isinstance(arguments, dict):
            raise AMQPInvalidArgument('arguments should be a dict or None')
        consume_rpc_result = self._consume_rpc_request(arguments, consumer_tag,
                                                       exclusive, no_ack,
                                                       no_local, queue)
        tag = self._consume_add_and_get_tag(consume_rpc_result)
        self._channel._consumer_callbacks[tag] = callback
        return tag

    def cancel(self, consumer_tag=''):
        """Cancel a queue consumer.

        :param str consumer_tag: Consumer tag

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :rtype: dict
        """
        if not compatibility.is_string(consumer_tag):
            raise AMQPInvalidArgument('consumer_tag should be a string')
        cancel_frame = specification.Basic.Cancel(consumer_tag=consumer_tag)
        result = self._channel.rpc_request(cancel_frame)
        self._channel.remove_consumer_tag(consumer_tag)
        return result

    def publish(self, body, routing_key, exchange='', properties=None,
                mandatory=False, immediate=False):
        """Publish a Message.

        :param bytes,str,unicode body: Message payload
        :param str routing_key: Message routing key
        :param str exchange: The exchange to publish the message to
        :param dict properties: Message properties
        :param bool mandatory: Requires the message is published
        :param bool immediate: Request immediate delivery

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :rtype: bool,None
        """
        self._validate_publish_parameters(body, exchange, immediate, mandatory,
                                          properties, routing_key)
        properties = properties or {}
        body = self._handle_utf8_payload(body, properties)
        properties = specification.Basic.Properties(**properties)
        method_frame = specification.Basic.Publish(exchange=exchange,
                                                   routing_key=routing_key,
                                                   mandatory=mandatory,
                                                   immediate=immediate)
        header_frame = pamqp_header.ContentHeader(body_size=len(body),
                                                  properties=properties)

        frames_out = [method_frame, header_frame]
        for body_frame in self._create_content_body(body):
            frames_out.append(body_frame)

        if self._channel.confirming_deliveries:
            with self._channel.rpc.lock:
                return self._publish_confirm(frames_out, mandatory)
        self._channel.write_frames(frames_out)

    def ack(self, delivery_tag=0, multiple=False):
        """Acknowledge Message.

        :param int/long delivery_tag: Server-assigned delivery tag
        :param bool multiple: Acknowledge multiple messages

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :return:
        """
        if not compatibility.is_integer(delivery_tag):
            raise AMQPInvalidArgument('delivery_tag should be an integer')
        elif not isinstance(multiple, bool):
            raise AMQPInvalidArgument('multiple should be a boolean')
        ack_frame = specification.Basic.Ack(delivery_tag=delivery_tag,
                                            multiple=multiple)
        self._channel.write_frame(ack_frame)

    def nack(self, delivery_tag=0, multiple=False, requeue=True):
        """Negative Acknowledgement.

        :param int/long delivery_tag: Server-assigned delivery tag
        :param bool multiple: Negative acknowledge multiple messages
        :param bool requeue: Re-queue the message

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :return:
        """
        if not compatibility.is_integer(delivery_tag):
            raise AMQPInvalidArgument('delivery_tag should be an integer')
        elif not isinstance(multiple, bool):
            raise AMQPInvalidArgument('multiple should be a boolean')
        elif not isinstance(requeue, bool):
            raise AMQPInvalidArgument('requeue should be a boolean')
        nack_frame = specification.Basic.Nack(delivery_tag=delivery_tag,
                                              multiple=multiple,
                                              requeue=requeue)
        self._channel.write_frame(nack_frame)

    def reject(self, delivery_tag=0, requeue=True):
        """Reject Message.

        :param int/long delivery_tag: Server-assigned delivery tag
        :param bool requeue: Re-queue the message

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :return:
        """
        if not compatibility.is_integer(delivery_tag):
            raise AMQPInvalidArgument('delivery_tag should be an integer')
        elif not isinstance(requeue, bool):
            raise AMQPInvalidArgument('requeue should be a boolean')
        reject_frame = specification.Basic.Reject(delivery_tag=delivery_tag,
                                                  requeue=requeue)
        self._channel.write_frame(reject_frame)

    def _consume_add_and_get_tag(self, consume_rpc_result):
        """Add the tag to the channel and return it.

        :param dict consume_rpc_result:

        :rtype: str
        """
        consumer_tag = consume_rpc_result['consumer_tag']
        self._channel.add_consumer_tag(consumer_tag)
        return consumer_tag

    def _consume_rpc_request(self, arguments, consumer_tag, exclusive, no_ack,
                             no_local, queue):
        """Create a Consume Frame and execute a RPC request.

        :param str queue: Queue name
        :param str consumer_tag: Consumer tag
        :param bool no_local: Do not deliver own messages
        :param bool no_ack: No acknowledgement needed
        :param bool exclusive: Request exclusive access
        :param dict arguments: Consume key/value arguments

        :rtype: dict
        """
        consume_frame = specification.Basic.Consume(queue=queue,
                                                    consumer_tag=consumer_tag,
                                                    exclusive=exclusive,
                                                    no_local=no_local,
                                                    no_ack=no_ack,
                                                    arguments=arguments)
        return self._channel.rpc_request(consume_frame)

    @staticmethod
    def _validate_publish_parameters(body, exchange, immediate, mandatory,
                                     properties, routing_key):
        """Validate Publish Parameters.

        :param bytes,str,unicode body: Message payload
        :param str routing_key: Message routing key
        :param str exchange: The exchange to publish the message to
        :param dict properties: Message properties
        :param bool mandatory: Requires the message is published
        :param bool immediate: Request immediate delivery

        :raises AMQPInvalidArgument: Invalid Parameters

        :return:
        """
        if not compatibility.is_string(body):
            raise AMQPInvalidArgument('body should be a string')
        elif not compatibility.is_string(routing_key):
            raise AMQPInvalidArgument('routing_key should be a string')
        elif not compatibility.is_string(exchange):
            raise AMQPInvalidArgument('exchange should be a string')
        elif properties is not None and not isinstance(properties, dict):
            raise AMQPInvalidArgument('properties should be a dict or None')
        elif not isinstance(mandatory, bool):
            raise AMQPInvalidArgument('mandatory should be a boolean')
        elif not isinstance(immediate, bool):
            raise AMQPInvalidArgument('immediate should be a boolean')

    @staticmethod
    def _handle_utf8_payload(body, properties):
        """Update the Body and Properties to the appropriate encoding.

        :param bytes,str,unicode body: Message payload
        :param dict properties: Message properties

        :return:
        """
        if 'content_encoding' not in properties:
            properties['content_encoding'] = 'utf-8'
        encoding = properties['content_encoding']
        if compatibility.is_unicode(body):
            body = body.encode(encoding)
        elif compatibility.PYTHON3 and isinstance(body, str):
            body = bytes(body, encoding=encoding)
        return body

    def _get_message(self, get_frame, auto_decode, message_impl):
        """Get and return a message using a Basic.Get frame.

        :param Basic.Get get_frame:
        :param bool auto_decode: Auto-decode strings when possible.
        :param class message_impl: Message implementation based on BaseMessage

        :rtype: Message
        """
        message_uuid = self._channel.rpc.register_request(
            get_frame.valid_responses + ['ContentHeader', 'ContentBody']
        )
        try:
            self._channel.write_frame(get_frame)
            get_ok_frame = self._channel.rpc.get_request(message_uuid,
                                                         raw=True,
                                                         multiple=True)
            if isinstance(get_ok_frame, specification.Basic.GetEmpty):
                return None
            content_header = self._channel.rpc.get_request(message_uuid,
                                                           raw=True,
                                                           multiple=True)
            body = self._get_content_body(message_uuid,
                                          content_header.body_size)
        finally:
            self._channel.rpc.remove(message_uuid)
        return message_impl(channel=self._channel,
                            body=body,
                            method=dict(get_ok_frame),
                            properties=dict(content_header.properties),
                            auto_decode=auto_decode)

    def _publish_confirm(self, frames_out, mandatory):
        """Confirm that message was published successfully.

        :param list frames_out:

        :rtype: bool
        """
        confirm_uuid = self._channel.rpc.register_request(['Basic.Ack',
                                                           'Basic.Nack'])
        self._channel.write_frames(frames_out)
        result = self._channel.rpc.get_request(confirm_uuid, raw=True)
        if mandatory:
            self._channel.check_for_exceptions()
        if isinstance(result, specification.Basic.Ack):
            return True
        return False

    def _create_content_body(self, body):
        """Split body based on the maximum frame size.

            This function is based on code from Rabbitpy.
            https://github.com/gmr/rabbitpy

        :param bytes,str,unicode body: Message payload

        :rtype: collections.Iterable
        """
        frames = int(math.ceil(len(body) / float(self._max_frame_size)))
        for offset in compatibility.RANGE(0, frames):
            start_frame = self._max_frame_size * offset
            end_frame = start_frame + self._max_frame_size
            body_len = len(body)
            if end_frame > body_len:
                end_frame = body_len
            yield pamqp_body.ContentBody(body[start_frame:end_frame])

    def _get_content_body(self, message_uuid, body_size):
        """Get Content Body using RPC requests.

        :param str uuid_body: Rpc Identifier.
        :param int body_size: Content Size.

        :rtype: str
        """
        body = bytes()
        while len(body) < body_size:
            body_piece = self._channel.rpc.get_request(message_uuid, raw=True,
                                                       multiple=True)
            if not body_piece.value:
                break
            body += body_piece.value
        return body
