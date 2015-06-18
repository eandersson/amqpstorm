"""AMQP-Storm Message."""
__author__ = 'eandersson'

from amqpstorm.exception import AMQPMessageError
from amqpstorm.compatibility import try_utf8_decode


class Message(object):
    """RabbitMQ Message Class."""
    __slots__ = ['auto_decode', '_body', '_channel', '_method', '_properties']

    def __init__(self, channel, auto_decode=True, **message):
        """
        :param Channel channel: amqp-storm Channel
        :param bool auto_decode: Auto-decode strings when possible. Does not
                                 apply to to_dict, or to_tuple.
        :param str|unicode body: Message body
        :param dict method: Message method
        :param dict properties: Message properties
        """
        self.auto_decode = auto_decode
        self._channel = channel
        self._body = message.get('body', None)
        self._method = message.get('method', None)
        self._properties = message.get('properties', {'headers': {}})

    def __iter__(self):
        for attribute in self.__slots__:
            if not attribute.startswith('_'):
                continue
            yield (attribute[1::], getattr(self, attribute))

    @staticmethod
    def create(channel, body, properties=None):
        """Create a new Message.

        :param Channel channel: AMQP-Storm Channel
        :param str|unicode body: Message body
        :param dict properties: Message properties
        :rtype: Message
        """
        return Message(channel, auto_decode=False,
                       body=body, properties=properties)

    @property
    def body(self):
        """Return the Message Body.

            If auto_decode is enabled, the body will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: bytes|str|unicode
        """
        if not self.auto_decode:
            return self._body
        return try_utf8_decode(self._body)

    @property
    def channel(self):
        """Return the Channel.

        :rtype: Channel
        """
        return self._channel

    @property
    def method(self):
        """Return the Message Method.

            If auto_decode is enabled, the any strings will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: dict
        """
        if not self.auto_decode:
            return self._method
        return self._decode_utf8_content(self._method)

    @property
    def properties(self):
        """Returns the Message Properties.

            If auto_decode is enabled, the any strings will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: dict
        """
        if not self.auto_decode:
            return self._properties
        return self._decode_utf8_content(self._properties)

    def ack(self):
        """Acknowledge Message.

        :return:
        """
        if not self._method:
            raise AMQPMessageError('Message.ack only available on '
                                   'incoming messages.')
        self._channel.basic.ack(delivery_tag=self._method['delivery_tag'])

    def nack(self, requeue=True):
        """Negative Acknowledgement.

        :param bool requeue:
        """
        if not self._method:
            raise AMQPMessageError('Message.nack only available on '
                                   'incoming messages.')
        self._channel.basic.nack(delivery_tag=self._method['delivery_tag'],
                                 requeue=requeue)

    def reject(self, requeue=True):
        """Reject Message.

        :param bool requeue: Requeue the message
        """
        if not self._method:
            raise AMQPMessageError('Message.reject only available on '
                                   'incoming messages.')
        self._channel.basic.reject(delivery_tag=self._method['delivery_tag'],
                                   requeue=requeue)

    def publish(self, routing_key, exchange='', mandatory=False,
                immediate=False):
        """Publish Message.

        :param str routing_key:
        :param str exchange:
        :param bool mandatory:
        :param bool immediate:
        """
        return self._channel.basic.publish(body=self._body,
                                           routing_key=routing_key,
                                           exchange=exchange,
                                           properties=self._properties,
                                           mandatory=mandatory,
                                           immediate=immediate)

    def to_dict(self):
        """Message to Dictionary.

        :rtype: dict
        """
        return {
            'body': self._body,
            'method': self._method,
            'properties': self._properties,
            'channel': self._channel
        }

    def to_tuple(self):
        """Message to Tuple.

        :rtype: tuple
        """
        return self._body, self._channel, self._method, self._properties

    def _decode_utf8_content(self, content):
        if not content:
            return content
        if isinstance(content, dict):
            return self._decode_dict_content(content)
        return try_utf8_decode(content)

    def _decode_dict_content(self, content):
        result = dict()
        for key, value in content.items():
            key = try_utf8_decode(key)
            if isinstance(value, dict):
                result[key] = self._decode_dict_content(value)
            elif isinstance(value, list):
                result[key] = self._decode_list_content(value)
            else:
                result[key] = try_utf8_decode(value)
        return result

    @staticmethod
    def _decode_list_content(content):
        result = list()
        for value in content:
            result.append(try_utf8_decode(value))
        return result
