"""AMQP-Storm Message."""
__author__ = 'eandersson'

from amqpstorm.compatibility import try_utf8_decode
from amqpstorm.exception import AMQPMessageError


class Message(object):
    """RabbitMQ Message Class."""
    __slots__ = ['_channel', '_body', '_method', '_properties']

    def __init__(self, channel, **message):
        self._channel = channel
        self._body = message.get('body', None)
        self._method = message.get('method', None)
        self._properties = message.get('properties', dict())

    def __iter__(self):
        for attribute in self.__slots__:
            yield (attribute[1::], getattr(self, attribute))

    @property
    def body(self):
        return try_utf8_decode(self._body)

    @property
    def channel(self):
        return self._channel

    @property
    def method(self):
        method = {}
        for key, value in self._method.items():
            method[key] = try_utf8_decode(value)
        return method

    @property
    def properties(self):
        properties = {}
        for key, value in self._properties.items():
            properties[key] = try_utf8_decode(value)
        return properties

    def ack(self):
        if not self._method:
            raise AMQPMessageError('method is None')
        self._channel.basic.ack(delivery_tag=self._method['delivery_tag'])

    def nack(self, requeue=False):
        if not self._method:
            raise AMQPMessageError('method is None')
        self._channel.basic.nack(delivery_tag=self._method['delivery_tag'],
                                 requeue=requeue)

    def reject(self, requeue=False):
        if not self._method:
            raise AMQPMessageError('method is None')
        self._channel.basic.reject(delivery_tag=self._method['delivery_tag'],
                                   requeue=requeue)

    def publish(self, routing_key, exchange=''):
        self._channel.basic.publish(self._body, routing_key, exchange,
                                    self._properties)

    def to_dict(self):
        """To Dictionary.
        :rtype: dict
        """
        return {
            'body': self._body,
            'method': self._method,
            'properties': self._properties,
            'channel': self._channel
        }

    def to_tuple(self):
        """To Tuple.
        :rtype: tuple
        """
        return self._body, self._channel, self._method, self._properties
