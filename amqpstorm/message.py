"""AMQP-Storm Message."""
__author__ = 'eandersson'

from amqpstorm.exception import AMQPMessageError

class Message(object):
    """RabbitMQ Message Class."""

    def __init__(self, channel, **message):
        self.channel = channel
        self.body = message.get('body', None)
        self.method = message.get('method', None)
        self.properties = message.get('properties', dict())

    def ack(self):
        if not self.method:
            raise AMQPMessageError('method is None')
        self.channel.basic.ack(delivery_tag=self.method['delivery_tag'])

    def nack(self, requeue=False):
        if not self.method:
            raise AMQPMessageError('method is None')
        self.channel.basic.nack(delivery_tag=self.method['delivery_tag'],
                                requeue=requeue)

    def reject(self, requeue=False):
        if not self.method:
            raise AMQPMessageError('method is None')
        self.channel.basic.reject(delivery_tag=self.method['delivery_tag'],
                                  requeue=requeue)

    def publish(self, routing_key, exchange=''):
        self.channel.basic.publish(self.body, routing_key, exchange,
                                   self.properties)

    def to_dict(self):
        """To Dictionary.
        :rtype: dict
        """
        return self.__dict__

    def to_tuple(self):
        """To Tuple.
        :rtype: tuple
        """
        return self.body, self.channel, self.method, self.properties
