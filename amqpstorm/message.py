""" AMQP-Storm Message. """
__author__ = 'eandersson'

from amqpstorm.exception import AMQPChannelError


class Message(object):
    """ RabbitMQ Message Class. """

    def __init__(self, channel,
                 basic_deliver,
                 content_header,
                 content_body):
        if len(content_body.value) != content_header.body_size:
            raise AMQPChannelError('payload != body_size')
        self.body = content_body.value
        self.channel = channel
        self.method = basic_deliver.__dict__
        self.properties = content_header.properties.__dict__

    def to_dict(self):
        """ To Dictionary.
        :rtype: dict
        """
        return self.__dict__

    def to_tuple(self):
        """ To Tuple.
        :rtype: tuple
        """
        return self.body, self.channel, self.method, self.properties
