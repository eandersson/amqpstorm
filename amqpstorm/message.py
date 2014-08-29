"""AMQP-Storm Message"""
__author__ = 'eandersson'


class Message(object):
    """RabbitMQ Message Class"""

    def __init__(self, body,
                 channel,
                 method,
                 properties):
        self.body = body
        self.channel = channel
        self.method = method
        self.properties = properties

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
