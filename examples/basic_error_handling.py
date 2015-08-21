__author__ = 'eandersson'

import logging

from amqpstorm import Connection
from amqpstorm import AMQPError

from examples import HOST
from examples import USERNAME
from examples import PASSWORD

logging.basicConfig(level=logging.DEBUG)


class Publisher(object):
    def __init__(self, host, username, password):
        self.channel = None
        self.connection = None
        self.host = host
        self.username = username
        self.password = password
        self.connect()

    def connect(self):
        self.connection = Connection(self.host, self.username, self.password)
        self.channel = self.connection.channel()

    def close_connection(self):
        self.channel.close()
        self.connection.close()

    def send_message(self, queue, message):
        if self.connection.is_closed:
            self.reconnect()
        try:
            self.channel.basic.publish(body=message,
                                       routing_key=queue)
        except AMQPError as why:
            # When handling AMQPError's here, be careful as you may
            # need to re-send the payload.
            print(why)
            self.reconnect()

    def reconnect(self):
        """ Re-connect.

        :return:
        """
        try:
            if self.connection.is_closed:
                self.connection.open()
            if self.channel.is_closed:
                self.channel.open()
        except AMQPError:
            raise


if __name__ == '__main__':
    PUBLISHER = Publisher(HOST, USERNAME, PASSWORD)
    PUBLISHER.send_message('simple_queue', 'Hello World!')
    PUBLISHER.close_connection()
