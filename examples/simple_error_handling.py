__author__ = 'eandersson'

import time
import logging

from amqpstorm import Connection
from amqpstorm import AMQPError


logging.basicConfig(level=logging.DEBUG)


def on_message(body, channel, header, properties):
    print "Message:", body
    channel.basic.ack(header['delivery_tag'])


def reconnect(connection, channel):
    """ On reconnect, make sure to re-open any channels previously opened.

    :param connection:
    :param channel:
    :return:
    """
    try:
        if connection.is_closed:
            connection.open()
            channel.open()
    except AMQPError:
        pass


def consumer():
    connection = Connection('127.0.0.1', 'guest', 'guest')
    channel = connection.channel()
    channel.queue.declare('simple_queue')
    while True:
        try:
            channel.basic.publish(body='Hello World!',
                                  routing_key='simple_queue')
        except AMQPError as why:
            print why
            reconnect(connection, channel)
        time.sleep(10)
    channel.close()
    connection.close()


if __name__ == '__main__':
    consumer()
