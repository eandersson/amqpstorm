__author__ = 'eandersson'

import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.DEBUG)


def publisher():
    connection = Connection('127.0.0.1', 'guest', 'guest')
    channel = connection.channel()
    channel.queue.declare('simple_queue')
    channel.confirm_deliveries()
    channel.basic.publish(body='Hello World!', routing_key='simple_queue', mandatory=True)


if __name__ == '__main__':
    publisher()
