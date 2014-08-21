__author__ = 'eandersson'

import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.DEBUG)


def publisher():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.publish(body='Hello World!',
                                  routing_key='simple_queue')


if __name__ == '__main__':
    publisher()
