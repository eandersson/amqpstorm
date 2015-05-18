__author__ = 'eandersson'

import logging

from amqpstorm import UriConnection

logging.basicConfig(level=logging.DEBUG)


def publisher():
    with UriConnection('amqp://guest:guest@127.0.0.1:5672/%2F') as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.publish(body='Hello World!',
                                  routing_key='simple_queue')


if __name__ == '__main__':
    publisher()
