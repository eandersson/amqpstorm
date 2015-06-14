__author__ = 'eandersson'

import logging

from amqpstorm import UriConnection

from examples import URI

logging.basicConfig(level=logging.DEBUG)


def publisher():
    with UriConnection(URI) as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.publish(body='Hello World!',
                                  routing_key='simple_queue')


if __name__ == '__main__':
    publisher()
