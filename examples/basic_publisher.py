__author__ = 'eandersson'

import logging

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD


logging.basicConfig(level=logging.DEBUG)


def publisher():
    with Connection(HOST, USERNAME, PASSWORD) as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.publish(body='Hello World!',
                                  routing_key='simple_queue',
                                  properties={'content_type': 'text/plain',
                                              'headers': {'key': 'value'}})


if __name__ == '__main__':
    publisher()
