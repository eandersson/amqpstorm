__author__ = 'eandersson'

import logging

from amqpstorm import Message
from amqpstorm import Connection
from examples import HOST
from examples import USERNAME
from examples import PASSWORD


logging.basicConfig(level=logging.DEBUG)


def publisher():
    with Connection(HOST, USERNAME, PASSWORD) as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            message = Message.create(channel, 'Hello World!',
                                     {'content_type': 'text/plain'})
            message.publish('simple_queue')


if __name__ == '__main__':
    publisher()
