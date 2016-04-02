__author__ = 'eandersson'

import logging

from amqpstorm import Connection
from amqpstorm import Message

logging.basicConfig(level=logging.DEBUG)


def publisher():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            properties = {
                'content_type': 'text/plain',
                'headers': {'key': 'value'}
            }
            message = Message.create(channel, 'Hello World!', properties)
            message.publish('simple_queue')


if __name__ == '__main__':
    publisher()
