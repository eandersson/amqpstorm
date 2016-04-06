import logging

from amqpstorm import Connection
from amqpstorm import Message

logging.basicConfig(level=logging.DEBUG)


def publisher():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            # Declare the Queue, 'simple_queue'.
            channel.queue.declare('simple_queue')

            # Message Properties.
            properties = {
                'content_type': 'text/plain',
                'headers': {'key': 'value'}
            }

            # Create the message.
            message = Message.create(channel, 'Hello World!', properties)

            # Publish the message to a queue called, 'simple_queue'.
            message.publish('simple_queue')


if __name__ == '__main__':
    publisher()
