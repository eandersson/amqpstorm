"""
A simple example publishing a message to RabbitMQ.
"""
import logging

from amqpstorm import Connection
from amqpstorm import Message

logging.basicConfig(level=logging.INFO)

with Connection('localhost', 'guest', 'guest') as connection:
    with connection.channel() as channel:
        # Declare the Queue, 'example_queue'.
        channel.queue.declare('example_queue')

        # Message Properties.
        properties = {
            'content_type': 'text/plain',
            'headers': {'key': 'value'}
        }

        # Create the message.
        message = Message.create(channel, 'Hello World!', properties)

        # Publish the message to a queue called, 'example_queue'.
        message.publish('example_queue')
