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

        # Enable server local transactions on channel.
        channel.tx.select()

        # Create the message.
        message = Message.create(channel, 'Hello World!', properties)

        # Publish the message to a queue called, 'example_queue'.
        message.publish('example_queue')

        # Commit the message(s).
        channel.tx.commit()

        # Alternatively rollback the message(s).
        # channel.tx.rollback()

        # You can also use the context manager.
        with channel.tx:
            message.publish('example_queue')
