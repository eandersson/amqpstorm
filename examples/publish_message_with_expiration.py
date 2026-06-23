import logging

from amqpstorm import Connection
from amqpstorm import Message

logging.basicConfig(level=logging.INFO)

with Connection('localhost', 'guest', 'guest') as connection:
    with connection.channel() as channel:
        # Declare a queue called, 'example_queue'.
        channel.queue.declare('example_queue')

        # Create the message with an expiration (time to live) set to 6000.
        message = Message.create(
            channel, 'Hello World',
            properties={"expiration": '6000'}
        )

        # Publish the message to the queue, 'example_queue'.
        message.publish('example_queue')
