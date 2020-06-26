import logging

from amqpstorm import Connection
from amqpstorm import Message

logging.basicConfig(level=logging.INFO)

with Connection('localhost', 'guest', 'guest') as connection:
    with connection.channel() as channel:
        # Declare a queue called, 'simple_queue'.
        channel.queue.declare('simple_queue')

        # Create the message with a expiration (time to live) set to 6000.
        message = Message.create(
            channel, 'Hello World',
            properties={"expiration": '6000'}
        )

        # Publish the message to the queue, 'simple_queue'.
        message.publish('simple_queue')
