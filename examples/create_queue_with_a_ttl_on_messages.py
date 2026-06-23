import logging

from amqpstorm import Connection
from amqpstorm import Message

logging.basicConfig(level=logging.INFO)


with Connection('localhost', 'guest', 'guest') as connection:
    with connection.channel() as channel:
        # Declare a queue called, 'example_ttl_queue' with
        # the message ttl set to 6000ms.
        channel.queue.declare('example_ttl_queue', arguments={
            'x-message-ttl': 6000,
        })

        # Create a message.
        message = Message.create(channel, 'Hello World')

        # Publish the message to a queue.
        message.publish('example_ttl_queue')
