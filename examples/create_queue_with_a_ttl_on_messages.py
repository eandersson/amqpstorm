import logging

from amqpstorm import Connection
from amqpstorm import Message

logging.basicConfig(level=logging.INFO)


def publish_message(channel, body, queue):
    # Create a message.
    message = Message.create(channel, body)

    # Publish the message to a queue.
    message.publish(queue)


if __name__ == '__main__':
    with Connection('127.0.0.1', 'guest', 'guest') as CONNECTION:
        with CONNECTION.channel() as CHANNEL:
            # Declare the Queue, 'simple_queue' with a message ttl of 6000ms.
            CHANNEL.queue.declare('simple_ttl_queue', arguments={
                'x-message-ttl': 6000,
            })

            # Publish the message to a queue called, 'simple_queue' with an
            # expiration set to 6000ms.
            publish_message(CHANNEL, 'Hello World', 'simple_ttl_queue')
