import logging

from amqpstorm import Connection
from amqpstorm import Message

logging.basicConfig(level=logging.INFO)


def publish_message(channel, body, queue, expiration="600"):
    # Create the message with a expiration (time to live).
    message = Message.create(channel, body,
                             properties={"expiration": expiration})

    # Publish the message to a queue.
    message.publish(queue)


if __name__ == '__main__':
    with Connection('127.0.0.1', 'guest', 'guest') as CONNECTION:
        with CONNECTION.channel() as CHANNEL:
            # Declare the Queue, 'simple_queue'.
            CHANNEL.queue.declare('simple_queue')

            # Publish the message to a queue called, 'simple_queue' with an
            # expiration set to 6000ms.
            publish_message(CHANNEL, 'Hello World', 'simple_queue', "6000")
