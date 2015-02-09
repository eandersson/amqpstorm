__author__ = 'eandersson'

import logging

from amqpstorm import Connection


logging.basicConfig(level=logging.DEBUG)

QUEUE_NAME = 'simple_queue'


def consumer():
    connection = Connection('127.0.0.1', 'guest', 'guest')
    channel = connection.channel()

    # Declare a queue.
    channel.queue.declare(QUEUE_NAME)

    # Publish something we can get.
    channel.basic.publish(body='Hello World!', routing_key=QUEUE_NAME)

    # Retrieve a single message.
    result = channel.basic.get(queue=QUEUE_NAME, no_ack=False)
    if result:
        # If we got a message, handle it.
        print('Message:', result['body'])

        # Mark the message as handle.
        channel.basic.ack(delivery_tag=result['method']['delivery_tag'])
    else:
        # The queue was empty.
        print("Queue '{0}' Empty.".format(QUEUE_NAME))

    channel.close()
    connection.close()


if __name__ == '__main__':
    consumer()
