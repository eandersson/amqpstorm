"""
A simple example consuming messages from RabbitMQ.
"""
import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.INFO)


def on_message(message):
    """This function is called on message received.

    :param message:
    :return:
    """
    print("Message:", message.body)

    # Acknowledge that we handled the message without any issues.
    message.ack()

    # Reject the message.
    # message.reject()

    # Reject the message, and put it back in the queue.
    # message.reject(requeue=True)


with Connection('localhost', 'guest', 'guest') as connection:
    with connection.channel() as channel:
        # Declare the Queue, 'simple_queue'.
        channel.queue.declare('simple_queue')

        # Set QoS to 100.
        # This will limit the consumer to only prefetch a 100 messages.

        # This is a recommended setting, as it prevents the
        # consumer from keeping all of the messages in a queue to itself.
        channel.basic.qos(100)

        # Start consuming the queue 'simple_queue' using the callback
        # 'on_message' and last require the message to be acknowledged.
        channel.basic.consume(on_message, 'simple_queue', no_ack=False)

        try:
            # Start consuming messages.
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.close()
