__author__ = 'eandersson'
import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.DEBUG)


def on_message(message):
    """This function is called on message received.

    :param message:
    :return:
    """
    print("Message:", message.body)

    # Acknowledge that we handled the message without any issues.
    message.ack()


def consumer():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            # Declare the Queue, 'simple_queue'.
            channel.queue.declare('simple_queue')

            # Set QoS to 100.
            # This will allow the consume to only queue 100
            # message. This is a recommended setting, as it prevents the
            # consumer from keeping all of the messages in a queue for itself.
            channel.basic.qos(1)

            # Start consuming the queue 'simple_queue' using the callback
            # 'on_message' and last require the message to be acknowledged.
            channel.basic.consume(on_message, 'simple_queue', no_ack=False)

            try:
                # Start consiming the messages.
                channel.start_consuming(to_tuple=False)
            except KeyboardInterrupt:
                channel.close()


if __name__ == '__main__':
    consumer()
