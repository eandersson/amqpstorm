import logging

from amqpstorm import Connection


logging.basicConfig(level=logging.DEBUG)


def on_message(body, channel, header, properties):
    """This function is called on message received.

    :param body: Message body
    :param channel: Channel
    :param header: Message header
    :param properties: Message properties
    :return:
    """
    print("Message:", body)

    # Acknowledge that we handled the message without any issues.
    channel.basic.ack(delivery_tag=header['delivery_tag'])


def consumer():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            # Declare the Queue, 'simple_queue'.
            channel.queue.declare('simple_queue')

            # Set QoS to 100.
            # This will allow the consume to only queue 100
            # message. This is a recommended setting, as it prevents the
            # consumer from keeping all of the messages in a queue for itself.
            channel.basic.qos(100)

            # Start consuming the queue 'simple_queue' using the callback
            # 'on_message' and last require the message to be acknowledged.
            channel.basic.consume(on_message, 'simple_queue', no_ack=False)

            try:
                # Start consiming the messages.
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.close()


if __name__ == '__main__':
    consumer()