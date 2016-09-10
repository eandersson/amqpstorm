import logging

from amqpstorm import Connection


logging.basicConfig(level=logging.DEBUG)


def on_message(body, channel, method, properties):
    """This function is called when a message is received.

    :param bytes|str|unicode body: Message payload
    :param Channel channel: AMQPStorm Channel
    :param dict method: Message method
    :param dict properties: Message properties
    :return:
    """
    print("Message:", body)

    # Acknowledge that we handled the message without any issues.
    channel.basic.ack(delivery_tag=method['delivery_tag'])


def consumer():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
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
                # to_tuple equal to True means that messages consumed
                # are returned as tuple, rather than a Message object.
                channel.start_consuming(to_tuple=True)
            except KeyboardInterrupt:
                channel.close()


if __name__ == '__main__':
    consumer()
