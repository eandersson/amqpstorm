__author__ = 'eandersson'
import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.DEBUG)


def on_message(message):
    print("Message:", message.body)
    message.ack()


def consumer():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.consume(on_message, 'simple_queue', no_ack=False)

            try:
                channel.start_consuming(to_tuple=False)
            except KeyboardInterrupt:
                channel.close()


if __name__ == '__main__':
    consumer()
