__author__ = 'eandersson'

import logging

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD

logging.basicConfig(level=logging.DEBUG)


def on_message(message):
    print("Message:", message.body)
    message.ack()


def consumer():
    with Connection(HOST, USERNAME, PASSWORD) as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.consume(on_message, 'simple_queue', no_ack=False)

            try:
                channel.start_consuming(to_tuple=False)
            except KeyboardInterrupt:
                channel.close()


if __name__ == '__main__':
    consumer()
