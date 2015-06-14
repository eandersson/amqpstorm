__author__ = 'eandersson'

import logging

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD


logging.basicConfig(level=logging.DEBUG)


def consumer():
    with Connection(HOST, USERNAME, PASSWORD) as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.consume('simple_queue', no_ack=False)
            for message in channel.build_inbound_messages():
                print(message.body)
                message.ack()


if __name__ == '__main__':
    consumer()
