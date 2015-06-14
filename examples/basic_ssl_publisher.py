__author__ = 'eandersson'

import logging

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD


logging.basicConfig(level=logging.DEBUG)


def on_message(body, channel, header, properties):
    print("Message:", body)
    channel.basic.ack(header['delivery_tag'])


def consumer():
    connection = Connection(HOST, USERNAME, PASSWORD,
                            ssl=True, port=5671)
    channel = connection.channel()
    channel.queue.declare('simple_queue')
    channel.basic.publish(body='Hello World!', routing_key='simple_queue')
    channel.close()
    connection.close()


if __name__ == '__main__':
    consumer()
