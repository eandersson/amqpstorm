__author__ = 'eandersson'

import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.DEBUG)


def on_message(body, channel, header, properties):
    print("Message:", body)
    channel.basic.ack(header['delivery_tag'])


def consumer():
    connection = Connection('127.0.0.1', 'guest', 'guest',
                            ssl=True, port=5671)
    channel = connection.channel()
    channel.queue.declare('simple_queue')
    channel.basic.publish(body='Hello World!', routing_key='simple_queue')
    channel.close()
    connection.close()


if __name__ == '__main__':
    consumer()
