__author__ = 'eandersson'

import logging

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD

logging.basicConfig(level=logging.DEBUG)


def on_message(body, channel, header, properties):
    print("Message:", body)
    channel.basic.ack(delivery_tag=header['delivery_tag'])


def consumer():
    with Connection(HOST, USERNAME, PASSWORD) as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.consume(on_message, 'simple_queue', no_ack=False)
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.close()


if __name__ == '__main__':
    consumer()
