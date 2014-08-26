__author__ = 'eandersson'

import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.DEBUG)


def on_message(body, channel, header, properties):
    print "Message:", body
    channel.basic.ack(header['delivery_tag'])


def consumer():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.consume(on_message, 'simple_queue', no_ack=False)
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.stop_consuming()


if __name__ == '__main__':
    consumer()
