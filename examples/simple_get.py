__author__ = 'eandersson'

import logging

from amqpstorm import Connection


logging.basicConfig(level=logging.DEBUG)


def consumer():
    connection = Connection('127.0.0.1', 'guest', 'guest')
    channel = connection.channel()
    channel.queue.declare('simple_queue')
    channel.basic.publish(body='Hello World!', routing_key='simple_queue')

    result = channel.basic.get(queue='simple_queue', no_ack=False)
    if result:
        print "Message:", result['body']
        channel.basic.ack(result['method']['delivery_tag'])
    else:
        print "Channel Empty."

    channel.close()
    connection.close()


if __name__ == '__main__':
    consumer()
