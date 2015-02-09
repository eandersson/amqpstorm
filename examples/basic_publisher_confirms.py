__author__ = 'eandersson'

import logging

from amqpstorm import Connection
from amqpstorm.exception import AMQPMessageError

logging.basicConfig(level=logging.DEBUG)


def publisher():
    connection = Connection('127.0.0.1', 'guest', 'guest')
    channel = connection.channel()
    channel.queue.declare('simple_queue')
    channel.confirm_deliveries()
    try:
        success = channel.basic.publish(body='Hello World!',
                                        routing_key='simple_queue',
                                        mandatory=True)
        # RabbitMQ could not publish the message.
        if not success:
            print('Unable to send message.')
            return

    # Internal error handling the message.
    except AMQPMessageError as why:
        print('Unable to send message: {0}'.format(why))
        return

    print('Message successfully sent.')


if __name__ == '__main__':
    publisher()
