import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.DEBUG)


def consumer():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')
            channel.basic.consume(queue='simple_queue', no_ack=False)
            for message in channel.build_inbound_messages():
                print(message.body)
                message.ack()


if __name__ == '__main__':
    consumer()
