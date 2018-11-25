import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.DEBUG)


def consume_until_queue_is_empty():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            while True:
                message = channel.basic.get('simple_queue')
                if not message:
                    print('Queue is empty')
                    break
                print(message.body)
                message.ack()


if __name__ == '__main__':
    consume_until_queue_is_empty()
