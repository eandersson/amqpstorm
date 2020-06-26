import logging

from amqpstorm import Connection

logging.basicConfig(level=logging.INFO)

with Connection('localhost', 'guest', 'guest') as connection:
    with connection.channel() as channel:
        channel.queue.declare('simple_queue')
        channel.basic.consume(queue='simple_queue', no_ack=False)
        for message in channel.build_inbound_messages():
            print(message.body)
            message.ack()
