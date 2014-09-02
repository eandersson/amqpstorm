"""
    Threaded Consumer Example #2 -- Shared Connection

    This should be used when the program handles multiple queues.

        * Connection
            * Thread-1
                * ChannelX
                    - Callback1
            * Thread-2
                * ChannelX
                    - Callback2

"""
__author__ = 'eandersson'

import time
import random
import logging
import threading

from amqpstorm import Connection


logging.basicConfig(level=logging.DEBUG)


def random_wait():
    """ Generate a random number between 0.1 and 1.

    :return:
    """
    return round(random.uniform(0.1, 1.0), 10)


def on_message(body, channel, header, properties):
    print("Message Received:", body, threading.currentThread())
    channel.basic.ack(header['delivery_tag'])

    # Slow the process down to keep the screen from being flooded.
    time.sleep(random_wait())


def consume_messages(connection):
    channel = connection.channel()
    channel.basic.consume(on_message, 'simple_queue', no_ack=False)
    channel.basic.qos(prefetch_count=100)
    channel.start_consuming()


connection = Connection('127.0.0.1', 'guest', 'guest')

threads = []
for index in range(2):
    consumer_thread = threading.Thread(target=consume_messages,
                                       args=(connection,))
    consumer_thread.daemon = True
    consumer_thread.start()
    threads.append(consumer_thread)

while sum([thread.isAlive() for thread in threads]):
    time.sleep(1)

connection.close()
