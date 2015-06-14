"""
    Threaded Consumer Example #1 -- Shared Channel

    This should be used when the processing of the payload is heavy, but
    the same function handles all payloads.

        - Each Channel is limited to one callback function.

        * Connection
            * ChannelX
                * Thread-1
                    - Callback1
                * Thread-2
                    - Callback1

"""
__author__ = 'eandersson'

import time
import random
import logging
import threading

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD


logging.basicConfig(level=logging.DEBUG)


def random_wait():
    """ Generate a random number between 0.1 and 1.

    :return:
    """
    return round(random.uniform(0.1, 1.0), 10)


def on_message(body, channel, header, properties):
    print("Message Received:", body, threading.currentThread())
    channel.basic.ack(delivery_tag=header['delivery_tag'])

    # Slow the process down to keep the screen from being flooded.
    time.sleep(random_wait())


def consume_messages(channel):
    channel.start_consuming()


connection = Connection(HOST, USERNAME, PASSWORD)
channel = connection.channel()
channel.basic.qos(prefetch_count=100)
channel.basic.consume(on_message, 'simple_queue', no_ack=False)

threads = []
for index in range(2):
    consumer_thread = threading.Thread(target=consume_messages,
                                       args=(channel,))
    consumer_thread.daemon = True
    consumer_thread.start()
    threads.append(consumer_thread)

while sum([thread.isAlive() for thread in threads]):
    time.sleep(1)

connection.close()
