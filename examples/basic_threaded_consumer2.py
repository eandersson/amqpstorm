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


def consume_messages(connection):
    channel = connection.channel()
    channel.basic.consume(on_message, 'simple_queue', no_ack=False)
    channel.basic.qos(prefetch_count=100)
    channel.start_consuming()


if __name__ == '__main__':
    CONNECTION = Connection(HOST, USERNAME, PASSWORD)

    THREADS = []
    for _ in range(2):
        THREAD = threading.Thread(target=consume_messages,
                                  args=(CONNECTION,))
        THREAD.daemon = True
        THREAD.start()
        THREADS.append(THREAD)

    while sum([thread.isAlive() for thread in THREADS]):
        time.sleep(1)

    CONNECTION.close()
