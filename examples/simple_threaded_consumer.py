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


logging.basicConfig(level=logging.DEBUG)


def random_wait():
    """ Generate a random number between 0.1 and 1.

    :return:
    """
    return round(random.uniform(0.1, 1.0), 10)


def on_message(body, channel, header, properties):
    print "Message Received:", body, threading.currentThread()
    channel.basic.ack(header['delivery_tag'])

    # Slow the process down to keep the screen from being flooded.
    time.sleep(random_wait())


def consume_messages(channel):
    channel.start_consuming()


connection = Connection('127.0.0.1', 'guest', 'guest')
channel = connection.channel()
channel.basic.qos(prefetch_count=100, global_=True)
channel.basic.consume(on_message, 'simple_queue', no_ack=False)

threads = []
for index in xrange(2):
    thread = threading.Thread(target=consume_messages,
                              args=(channel,))
    thread.daemon = True
    thread.start()
    threads.append(thread)

while sum([thread.isAlive() for thread in threads]):
    time.sleep(1)

connection.close()
