__author__ = 'eandersson'

import time
import logging
import threading

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD

logging.basicConfig(level=logging.DEBUG)


def send_messages(connection):
    start_time = time.time()
    channel = connection.channel()
    channel.queue.declare('simple_queue')
    messages_sent = 0
    while True:
        # If connection is blocked, wait before trying to publish again.
        if connection.is_blocked:
            time.sleep(1)
            continue

        # Increment message counter.
        messages_sent += 1

        # After 10,000 messages has been sent, stop publishing on
        # this thread.
        if connection.is_closed or messages_sent >= 10000:
            logging.info("[Channel #{0}] {1} Messages Sent in: {2}s"
                         .format(int(channel), messages_sent,
                                 time.time() - start_time))
            break

        # Publish a message to the queue.
        channel.basic.publish('Hey World!', 'simple_queue')


if __name__ == '__main__':
    CONNECTION = Connection(HOST, USERNAME, PASSWORD)

    THREADS = []
    for index in range(5):
        THREAD = threading.Thread(target=send_messages,
                                  args=(CONNECTION,))
        THREAD.daemon = True
        THREAD.start()
        THREADS.append(THREAD)

    while sum([thread.isAlive() for thread in THREADS]):
        time.sleep(1)

    CONNECTION.close()
