__author__ = 'eandersson'

import time
import logging
import threading

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD


logging.basicConfig(level=logging.DEBUG)
connection = None
try:
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

            # Publish a message to the queue.
            channel.basic.publish('Hey World!', 'simple_queue')

            # After 10,000 messages has been sent, stop publishing on
            # this thread.
            if messages_sent >= 10000:
                logging.info("[Channel #{0}] Messages Sent in: {1}s"
                             .format(int(channel), time.time() - start_time))
                break

            # Increment message counter.
            messages_sent += 1

    connection = Connection(HOST, USERNAME, PASSWORD)

    threads = []
    for index in range(5):
        publisher_thread = threading.Thread(target=send_messages,
                                            args=(connection,))
        publisher_thread.daemon = True
        publisher_thread.start()
        publisher_thread.isAlive()
        threads.append(publisher_thread)

    while sum([thread.isAlive() for thread in threads]):
        time.sleep(1)

    connection.close()
except Exception as why:
    if not connection:
        print("General Exception:", why)
    else:
        for exception in connection.exceptions:
            print(exception)
