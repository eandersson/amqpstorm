__author__ = 'eandersson'

import time
import logging
import threading

from amqpstorm import Connection


logging.basicConfig(level=logging.DEBUG)
connection = None
try:
    def send_messages(connection):
        start_time = time.time()
        channel = connection.channel()
        channel.confirm_deliveries()
        messages_sent = 0
        while True:
            channel.basic.publish('Hey World!', 'simple_queue')
            if messages_sent >= 100000:
                logging.info(
                    "Messages Sent in: {0}s".format(time.time() - start_time))
                break
            messages_sent += 1


    connection = Connection('127.0.0.1', 'guest', 'guest')
    channel = connection.channel()
    channel.queue.declare('simple_queue')
    #channel.queue.purge('simple_queue')

    threads = []
    for index in xrange(2):
        thread = threading.Thread(target=send_messages,
                                  args=(connection,))
        thread.daemon = True
        thread.start()
        thread.isAlive()
        threads.append(thread)

    while sum([thread.isAlive() for thread in threads]):
        time.sleep(1)

    connection.close()
except Exception as exc:
    if not connection:
        print "General Exception:", exc
    else:
        for exception in connection.exceptions:
            print exception