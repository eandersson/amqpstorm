"""
Robust Consumer that will automatically re-connect on failure.
"""
import logging
import time

import amqpstorm
from amqpstorm import Connection

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()


class Consumer(object):
    def __init__(self, max_retries=None):
        self.max_retries = max_retries
        self.connection = None

    def create_connection(self):
        """Create a connection.

        :return:
        """
        attempts = 0
        while True:
            attempts += 1
            try:
                self.connection = Connection('localhost', 'guest', 'guest')
                break
            except amqpstorm.AMQPError as why:
                LOGGER.exception(why)
                if self.max_retries and attempts > self.max_retries:
                    break
                time.sleep(min(attempts * 2, 30))
            except KeyboardInterrupt:
                break

    def start(self):
        """Start the Consumers.

        :return:
        """
        if not self.connection:
            self.create_connection()
        while True:
            try:
                channel = self.connection.channel()
                channel.queue.declare('simple_queue')
                channel.basic.consume(self, 'simple_queue', no_ack=False)
                channel.start_consuming()
                if not channel.consumer_tags:
                    channel.close()
            except amqpstorm.AMQPError as why:
                LOGGER.exception(why)
                self.create_connection()
            except KeyboardInterrupt:
                self.connection.close()
                break

    def __call__(self, message):
        print("Message:", message.body)

        # Acknowledge that we handled the message without any issues.
        message.ack()

        # Reject the message.
        # message.reject()

        # Reject the message, and put it back in the queue.
        # message.reject(requeue=True)


if __name__ == '__main__':
    CONSUMER = Consumer()
    CONSUMER.start()
