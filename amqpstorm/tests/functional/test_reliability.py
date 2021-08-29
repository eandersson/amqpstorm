import imp
import sys
import threading
import time

from amqpstorm import AMQPConnectionError
from amqpstorm import AMQPMessageError
from amqpstorm import Connection
from amqpstorm import UriConnection
from amqpstorm import compatibility
from amqpstorm.tests import HOST
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import URI
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class ReliabilityFunctionalTests(TestFunctionalFramework):
    @setup(new_connection=False, queue=True)
    def test_functional_open_new_connection_loop(self):
        for _ in range(25):
            self.connection = self.connection = Connection(HOST, USERNAME,
                                                           PASSWORD)
            self.channel = self.connection.channel()

            # Make sure that it's a new channel.
            self.assertEqual(int(self.channel), 1)

            self.channel.queue.declare(self.queue_name)

            # Verify that the Connection/Channel has been opened properly.
            self.assertIsNotNone(self.connection._io.socket)
            self.assertIsNotNone(self.connection._io.poller)
            self.assertTrue(self.connection.is_open)

            self.channel.close()
            self.connection.close()

            # Verify that the Connection has been closed properly.
            self.assertTrue(self.connection.is_closed)
            self.assertIsNone(self.connection._io.socket)
            self.assertIsNone(self.connection._io.poller)
            self.assertFalse(self.connection._io._running.is_set())
            self.assertFalse(self.connection.exceptions)

    @setup(new_connection=False, queue=True)
    def test_functional_open_close_connection_loop(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD, lazy=True)
        for _ in range(25):
            self.connection.open()
            channel = self.connection.channel()

            # Make sure that it's a new channel.
            self.assertEqual(int(channel), 1)

            channel.queue.declare(self.queue_name)

            channel.close()

            # Verify that the Connection/Channel has been opened properly.
            self.assertIsNotNone(self.connection._io.socket)
            self.assertIsNotNone(self.connection._io.poller)
            self.assertTrue(self.connection.is_open)

            self.connection.close()

            # Verify that the Connection has been closed properly.
            self.assertTrue(self.connection.is_closed)
            self.assertIsNone(self.connection._io.socket)
            self.assertIsNone(self.connection._io.poller)
            self.assertFalse(self.connection._io._running.is_set())
            self.assertFalse(self.connection.exceptions)

    @setup(new_connection=True, new_channel=False, queue=True)
    def test_functional_close_gracefully_after_publish_mandatory_fails(self):
        for index in range(3):
            channel = self.connection.channel()

            # Try to publish 25 bad messages.
            for _ in range(25):
                try:
                    channel.basic.publish('', self.queue_name, '', None, True,
                                          False)
                except AMQPMessageError:
                    pass

            # Sleep for 0.1s to make sure RabbitMQ has time to catch up.
            time.sleep(0.1)

            self.assertTrue(channel.exceptions)

            channel.close()

    @setup(new_connection=False, queue=True)
    def test_functional_open_close_channel_loop(self):
        self.connection = self.connection = Connection(HOST, USERNAME,
                                                       PASSWORD)
        for _ in range(25):
            channel = self.connection.channel()

            # Verify that the Channel has been opened properly.
            self.assertTrue(self.connection.is_open)
            self.assertTrue(channel.is_open)

            # Channel id should be staying at 1.
            self.assertEqual(int(channel), 1)

            channel.close()

            # Verify that theChannel has been closed properly.
            self.assertTrue(self.connection.is_open)
            self.assertTrue(channel.is_closed)

    @setup(new_connection=False, queue=True)
    def test_functional_open_multiple_channels(self):
        self.connection = self.connection = Connection(HOST, USERNAME,
                                                       PASSWORD, lazy=True)

        for _ in range(5):
            channels = []
            self.connection.open()
            for index in range(10):
                channel = self.connection.channel()
                channels.append(channel)

                # Verify that the Channel has been opened properly.
                self.assertTrue(channel.is_open)
                self.assertEqual(int(channel), len(channels))
            self.connection.close()

    @setup(new_connection=False, queue=False)
    def test_functional_close_performance(self):
        """Make sure closing a connection never takes longer than ~1 seconds.

        :return:
        """
        for _ in range(10):
            self.connection = self.connection = Connection(HOST, USERNAME,
                                                           PASSWORD)
            start_time = time.time()
            self.connection.close()
            self.assertLess(time.time() - start_time, 3)

    @setup(new_connection=False, queue=False)
    def test_functional_close_after_channel_close_forced_by_server(self):
        """Make sure the channel is closed instantly when the remote server
        closes it.

        :return:
        """
        for _ in range(10):
            self.connection = self.connection = Connection(HOST, USERNAME,
                                                           PASSWORD)
            self.channel = self.connection.channel(rpc_timeout=360)

            start_time = time.time()
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name,
                                       exchange='invalid')
            self.channel.close()
            self.assertLess(time.time() - start_time, 3)

            start_time = time.time()
            self.connection.close()
            self.assertLess(time.time() - start_time, 3)

    @setup(new_connection=False)
    def test_functional_uri_connection(self):
        self.connection = UriConnection(URI)
        self.channel = self.connection.channel()
        self.assertTrue(self.connection.is_open)

    def test_functional_ssl_connection_without_ssl(self):
        restore_func = sys.modules['ssl']
        try:
            sys.modules['ssl'] = None
            imp.reload(compatibility)
            self.assertIsNone(compatibility.ssl)
            self.assertRaisesRegexp(
                AMQPConnectionError,
                'Python not compiled with support for TLSv1 or higher',
                Connection, HOST, USERNAME, PASSWORD, ssl=True
            )
        finally:
            sys.modules['ssl'] = restore_func
            imp.reload(compatibility)


class PublishAndConsume1kTest(TestFunctionalFramework):
    messages_to_send = 1000
    messages_consumed = 0
    lock = threading.Lock()

    def configure(self):
        self.disable_logging_validation()

    def publish_messages(self):
        for _ in range(self.messages_to_send):
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name)

    def consume_messages(self):
        channel = self.connection.channel()
        channel.basic.consume(queue=self.queue_name,
                              no_ack=False)
        for message in channel.build_inbound_messages(
                break_on_empty=False):
            self.increment_message_count()
            message.ack()
            if self.messages_consumed == self.messages_to_send:
                break

    def increment_message_count(self):
        with self.lock:
            self.messages_consumed += 1

    @setup(queue=True)
    def test_functional_publish_and_consume_1k_messages(self):
        self.channel.queue.declare(self.queue_name)

        publish_thread = threading.Thread(target=self.publish_messages, )
        publish_thread.daemon = True
        publish_thread.start()

        for _ in range(4):
            consumer_thread = threading.Thread(target=self.consume_messages, )
            consumer_thread.daemon = True
            consumer_thread.start()

        start_time = time.time()
        while self.messages_consumed != self.messages_to_send:
            if time.time() - start_time >= 60:
                break
            time.sleep(0.1)

        for channel in list(self.connection.channels.values()):
            channel.stop_consuming()
            channel.close()

        self.assertEqual(self.messages_consumed, self.messages_to_send,
                         'test took too long')


class Consume1kUntilEmpty(TestFunctionalFramework):
    messages_to_send = 1000

    def configure(self):
        self.disable_logging_validation()

    def publish_messages(self):
        for _ in range(self.messages_to_send):
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name)

    @setup(queue=True)
    def test_functional_publish_and_consume_until_empty(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()
        self.publish_messages()

        channel = self.connection.channel()
        channel.basic.consume(queue=self.queue_name,
                              no_ack=False)
        message_count = 0
        for message in channel.build_inbound_messages(break_on_empty=True):
            message_count += 1
            message.ack()

        result = channel.queue.declare(self.queue_name, passive=True)
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(message_count, self.messages_to_send,
                         'not all messages consumed')

        channel.close()
