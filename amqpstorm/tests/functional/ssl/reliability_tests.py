import ssl
import threading
import time

from amqpstorm import Connection
from amqpstorm import UriConnection
from amqpstorm.tests import CAFILE
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import SSL_HOST
from amqpstorm.tests import SSL_URI
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class SSLReliabilityFunctionalTests(TestFunctionalFramework):
    @setup(new_connection=False, queue=True)
    def test_functional_ssl_open_new_connection_loop(self):
        ssl_options = {
            'context': ssl.create_default_context(cafile=CAFILE),
            'server_hostname': SSL_HOST
        }

        for _ in range(5):
            self.connection = self.connection = Connection(
                SSL_HOST, USERNAME, PASSWORD, port=5671, ssl=True,
                ssl_options=ssl_options, timeout=1)
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
    def test_functional_ssl_open_close_connection_loop(self):
        ssl_options = {
            'context': ssl.create_default_context(cafile=CAFILE),
            'server_hostname': SSL_HOST
        }
        self.connection = self.connection = Connection(
            SSL_HOST, USERNAME, PASSWORD, port=5671, ssl=True,
            ssl_options=ssl_options, timeout=1, lazy=True)

        for _ in range(5):
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

    @setup(new_connection=False, queue=False)
    def test_functional_ssl_open_close_channel_loop(self):
        ssl_options = {
            'context': ssl.create_default_context(cafile=CAFILE),
            'server_hostname': SSL_HOST
        }
        self.connection = self.connection = Connection(
            SSL_HOST, USERNAME, PASSWORD, port=5671, ssl=True,
            ssl_options=ssl_options)

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
    def test_functional_ssl_open_multiple_channels(self):
        ssl_options = {
            'context': ssl.create_default_context(cafile=CAFILE),
            'server_hostname': SSL_HOST
        }
        self.connection = self.connection = Connection(
            SSL_HOST, USERNAME, PASSWORD, port=5671, ssl=True,
            ssl_options=ssl_options, lazy=True)

        for _ in range(5):
            channels = []
            self.connection.open()
            for index in range(3):
                channel = self.connection.channel()
                channels.append(channel)

                # Verify that the Channel has been opened properly.
                self.assertTrue(channel.is_open)
                self.assertEqual(int(channel), len(channels))
            self.connection.close()

    @setup(new_connection=False, queue=False)
    def test_functional_ssl_close_performance(self):
        """Make sure closing a connection never takes longer than ~1 seconds.

        :return:
        """
        for _ in range(10):
            ssl_options = {
                'context': ssl.create_default_context(cafile=CAFILE),
                'server_hostname': SSL_HOST
            }
            self.connection = self.connection = Connection(
                SSL_HOST, USERNAME, PASSWORD, timeout=60, port=5671, ssl=True,
                ssl_options=ssl_options)

            start_time = time.time()
            self.connection.close()
            self.assertLess(time.time() - start_time, 3)

    @setup(new_connection=False)
    def test_functional_ssl_uri_connection(self):
        self.connection = UriConnection(SSL_URI)
        self.channel = self.connection.channel()
        self.assertTrue(self.connection.is_open)

    @setup(new_connection=False)
    def test_functional_ssl_uri_connection_with_context(self):
        ssl_options = {
            'context': ssl.create_default_context(cafile=CAFILE),
            'server_hostname': SSL_HOST
        }

        self.connection = UriConnection(SSL_URI, ssl_options=ssl_options)
        self.channel = self.connection.channel()
        self.assertTrue(self.connection.is_open)


class PublishAndConsume1kWithSSLTest(TestFunctionalFramework):
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

    @setup(new_connection=False, queue=False)
    def test_functional_publish_1k_with_ssl(self):
        ssl_options = {
            'context': ssl.create_default_context(cafile=CAFILE),
            'server_hostname': SSL_HOST
        }
        self.connection = self.connection = Connection(
            SSL_HOST, USERNAME, PASSWORD, port=5671, ssl=True,
            ssl_options=ssl_options)

        self.channel = self.connection.channel()
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


class Consume1kWithSSLUntilEmpty(TestFunctionalFramework):
    messages_to_send = 1000

    def configure(self):
        self.disable_logging_validation()

    def publish_messages(self):
        for _ in range(self.messages_to_send):
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name)

    @setup(new_connection=False, queue=True)
    def test_functional_consume_with_ssl_until_empty(self):
        ssl_options = {
            'context': ssl.create_default_context(cafile=CAFILE),
            'server_hostname': SSL_HOST
        }
        self.connection = self.connection = Connection(
            SSL_HOST, USERNAME, PASSWORD, port=5671, ssl=True,
            ssl_options=ssl_options)

        self.channel = self.connection.channel()
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
