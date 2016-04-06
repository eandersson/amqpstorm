import logging
import threading
import time
import uuid

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm import Message
from amqpstorm import Channel
from amqpstorm import Connection
from amqpstorm import UriConnection
from amqpstorm import AMQPMessageError
from amqpstorm import AMQPChannelError

HOST = '127.0.0.1'
USERNAME = 'guest'
PASSWORD = 'guest'
URI = 'amqp://guest:guest@127.0.0.1:5672/%2F'

logging.basicConfig(level=logging.DEBUG)


class OpenCloseOpenCloseTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD, lazy=True)

    def test_open_close_loop(self):
        for _ in range(100):
            self.connection.open()
            self.channel = self.connection.channel()

            # Verify that the Connection/Channel has been opened properly.
            self.assertIsNotNone(self.connection._io.socket)
            self.assertIsNotNone(self.connection._io.poller)
            self.assertTrue(self.channel.is_open)
            self.assertTrue(self.connection.is_open)

            self.channel.queue.declare('test.open.close')
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key='test.open.close')
            self.channel.close()
            self.connection.close()

            # Verify that the Connection/Channel has been closed properly.
            self.assertTrue(self.channel.is_closed)
            self.assertTrue(self.connection.is_closed)
            self.assertIsNone(self.connection._io.socket)
            self.assertIsNone(self.connection._io.poller)

        time.sleep(0.1)

        self.assertEqual(threading.activeCount(), 1,
                         msg='Current Active threads: %s'
                             % threading._active)

    def tearDown(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.delete('test.open.close')
        self.channel.close()
        self.connection.close()


class PublishAndGetMessagesTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.get')
        self.channel.queue.purge('test.basic.get')

    def test_publish_and_get_five_messages(self):
        # Publish 5 Messages.
        for _ in range(5):
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key='test.basic.get')

        # Sleep for 0.5s to make sure RabbitMQ has time to catch up.
        time.sleep(0.5)

        # Get 5 messages.
        for _ in range(5):
            payload = self.channel.basic.get('test.basic.get', to_dict=False)
            self.assertIsInstance(payload, Message)

    def tearDown(self):
        self.channel.queue.delete('test.basic.get')
        self.channel.close()
        self.connection.close()


class Publish50kTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.50k')
        self.channel.queue.purge('test.basic.50k')

    def test_publish_50k_messages(self):
        body = str(uuid.uuid4())
        # Publish 5 Messages.
        for _ in range(50000):
            self.channel.basic.publish(body=body,
                                       routing_key='test.basic.50k')

        # Sleep for 2.5s to make sure RabbitMQ has time to catch up.
        time.sleep(2.5)

        result = self.channel.queue.declare(queue='test.basic.50k',
                                            passive=True)
        self.assertEqual(result['message_count'], 50000)

    def tearDown(self):
        self.channel.queue.delete('test.basic.50k')
        self.channel.close()
        self.connection.close()


class PublishWithPropertiesAndGetTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.properties')
        self.channel.queue.purge('test.basic.properties')
        self.channel.confirm_deliveries()

    def test_publish_with_properties_and_get(self):
        app_id = 'travis-ci'.encode('utf-8')
        properties = {
            'headers': {
                'key': 1234567890,
                'alpha': 'omega'
            }
        }

        message = Message.create(self.channel,
                                 body=str(uuid.uuid4()),
                                 properties=properties)
        # Assign Property app_id
        message.app_id = app_id

        # Check that it was set correctly.
        self.assertEqual(message.properties['app_id'], app_id)

        # Get Property Correlation Id
        correlation_id = message.correlation_id

        # Publish Message
        message.publish(routing_key='test.basic.properties')

        # Sleep for 0.5s to make sure RabbitMQ has time to catch up.
        time.sleep(0.5)

        # New way
        payload = self.channel.basic.get('test.basic.properties',
                                         to_dict=False)
        self.assertEqual(payload.properties['headers']['key'], 1234567890)
        self.assertEqual(payload.properties['headers']['alpha'], 'omega')
        self.assertEqual(payload.app_id, app_id.decode('utf-8'))
        self.assertEqual(payload.correlation_id, correlation_id)
        self.assertIsInstance(payload.properties['app_id'], str)
        self.assertIsInstance(payload.properties['correlation_id'], str)

        # Old way
        result = payload.to_dict()
        self.assertEqual(result['properties']['headers'][b'key'], 1234567890)
        self.assertEqual(result['properties']['headers'][b'alpha'], b'omega')
        self.assertIsInstance(result['properties']['app_id'], bytes)
        self.assertIsInstance(result['properties']['correlation_id'], bytes)
        self.assertEqual(result['properties']['app_id'], app_id)
        self.assertEqual(result['properties']['correlation_id'],
                         correlation_id.encode('utf-8'))

    def tearDown(self):
        self.channel.queue.delete('test.basic.properties')
        self.channel.close()
        self.connection.close()


class PublishMessageAndResend(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.resend')
        self.channel.queue.purge('test.basic.resend')
        self.channel.confirm_deliveries()
        message = Message.create(self.channel,
                                 body=str(uuid.uuid4()))
        message.app_id = 'travis-ci'
        message.publish('test.basic.resend')

    def test_publish_with_properties_and_get(self):
        message = self.channel.basic.get('test.basic.resend',
                                         to_dict=False, no_ack=True)

        # Check original app_id
        self.assertEqual(message.app_id, 'travis-ci')

        # Assign Property app_id
        app_id = 'travis-ci-2'.encode('utf-8')
        message.app_id = app_id

        # Check that it was set correctly.
        self.assertEqual(message.properties['app_id'], app_id)

        # Get Property Correlation Id
        correlation_id = message.correlation_id

        # Publish Message
        message.publish(routing_key='test.basic.resend')

        # Sleep for 0.5s to make sure RabbitMQ has time to catch up.
        time.sleep(0.5)

        # New way
        payload = self.channel.basic.get('test.basic.resend',
                                         to_dict=False, no_ack=True)
        self.assertEqual(payload.app_id, app_id.decode('utf-8'))
        self.assertEqual(payload.correlation_id, correlation_id)
        self.assertIsInstance(payload.properties['app_id'], str)
        self.assertIsInstance(payload.properties['correlation_id'], str)

        # Old way
        result = payload.to_dict()
        self.assertIsInstance(result['properties']['app_id'], bytes)
        self.assertIsInstance(result['properties']['correlation_id'], bytes)
        self.assertEqual(result['properties']['app_id'], app_id)
        self.assertEqual(result['properties']['correlation_id'],
                         correlation_id.encode('utf-8'))

    def tearDown(self):
        self.channel.queue.delete('test.basic.resend')
        self.channel.close()
        self.connection.close()


class PublishAndConsumeMessagesTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.consume')
        self.channel.queue.purge('test.basic.consume')
        self.channel.confirm_deliveries()

    def test_publish_and_consume_five_messages(self):
        for _ in range(5):
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key='test.basic.consume')

        # Store and inbound messages.
        inbound_messages = []

        def on_message(message):
            self.assertIsInstance(message.body, (bytes, str))
            self.assertIsInstance(message.channel, Channel)
            self.assertIsInstance(message.properties, dict)
            self.assertIsInstance(message.method, dict)
            inbound_messages.append(message)

        self.channel.basic.consume(callback=on_message,
                                   queue='test.basic.consume',
                                   no_ack=True)

        # Sleep for 0.5s to make sure RabbitMQ has time to catch up.
        time.sleep(0.5)

        self.channel.process_data_events(to_tuple=False)

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

    def tearDown(self):
        self.channel.queue.delete('test.basic.consume')
        self.channel.close()
        self.connection.close()


class GeneratorConsumeMessagesTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.generator')
        self.channel.queue.purge('test.basic.generator')
        self.channel.confirm_deliveries()
        for _ in range(5):
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key='test.basic.generator')
        self.channel.basic.consume(queue='test.basic.generator',
                                   no_ack=True)
        # Sleep for 0.5s to make sure RabbitMQ has time to catch up.
        time.sleep(0.5)

    def test_generator_consume(self):
        # Store and inbound messages.
        inbound_messages = []
        for message in \
                self.channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(message, Message)
            inbound_messages.append(message)

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

    def tearDown(self):
        self.channel.queue.delete('test.basic.generator')
        self.channel.close()
        self.connection.close()


class ConsumeAndRedeliverTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.consume.redeliver')
        self.channel.queue.purge('test.consume.redeliver')
        self.message = str(uuid.uuid4())
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key='test.consume.redeliver')

        def on_message(message):
            message.reject()

        self.channel.basic.consume(callback=on_message,
                                   queue='test.consume.redeliver',
                                   no_ack=False)
        self.channel.process_data_events(to_tuple=False)

        # Sleep for 0.5s to make sure RabbitMQ has time to catch up.
        time.sleep(0.5)

    def test_consume_and_redeliver(self):
        # Store and inbound messages.
        inbound_messages = []

        def on_message(message):
            inbound_messages.append(message)
            self.assertEqual(message.body, self.message)
            message.ack()

        self.channel.basic.consume(callback=on_message,
                                   queue='test.consume.redeliver',
                                   no_ack=False)
        self.channel.process_data_events(to_tuple=False)
        self.assertEqual(len(inbound_messages), 1)

    def tearDown(self):
        self.channel.queue.delete('test.consume.redeliver')
        self.channel.close()
        self.connection.close()


class GetAndRedeliverTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.get.redeliver')
        self.channel.queue.purge('test.get.redeliver')
        self.channel.confirm_deliveries()
        self.message = str(uuid.uuid4())
        self.channel.basic.publish(body=self.message,
                                   routing_key='test.get.redeliver')
        message = self.channel.basic.get('test.get.redeliver', no_ack=False,
                                         to_dict=False)
        message.reject()
        # Sleep for 0.5s to make sure RabbitMQ has time to catch up.
        time.sleep(0.5)

    def test_get_and_redeliver(self):
        message = self.channel.basic.get('test.get.redeliver', no_ack=False,
                                         to_dict=False)
        self.assertEqual(message.body, self.message)

    def tearDown(self):
        self.channel.queue.delete('test.get.redeliver')
        self.channel.close()
        self.connection.close()


class PublisherConfirmsTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.confirm')
        self.channel.queue.purge('test.basic.confirm')
        self.channel.confirm_deliveries()

    def test_publish_and_confirm(self):
        self.channel.basic.publish(body=str(uuid.uuid4()),
                                   routing_key='test.basic.confirm')

        # Sleep for 0.5s to make sure RabbitMQ has time to catch up.
        time.sleep(0.5)

        payload = self.channel.queue.declare('test.basic.confirm',
                                             passive=True)
        self.assertEqual(payload['message_count'], 1)

    def tearDown(self):
        self.channel.queue.delete('test.basic.confirm')
        self.channel.close()
        self.connection.close()


class PublisherConfirmFailsTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.confirm_deliveries()

    def test_publish_confirm_to_invalid_queue(self):
        self.assertRaises(AMQPMessageError,
                          self.channel.basic.publish,
                          body=str(uuid.uuid4()),
                          exchange='amq.direct',
                          mandatory=True,
                          routing_key='test.basic.confirm.fails')

    def tearDown(self):
        self.channel.close()
        self.connection.close()


class QueueCreateTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()

    def test_queue_create(self):
        # Create the Queue.
        self.channel.queue.declare('test.queue.create')

        # Confirm that the Queue was declared.
        self.channel.queue.declare('test.queue.create', passive=True)

    def tearDown(self):
        self.channel.queue.delete('test.queue.create')
        self.channel.close()
        self.connection.close()


class QueueDeleteTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.queue.delete')

    def test_queue_delete(self):
        # Delete the Queue.
        self.channel.queue.delete('test.queue.delete')

        # Confirm that the Queue was deleted.
        self.assertRaises(AMQPChannelError, self.channel.queue.declare,
                          'test.queue.delete', passive=True)

    def tearDown(self):
        self.channel.close()
        self.connection.close()


class ExchangeCreateTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()

    def test_exchange_create(self):
        # Create the Exchange.
        self.channel.exchange.declare('test.exchange.create')

        # Confirm that the Exchange was declared.
        self.channel.exchange.declare('test.exchange.create', passive=True)

    def tearDown(self):
        self.channel.exchange.delete('test.exchange.create')
        self.channel.close()
        self.connection.close()


class ExchangeDeleteTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.exchange.declare('test.exchange.delete')

    def test_exchange_delete(self):
        # Delete the Exchange.
        self.channel.exchange.delete('test.exchange.delete')

        # Confirm that the Exchange was deleted.
        self.assertRaises(AMQPChannelError, self.channel.exchange.declare,
                          'test.exchange.delete', passive=True)

    def tearDown(self):
        self.channel.close()
        self.connection.close()


class UriConnectionTest(unittest.TestCase):
    def test_uri_connection(self):
        self.connection = UriConnection(URI)
        self.channel = self.connection.channel()
        self.assertTrue(self.connection.is_open)
        self.channel.close()
        self.connection.close()


class PublishFailAndFix(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.confirm_deliveries()

    def test_publish_and_confirm(self):
        try:
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key='test.publish.fail.and.fix',
                                       mandatory=True)
        except AMQPChannelError as why:
            self.assertTrue(self.channel.is_open)
            self.assertEqual(why.error_code, 312)
            if why.error_code == 312:
                self.channel.queue.declare('test.publish.fail.and.fix')

        result = \
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key='test.publish.fail.and.fix',
                                       mandatory=True)
        self.assertTrue(result)

        payload = self.channel.queue.declare('test.publish.fail.and.fix',
                                             passive=True)
        self.assertEqual(payload['message_count'], 1)

    def tearDown(self):
        self.channel.queue.delete('test.publish.fail.and.fix')
        self.channel.close()
        self.connection.close()


class PublishAndFail(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.publish.and.fail')
        self.channel.confirm_deliveries()

    def test_publish_and_confirm(self):
        message = Message.create(self.channel, 'hello world')
        self.assertRaises(AMQPChannelError, message.publish, 'hello_world',
                          exchange='fake_exchange', mandatory=True)
        self.assertFalse(self.channel.is_open)

    def tearDown(self):
        self.channel = self.connection.channel()
        self.channel.queue.delete('test.publish.and.fail')
        self.channel.close()
        self.connection.close()
