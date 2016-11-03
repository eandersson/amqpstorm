import logging
import time
import uuid

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm import Message
from amqpstorm import Channel
from amqpstorm import Connection
from amqpstorm import AMQPMessageError
from amqpstorm import AMQPChannelError
from amqpstorm.tests.functional import HOST
from amqpstorm.tests.functional import USERNAME
from amqpstorm.tests.functional import PASSWORD

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)


class PublishAndGetMessagesTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None
    message = str(uuid.uuid4())

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)

    def test_functional_publish_and_get_five_messages(self):
        # Publish 5 Messages.
        for _ in range(5):
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        # Get 5 messages.
        for _ in range(5):
            payload = self.channel.basic.get(self.queue_name)
            self.assertIsInstance(payload, Message)
            self.assertEqual(payload.body, self.message)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishAndGetEmptyMessagesTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)

    def test_functional_publish_and_get_five_empty_messages(self):
        # Publish 5 Messages.
        for _ in range(5):
            self.channel.basic.publish(body=b'',
                                       routing_key=self.queue_name)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        # Get 5 messages.
        inbound_messages = []
        for _ in range(5):
            payload = self.channel.basic.get(self.queue_name)
            self.assertIsInstance(payload, Message)
            self.assertEqual(payload.body, b'')
            inbound_messages.append(payload)

        self.assertEqual(len(inbound_messages), 5)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishAndGetLargeMessageTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)

    def test_functional_publish_and_get_large_message(self):
        body = str(uuid.uuid4()) * 65536

        # Publish a single large message
        self.channel.basic.publish(body=body,
                                   routing_key=self.queue_name)

        payload = self.channel.basic.get(self.queue_name)
        self.assertEqual(body, payload.body)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishLargeMessagesAndConsumeTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)

    def test_functional_publish_5_large_messages(self):
        body = str(uuid.uuid4()) * 8192
        messages_to_publish = 5

        self.channel.basic.consume(queue=self.queue_name,
                                   no_ack=True)
        # Publish 5 Messages.
        for _ in range(messages_to_publish):
            self.channel.basic.publish(body=body,
                                       routing_key=self.queue_name)

        inbound_messages = []
        for message in \
                self.channel.build_inbound_messages(break_on_empty=True):
            self.assertEqual(message.body, body)
            inbound_messages.append(message)
        self.assertEqual(len(inbound_messages), messages_to_publish)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishEmptyMessagesAndConsumeTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)

    def test_functional_publish_5_empty_messages(self):
        body = b''
        messages_to_publish = 5

        self.channel.basic.consume(queue=self.queue_name,
                                   no_ack=True)
        # Publish 5 Messages.
        for _ in range(messages_to_publish):
            self.channel.basic.publish(body=body,
                                       routing_key=self.queue_name)

        inbound_messages = []
        for message in \
                self.channel.build_inbound_messages(break_on_empty=True):
            self.assertEqual(message.body, body)
            inbound_messages.append(message)
        self.assertEqual(len(inbound_messages), messages_to_publish)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishLargeMessagesAndGetTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)

    def test_functional_publish_5_large_messages(self):
        body = str(uuid.uuid4()) * 8192
        messages_to_publish = 5

        # Publish 5 Messages.
        for _ in range(messages_to_publish):
            self.channel.basic.publish(body=body,
                                       routing_key=self.queue_name)

        inbound_messages = []
        for _ in range(messages_to_publish):
            message = self.channel.basic.get(self.queue_name,
                                             no_ack=True)
            self.assertEqual(message.body, body)
            inbound_messages.append(message)
        self.assertEqual(len(inbound_messages), messages_to_publish)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishWithPropertiesAndGetTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()

    def test_functional_publish_with_properties_and_get(self):
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
        message.publish(routing_key=self.queue_name)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        # New way
        payload = self.channel.basic.get(self.queue_name)
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
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishMessageAndResend(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()
        message = Message.create(self.channel,
                                 body=str(uuid.uuid4()))
        message.app_id = 'travis-ci'
        message.publish(self.queue_name)

    def test_functional_publish_with_properties_and_get(self):
        message = self.channel.basic.get(self.queue_name, no_ack=True)

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
        message.publish(routing_key=self.queue_name)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        # New way
        payload = self.channel.basic.get(self.queue_name, no_ack=True)
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
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishAndConsumeMessagesTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()

    def test_functional_publish_and_consume_five_messages(self):
        for _ in range(5):
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key=self.queue_name)

        # Store and inbound messages.
        inbound_messages = []

        def on_message(message):
            self.assertIsInstance(message.body, (bytes, str))
            self.assertIsInstance(message.channel, Channel)
            self.assertIsInstance(message.properties, dict)
            self.assertIsInstance(message.method, dict)
            inbound_messages.append(message)

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=True)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        self.channel.process_data_events()

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class GeneratorConsumeMessagesTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()
        for _ in range(5):
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key=self.queue_name)
        self.channel.basic.consume(queue=self.queue_name,
                                   no_ack=True)
        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

    def test_functional_generator_consume(self):
        # Store and inbound messages.
        inbound_messages = []
        for message in \
                self.channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(message, Message)
            inbound_messages.append(message)

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class ConsumeAndRedeliverTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None
    message = str(uuid.uuid4())

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

        def on_message(message):
            message.reject()

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=False)
        self.channel.process_data_events()

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

    def test_functional_consume_and_redeliver(self):
        # Store and inbound messages.
        inbound_messages = []

        def on_message(message):
            inbound_messages.append(message)
            self.assertEqual(message.body, self.message)
            message.ack()

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=False)
        self.channel.process_data_events()
        self.assertEqual(len(inbound_messages), 1)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class GetAndRedeliverTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None
    message = str(uuid.uuid4())

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)
        message = self.channel.basic.get(self.queue_name, no_ack=False)
        message.reject()
        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

    def test_functional_get_and_redeliver(self):
        message = self.channel.basic.get(self.queue_name, no_ack=False)
        self.assertEqual(message.body, self.message)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class GetWithoutAutoDecodeTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None
    message = str(uuid.uuid4())

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

    def test_functional_get_and_redeliver(self):
        message = self.channel.basic.get(self.queue_name, no_ack=False,
                                         auto_decode=False)
        self.assertIsInstance(message.body, bytes)
        self.assertEqual(message.body, self.message.encode('utf-8'))

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class GetWithAutoDecodeTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None
    message = str(uuid.uuid4())

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

    def test_functional_get_and_redeliver(self):
        message = self.channel.basic.get(self.queue_name, no_ack=False,
                                         auto_decode=True)
        self.assertIsInstance(message.body, str)
        self.assertEqual(message.body, self.message)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class ConsumeWithAutoDecodeTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None
    message = str(uuid.uuid4())

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

    def test_functional_get_and_redeliver(self):
        def on_message(message):
            self.assertIsInstance(message.body, str)
            self.assertEqual(message.body, self.message)
            message.channel.stop_consuming()

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=True)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        self.channel.start_consuming(auto_decode=True)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class ConsumeWithoutAutoDecodeTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None
    message = str(uuid.uuid4())

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

    def test_functional_get_and_redeliver(self):
        def on_message(message):
            self.assertIsInstance(message.body, bytes)
            self.assertEqual(message.body, self.message.encode('utf-8'))
            message.channel.stop_consuming()

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=True)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        self.channel.start_consuming(auto_decode=False)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublisherConfirmsTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()

    def test_functional_publish_and_confirm(self):
        self.channel.basic.publish(body=str(uuid.uuid4()),
                                   routing_key=self.queue_name)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        payload = self.channel.queue.declare(self.queue_name,
                                             passive=True)
        self.assertEqual(payload['message_count'], 1)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublisherConfirmFailsTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.confirm_deliveries()

    def test_functional_publish_confirm_to_invalid_queue(self):
        self.assertRaises(AMQPMessageError,
                          self.channel.basic.publish,
                          body=str(uuid.uuid4()),
                          exchange='amq.direct',
                          mandatory=True,
                          routing_key=self.queue_name)

    def tearDown(self):
        self.channel.close()
        self.connection.close()


class PublishFailAndFix(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.confirm_deliveries()

    def test_functional_publish_and_confirm(self):
        try:
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key=self.queue_name,
                                       mandatory=True)
        except AMQPChannelError as why:
            self.assertTrue(self.channel.is_open)
            self.assertEqual(why.error_code, 312)
            if why.error_code == 312:
                self.channel.queue.declare(self.queue_name)

        result = \
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key=self.queue_name,
                                       mandatory=True)
        self.assertTrue(result)

        payload = self.channel.queue.declare(self.queue_name,
                                             passive=True)
        self.assertEqual(payload['message_count'], 1)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class PublishAndFail(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()

    def test_functional_publish_and_confirm(self):
        message = Message.create(self.channel, 'hello world')
        self.assertRaises(AMQPChannelError, message.publish, 'hello_world',
                          exchange='fake_exchange', mandatory=True)
        self.assertFalse(self.channel.is_open)

    def tearDown(self):
        self.channel = self.connection.channel()
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()


class StartStopConsumeTest(unittest.TestCase):
    connection = None
    channel = None
    queue_name = None

    def setUp(self):
        self.queue_name = self.__class__.__name__
        self.connection = Connection(HOST, USERNAME, PASSWORD)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.purge(self.queue_name)
        self.channel.confirm_deliveries()

    def test_functional_start_stop_consumer(self):
        for _ in range(5):
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key=self.queue_name)

        # Store and inbound messages.
        inbound_messages = []

        def on_message(message):
            self.assertIsInstance(message.body, (bytes, str))
            self.assertIsInstance(message.channel, Channel)
            self.assertIsInstance(message.properties, dict)
            self.assertIsInstance(message.method, dict)
            inbound_messages.append(message)
            if len(inbound_messages) >= 5:
                message.channel.stop_consuming()

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=True)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        self.channel.start_consuming()

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

    def tearDown(self):
        self.channel.queue.delete(self.queue_name)
        self.channel.close()
        self.connection.close()
