__author__ = 'eandersson'

import uuid
import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm import Connection
from amqpstorm.exception import AMQPMessageError


logging.basicConfig(level=logging.DEBUG)


class PublishAndGetMessagesTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection('localhost', 'guest', 'guest')
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.get')
        self.channel.queue.purge('test.basic.get')

    def test_publish_and_get_five_messages(self):
        # Publish 5 Messages.
        for _ in range(5):
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key='test.basic.get')

        # Get 5 messages.
        for _ in range(5):
            payload = self.channel.basic.get('test.basic.get')
            self.assertIsInstance(payload, dict)

    def tearDown(self):
        self.channel.queue.delete('test.basic.get')
        self.channel.close()
        self.connection.close()


class PublishAndConsumeMessagesTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection('localhost', 'guest', 'guest')
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.consume')
        self.channel.queue.purge('test.basic.consume')

    def test_publish_and_consume_five_messages(self):
        for _ in range(5):
            self.channel.basic.publish(body=str(uuid.uuid4()),
                                       routing_key='test.basic.consume')

        # Store and inbound messages.
        inbound_messages = []

        def on_message(*args):
            inbound_messages.append(args)

        self.channel.basic.consume(callback=on_message,
                                   queue='test.basic.consume',
                                   no_ack=True)
        self.channel.process_data_events()

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

    def tearDown(self):
        self.channel.queue.delete('test.basic.consume')
        self.channel.close()
        self.connection.close()


class ConsumeAndRedeliverTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection('localhost', 'guest', 'guest')
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
        self.connection = Connection('localhost', 'guest', 'guest')
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.get.redeliver')
        self.channel.queue.purge('test.get.redeliver')
        self.message = str(uuid.uuid4())
        self.channel.basic.publish(body=self.message,
                                   routing_key='test.get.redeliver')
        payload = self.channel.basic.get('test.get.redeliver', no_ack=False)
        self.channel.basic.reject(
            delivery_tag=payload['method']['delivery_tag']
        )

    def test_get_and_redeliver(self):
        payload = self.channel.basic.get('test.get.redeliver', no_ack=False)
        self.assertEqual(payload['body'].decode('utf-8'), self.message)

    def tearDown(self):
        self.channel.queue.delete('test.get.redeliver')
        self.channel.close()
        self.connection.close()


class PublisherConfirmsTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection('localhost', 'guest', 'guest')
        self.channel = self.connection.channel()
        self.channel.queue.declare('test.basic.confirm')
        self.channel.queue.purge('test.basic.confirm')
        self.channel.confirm_deliveries()

    def test_publish_and_confirm(self):
        self.channel.basic.publish(body=str(uuid.uuid4()),
                                   routing_key='test.basic.confirm')
        payload = self.channel.queue.declare('test.basic.confirm',
                                             passive=True)
        self.assertEqual(payload['message_count'], 1)

    def tearDown(self):
        self.channel.queue.delete('test.basic.confirm')
        self.channel.close()
        self.connection.close()


class PublisherConfirmFailsTest(unittest.TestCase):
    def setUp(self):
        self.connection = Connection('localhost', 'guest', 'guest')
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
