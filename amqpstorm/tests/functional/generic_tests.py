import time

from amqpstorm import AMQPChannelError
from amqpstorm import AMQPMessageError
from amqpstorm import Channel
from amqpstorm import Message
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class GenericTest(TestFunctionalFramework):
    def configure(self):
        self.disable_logging_validation()

    @setup(queue=True)
    def test_functional_publish_and_get_five_messages(self):
        self.channel.queue.declare(self.queue_name)

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

    @setup(queue=True)
    def test_functional_publish_and_get_five_empty_messages(self):
        self.channel.queue.declare(self.queue_name)

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

    @setup(queue=True)
    def test_functional_publish_and_get_a_large_message(self):
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)

        body = self.message * 65536

        # Publish a single large message
        self.channel.basic.publish(body=body,
                                   routing_key=self.queue_name)

        payload = self.channel.basic.get(self.queue_name)
        self.assertEqual(body, payload.body)

    @setup(queue=True)
    def test_functional_publish_5_large_messages_and_consume(self):
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)

        body = self.message * 8192
        messages_to_publish = 5

        self.channel.basic.consume(queue=self.queue_name,
                                   no_ack=True)
        # Publish 5 Messages.
        for _ in range(messages_to_publish):
            self.channel.basic.publish(body=body,
                                       routing_key=self.queue_name)

        inbound_messages = []
        for message in self.channel.build_inbound_messages(
                break_on_empty=True):
            self.assertEqual(message.body, body)
            inbound_messages.append(message)
        self.assertEqual(len(inbound_messages), messages_to_publish)

    @setup(queue=True)
    def test_functional_publish_5_empty_messages(self):
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)

        body = b''
        messages_to_publish = 5

        self.channel.basic.consume(queue=self.queue_name,
                                   no_ack=True)
        # Publish 5 Messages.
        for _ in range(messages_to_publish):
            self.channel.basic.publish(body=body,
                                       routing_key=self.queue_name)

        inbound_messages = []
        for message in self.channel.build_inbound_messages(
                break_on_empty=True):
            self.assertEqual(message.body, body)
            inbound_messages.append(message)
        self.assertEqual(len(inbound_messages), messages_to_publish)

    @setup(queue=True)
    def test_functional_publish_5_large_messages_and_get(self):
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)

        body = self.message * 8192
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

    @setup(queue=True)
    def test_functional_publish_with_properties_and_get(self):
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)

        app_id = 'travis-ci'
        properties = {
            'headers': {
                'key': 1234567890,
                'alpha': 'omega'
            }
        }

        message = Message.create(self.channel,
                                 body=self.message,
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
        self.assertEqual(payload.app_id, app_id)
        self.assertEqual(payload.correlation_id, correlation_id)
        self.assertIsInstance(payload.properties['app_id'], str)
        self.assertIsInstance(payload.properties['correlation_id'], str)

        # Old way
        result = payload.to_dict()
        self.assertEqual(result['properties']['headers']['key'], 1234567890)
        self.assertEqual(result['properties']['headers']['alpha'], b'omega')
        self.assertIsInstance(result['properties']['app_id'], str)
        self.assertIsInstance(result['properties']['correlation_id'], str)
        self.assertEqual(result['properties']['app_id'], app_id)
        self.assertEqual(result['properties']['correlation_id'],
                         correlation_id)

    @setup(queue=True)
    def test_functional_publish_and_change_app_id(self):
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)
        message = Message.create(self.channel,
                                 body=self.message)
        message.app_id = 'travis-ci'
        message.publish(self.queue_name)

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

        payload = self.channel.basic.get(self.queue_name, no_ack=True)
        self.assertEqual(payload.app_id, app_id.decode('utf-8'))
        self.assertEqual(payload.correlation_id, correlation_id)
        self.assertIsInstance(payload.properties['app_id'], str)
        self.assertIsInstance(payload.properties['correlation_id'], str)

    @setup(queue=True)
    def test_functional_publish_and_consume_five_messages(self):
        self.channel.confirm_deliveries()
        self.channel.queue.declare(self.queue_name)

        for _ in range(5):
            self.channel.basic.publish(body=self.message,
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

    @setup(queue=True)
    def test_functional_generator_consume(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()
        for _ in range(5):
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name)
        self.channel.basic.consume(queue=self.queue_name,
                                   no_ack=True)
        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        # Store and inbound messages.
        inbound_messages = []

        for message in self.channel.build_inbound_messages(
                break_on_empty=True):
            self.assertIsInstance(message, Message)
            inbound_messages.append(message)

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

    @setup(queue=True)
    def test_functional_consume_and_redeliver(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

        def on_message_first(message):
            self.channel.stop_consuming()
            message.reject()

        self.channel.basic.consume(callback=on_message_first,
                                   queue=self.queue_name,
                                   no_ack=False)
        self.channel.process_data_events()

        # Store and inbound messages.
        inbound_messages = []

        # Close current channel and open a new one.
        self.channel.close()

        # Sleep for 0.1s to make sure RabbitMQ has time to catch up.
        time.sleep(0.1)

        channel = self.connection.channel()

        def on_message_second(message):
            inbound_messages.append(message)
            self.assertEqual(message.body, self.message)

        channel.basic.consume(callback=on_message_second,
                              queue=self.queue_name,
                              no_ack=True)
        channel.process_data_events()

        # Sleep for 0.1s to make sure RabbitMQ has time to catch up.
        time.sleep(0.1)

        start_time = time.time()
        while len(inbound_messages) != 1:
            if time.time() - start_time >= 30:
                break
            time.sleep(0.1)

        self.assertEqual(len(inbound_messages), 1)

    @setup(queue=True)
    def test_functional_redelivered(self):
        self.channel.queue.declare(self.queue_name)

        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name,
                                   mandatory=True)

        # Sleep for 0.1s to make sure RabbitMQ has time to catch up.
        time.sleep(0.1)

        message = self.channel.basic.get(self.queue_name, no_ack=False)
        message.reject(requeue=True)

        # Store and inbound messages.
        inbound_messages = []

        def on_message(message):
            inbound_messages.append(message)
            self.assertTrue(message.redelivered)

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=True)

        start_time = time.time()
        while len(inbound_messages) == 0:
            self.channel.process_data_events()
            if time.time() - start_time >= 30:
                break
            time.sleep(0.1)

        self.assertEqual(len(inbound_messages), 1)

    @setup(queue=True)
    def test_functional_get_and_redeliver(self):
        self.channel.queue.declare(self.queue_name)

        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

        # Sleep for 0.1s to make sure RabbitMQ has time to catch up.
        time.sleep(0.1)

        message = self.channel.basic.get(self.queue_name, no_ack=False)
        message.reject(requeue=True)

        # Sleep for 0.1s to make sure RabbitMQ has time to catch up.
        time.sleep(0.1)

        message = self.channel.basic.get(self.queue_name, no_ack=True)
        self.assertEqual(message.body, self.message)

    @setup(queue=True)
    def test_functional_publish_and_get(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

        message = self.channel.basic.get(self.queue_name, no_ack=True,
                                         auto_decode=False)
        self.assertIsInstance(message.body, bytes)
        self.assertEqual(message.body, self.message.encode('utf-8'))

    @setup(queue=True)
    def test_functional_publish_and_get_auto_decode(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

        message = self.channel.basic.get(self.queue_name, no_ack=False,
                                         auto_decode=True)
        self.assertIsInstance(message.body, str)
        self.assertEqual(message.body, self.message)

    @setup(queue=True)
    def test_functional_publish_and_consume_auto_decode(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

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

    @setup(queue=True)
    def test_functional_publish_and_consume(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()
        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

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

    @setup(queue=True)
    def test_functional_publish_and_confirm(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()

        self.channel.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        payload = self.channel.queue.declare(self.queue_name,
                                             passive=True)
        self.assertEqual(payload['message_count'], 1)

    @setup(queue=True)
    def test_functional_publish_confirm_to_invalid_queue(self):
        self.channel.confirm_deliveries()
        self.assertRaises(AMQPMessageError,
                          self.channel.basic.publish,
                          body=self.message,
                          exchange='amq.direct',
                          mandatory=True,
                          routing_key=self.queue_name)

    @setup(queue=True)
    def test_functional_publish_fail_publish_and_confirm(self):
        self.channel.confirm_deliveries()
        try:
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name,
                                       mandatory=True)
        except AMQPChannelError as why:
            self.assertTrue(self.channel.is_open)
            self.assertEqual(why.error_code, 312)
            if why.error_code == 312:
                self.channel.queue.declare(self.queue_name)

        result = self.channel.basic.publish(
            body=self.message,
            routing_key=self.queue_name,
            mandatory=True
        )
        self.assertTrue(result)

        payload = self.channel.queue.declare(self.queue_name,
                                             passive=True)
        self.assertEqual(payload['message_count'], 1)

    @setup(queue=True)
    def test_functional_publish_fail_recover(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()

        message = Message.create(self.channel, self.message)
        self.assertRaises(AMQPChannelError, message.publish, self.message,
                          exchange=self.exchange_name, mandatory=True)
        self.assertFalse(self.channel.is_open)

    @setup(queue=True)
    def test_functional_start_stop_consumer(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()

        for _ in range(5):
            self.channel.basic.publish(body=self.message,
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
