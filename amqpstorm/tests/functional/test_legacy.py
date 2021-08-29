import time

from amqpstorm import Channel
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class LegacyFunctionalTests(TestFunctionalFramework):
    def configure(self):
        self.disable_logging_validation()

    @setup(queue=True)
    def test_functional_start_stop_consumer_tuple(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()

        for _ in range(5):
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name)

        # Store and inbound messages.
        inbound_messages = []

        def on_message(body, channel, method, properties):
            self.assertIsInstance(body, (bytes, str))
            self.assertIsInstance(channel, Channel)
            self.assertIsInstance(properties, dict)
            self.assertIsInstance(method, dict)
            inbound_messages.append(body)
            if len(inbound_messages) >= 5:
                channel.stop_consuming()

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=True)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        self.channel.start_consuming(to_tuple=True)

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

    @setup(queue=True)
    def test_functional_publish_and_consume_five_messages_tuple(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.confirm_deliveries()

        for _ in range(5):
            self.channel.basic.publish(body=self.message,
                                       routing_key=self.queue_name)

        # Store and inbound messages.
        inbound_messages = []

        def on_message(body, channel, method, properties):
            self.assertEqual(body, self.message.encode('utf-8'))
            self.assertIsInstance(body, (bytes, str))
            self.assertIsInstance(channel, Channel)
            self.assertIsInstance(properties, dict)
            self.assertIsInstance(method, dict)
            inbound_messages.append(body)

        self.channel.basic.consume(callback=on_message,
                                   queue=self.queue_name,
                                   no_ack=True)

        # Sleep for 0.01s to make sure RabbitMQ has time to catch up.
        time.sleep(0.01)

        self.channel.process_data_events(to_tuple=True)

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
                break_on_empty=True,
                to_tuple=True):
            self.assertIsInstance(message, tuple)
            self.assertIsInstance(message[0], bytes)
            self.assertIsInstance(message[1], Channel)
            self.assertIsInstance(message[2], dict)
            self.assertIsInstance(message[3], dict)
            inbound_messages.append(message)

        # Make sure all five messages were downloaded.
        self.assertEqual(len(inbound_messages), 5)

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
            payload = self.channel.basic.get(self.queue_name, to_dict=True)
            self.assertIsInstance(payload, dict)
