import logging
import time

from amqpstorm import Connection
from amqpstorm.tests import HOST
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import retry_function_wrapper
from amqpstorm.tests.functional.utility import setup

LOGGER = logging.getLogger(__name__)


class WebFunctionalTests(TestFunctionalFramework):
    def configure(self):
        self.disable_logging_validation()

    @setup()
    def test_functional_client_properties(self):
        connections = retry_function_wrapper(self.api.connection.list)
        self.assertIsNotNone(connections)

        client_properties = connections[0]['client_properties']

        result = self.connection._channel0._client_properties()

        self.assertIsInstance(result, dict)
        self.assertEqual(result['information'],
                         client_properties['information'])
        self.assertEqual(result['product'], client_properties['product'])
        self.assertEqual(result['platform'], client_properties['platform'])

    @setup(queue=True)
    def test_functional_consume_web_message(self):
        self.channel.queue.declare(self.queue_name)
        self.api.basic.publish(body=self.message,
                               routing_key=self.queue_name)

        # Sleep for 1s to make sure RabbitMQ has time to catch up.
        time.sleep(1)

        result = self.channel.basic.get(self.queue_name)

        self.assertEqual(result.body, self.message)

    @setup(queue=True)
    def test_functional_remove_queue_while_consuming(self):
        self.channel.queue.declare(self.queue_name)
        for _ in range(10):
            self.api.basic.publish(body=self.message,
                                   routing_key=self.queue_name)

        self.channel.basic.consume(queue=self.queue_name, no_ack=True)

        queue_deleted = False
        messages_received = 0
        for _ in self.channel.build_inbound_messages(break_on_empty=True):
            messages_received += 1
            if not queue_deleted:
                self.api.queue.delete(self.queue_name)
                queue_deleted = True

        self.assertFalse(self.channel._inbound)

    @setup(new_connection=False, queue=True)
    def test_functional_alternative_virtual_host(self):
        self.api.virtual_host.create(self.virtual_host_name)

        self.api.user.set_permission(USERNAME, self.virtual_host_name)

        self.connection = Connection(HOST, USERNAME, PASSWORD,
                                     virtual_host=self.virtual_host_name,
                                     timeout=1)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.delete(self.queue_name)
        self.api.user.delete_permission(USERNAME, self.virtual_host_name)
        self.api.virtual_host.delete(self.virtual_host_name)
