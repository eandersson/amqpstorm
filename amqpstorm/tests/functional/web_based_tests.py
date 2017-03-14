import time

from amqpstorm import Connection
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.tests import HOST
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.utility import TestFunctionalFramework
from amqpstorm.tests.utility import retry_function_wrapper
from amqpstorm.tests.utility import setup


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

    @setup()
    def test_functional_connection_forcefully_closed(self):
        connection_list = retry_function_wrapper(self.api.connection.list)
        self.assertIsNotNone(connection_list)

        for connection in connection_list:
            self.api.connection.close(connection['name'])

        time.sleep(0.1)

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection was closed by remote server: '
            'CONNECTION_FORCED - Closed via management api',
            self.channel.basic.publish, 'body', 'routing_key'
        )

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection was closed by remote server: '
            'CONNECTION_FORCED - Closed via management api',
            self.channel.check_for_errors
        )

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection was closed by remote server: '
            'CONNECTION_FORCED - Closed via management api',
            self.connection.check_for_errors
        )

    @setup(new_connection=False, queue=True)
    def test_functional_alternative_virtual_host(self):
        self.api.virtual_host.create(self.virtual_host_name)

        self.api.user.set_permission('guest', self.virtual_host_name)

        self.connection = Connection(HOST, USERNAME, PASSWORD,
                                     virtual_host=self.virtual_host_name,
                                     timeout=1)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.delete(self.queue_name)
        self.api.user.delete_permission('guest', self.virtual_host_name)
        self.api.virtual_host.delete(self.virtual_host_name)
