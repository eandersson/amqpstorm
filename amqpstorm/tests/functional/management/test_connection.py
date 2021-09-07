import uuid

from amqpstorm import AMQPConnectionError
from amqpstorm import Connection
from amqpstorm.management import ManagementApi
from amqpstorm.tests import HOST
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import retry_function_wrapper
from amqpstorm.tests.functional.utility import setup


class ApiConnectionFunctionalTests(TestFunctionalFramework):
    @setup()
    def test_api_connection_get(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        connections = retry_function_wrapper(api.connection.list)
        self.assertTrue(connections)

        for conn in connections:
            self.assertIsInstance(api.connection.get(conn['name']), dict)

    @setup()
    def test_api_connection_list(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        connections = retry_function_wrapper(api.connection.list)
        self.assertIsNotNone(connections)

        self.assertGreater(len(connections), 0)
        self.assertIsInstance(connections[0], dict)

    def test_api_connection_client_properties(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD, timeout=1)

        client_properties = {'platform': 'Atari', 'license': 'MIT'}
        connection = Connection(HOST, USERNAME, PASSWORD, timeout=1,
                                client_properties=client_properties)

        connections = retry_function_wrapper(api.connection.list)

        self.assertIsNotNone(connections)
        self.assertGreater(len(connections), 0)

        atari_found = False
        for conn in api.connection.list():
            if conn['client_properties']['platform'] == 'Atari':
                atari_found = True
                break
        self.assertTrue(atari_found, 'Could not find custom client properties')

        connection.close()

    def test_api_connection_close(self):
        connection_id = str(uuid.uuid4())
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD, timeout=1)

        client_properties = {'platform': connection_id}
        connection = Connection(HOST, USERNAME, PASSWORD, timeout=1,
                                client_properties=client_properties)

        connections = retry_function_wrapper(api.connection.list)

        self.assertIsNotNone(connections)
        self.assertGreater(len(connections), 0)

        connection_found = False
        for conn in api.connection.list():
            if conn['client_properties']['platform'] != connection_id:
                continue
            connection_found = True
            api.connection.close(conn['name'], reason=connection_id)

        self.assertTrue(connection_found)
        self.assertRaisesRegex(
            AMQPConnectionError, 'connection closed',
            connection.channel, 1
        )
