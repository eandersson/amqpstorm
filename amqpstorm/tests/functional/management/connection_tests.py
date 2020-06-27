import time

from amqpstorm import Connection
from amqpstorm.exception import AMQPConnectionError
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

        self.assertEqual(len(connections), 1)
        self.assertIsInstance(connections[0], dict)

    def test_api_connection_close(self):
        reason = 'travis-ci'
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD, timeout=1)

        self.assertEqual(len(api.connection.list()), 0,
                         'found an open connection, test will fail')

        connection = Connection(HOST, USERNAME, PASSWORD, timeout=1)

        connections = retry_function_wrapper(api.connection.list)
        self.assertIsNotNone(connections)

        self.assertEqual(len(connections), 1)

        for conn in api.connection.list():
            self.assertEqual(api.connection.close(conn['name'],
                                                  reason=reason), None)

        time.sleep(1)

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection was closed by remote server: '
            'CONNECTION_FORCED - %s' % reason,
            connection.check_for_errors
        )

        self.assertEqual(len(api.connection.list()), 0)

        connection.close()

    def test_api_connection_client_properties(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD, timeout=1)

        self.assertEqual(len(api.connection.list()), 0,
                         'found an open connection, test will fail')

        cp = {'platform': 'Atari', 'license': 'MIT'}
        connection = Connection(HOST, USERNAME, PASSWORD, timeout=1,
                                client_properties=cp)

        connections = retry_function_wrapper(api.connection.list)

        self.assertIsNotNone(connections)

        self.assertEqual(len(connections), 1)

        for conn in api.connection.list():
            self.assertEqual(conn['client_properties']['platform'], 'Atari')

        connection.close()
