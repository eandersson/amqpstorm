import logging
import time

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.management import ManagementApi
from amqpstorm.tests.functional import HOST
from amqpstorm.tests.functional import HTTP_URL
from amqpstorm.tests.functional import USERNAME
from amqpstorm.tests.functional import PASSWORD
from amqpstorm import Connection
from amqpstorm.exception import AMQPConnectionError

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)


class ApiConnectionFunctionalTests(unittest.TestCase):
    def test_api_connection_get(self):
        connection = Connection(HOST, USERNAME, PASSWORD)

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        for conn in api.connection.list():
            self.assertIsInstance(api.connection.get(conn['name']), dict)

        connection.close()

    def test_api_connection_list(self):
        connection = Connection(HOST, USERNAME, PASSWORD)

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        result = api.connection.list()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], dict)

        connection.close()

    def test_api_connection_close(self):
        reason = 'travis-ci'
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        connection = Connection(HOST, USERNAME, PASSWORD, timeout=1)
        self.assertEqual(len(api.connection.list()), 1)

        for conn in api.connection.list():
            self.assertEqual(api.connection.close(conn['name'],
                                                  reason=reason), None)

        time.sleep(1)

        error_message = (
            'Connection was closed by remote server: CONNECTION_FORCED - %s' %
            reason
        )
        self.assertRaisesRegexp(AMQPConnectionError, error_message,
                                connection.check_for_errors)

        self.assertEqual(len(api.connection.list()), 0)

        connection.close()
