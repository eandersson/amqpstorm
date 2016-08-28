import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.management import ManagementApi
from amqpstorm.management import ApiError
from amqpstorm.tests.functional import HTTP_URL
from amqpstorm.tests.functional import USERNAME
from amqpstorm.tests.functional import PASSWORD

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)


class ApiExchangeFunctionalTests(unittest.TestCase):
    def test_api_exchange_get(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        self.assertIsInstance(api.exchange.get('amq.direct'), dict)

    def test_api_exchange_list(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        exchanges = api.exchange.list()

        self.assertIsInstance(exchanges, list)
        self.assertGreater(len(exchanges), 0)

        for exchange in exchanges:
            self.assertIsInstance(exchange, dict)
            self.assertIn('name', exchange)
            self.assertIn('type', exchange)
            self.assertIn('auto_delete', exchange)

    def test_api_exchange_list_all(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        exchanges = api.exchange.list(show_all=True)

        self.assertIsInstance(exchanges, list)
        self.assertGreater(len(exchanges), 0)

        for exchange in exchanges:
            self.assertIsInstance(exchange, dict)
            self.assertIn('name', exchange)
            self.assertIn('type', exchange)
            self.assertIn('auto_delete', exchange)

    def test_api_exchange_declare(self):
        exchange = 'test_api_exchange_declare'
        exchange_type = 'direct'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        try:
            self.assertIsNone(api.exchange.declare(exchange, exchange_type,
                                                   passive=False,
                                                   durable=True))

            result = api.exchange.get(exchange)
            self.assertIsInstance(result, dict)
            self.assertEqual(result['name'], exchange)
            self.assertEqual(result['type'], exchange_type)
            self.assertEqual(result['auto_delete'], False)
            self.assertEqual(result['durable'], True)
        finally:
            api.exchange.delete(exchange)

    def test_api_exchange_declare_passive(self):
        exchange = 'test_queue_declare_passive'

        expected_error_message = (
            'NOT-FOUND - The client attempted to work '
            'with a server entity that does not exist.'
        )

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        try:
            api.exchange.declare(exchange, passive=True)
        except ApiError as why:
            self.assertEqual(str(why), expected_error_message)
            self.assertEqual(why.error_type, 'NOT-FOUND')
            self.assertEqual(why.error_code, 404)

    def test_api_exchange_bind_and_unbind(self):
        source_name = 'amq.match'
        destination_name = 'amq.direct'
        routing_key = 'travis-ci'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        bindings = len(api.exchange.bindings(source_name))

        self.assertIsNone(api.exchange.bind(source=source_name,
                                            destination=destination_name,
                                            routing_key=routing_key,
                                            arguments=None))

        self.assertEqual(len(api.exchange.bindings(source_name)),
                         bindings + 1)

        self.assertIsNone(api.exchange.unbind(source=source_name,
                                              destination=destination_name,
                                              routing_key=routing_key))

        self.assertEqual(len(api.exchange.bindings(source_name)), bindings)
