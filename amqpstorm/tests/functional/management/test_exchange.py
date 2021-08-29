from amqpstorm.management import ApiError
from amqpstorm.management import ManagementApi
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class ApiExchangeFunctionalTests(TestFunctionalFramework):
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

    @setup(new_connection=False, exchange=True)
    def test_api_exchange_declare(self):
        exchange_type = 'direct'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        self.assertIsNone(api.exchange.declare(self.exchange_name,
                                               exchange_type,
                                               passive=False,
                                               durable=True))

        result = api.exchange.get(self.exchange_name)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['name'], self.exchange_name)
        self.assertEqual(result['type'], exchange_type)
        self.assertEqual(result['auto_delete'], False)
        self.assertEqual(result['durable'], True)

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

    def test_api_exchange_declare_passive_exists(self):
        exchange = 'test_queue_declare_passive_exists'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        api.exchange.declare(exchange)

        self.assertIsNotNone(api.exchange.declare(exchange, passive=True))

    @setup(new_connection=False, exchange=True)
    def test_api_exchange_delete(self):
        exchange_type = 'direct'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        api.exchange.declare(self.exchange_name,
                             exchange_type,
                             passive=False,
                             durable=True)

        self.assertIsNone(api.exchange.delete(self.exchange_name))

        self.assertRaisesRegexp(
            ApiError,
            'NOT-FOUND - The client attempted to work '
            'with a server entity that does not exist.',
            api.exchange.get, self.exchange_name
        )

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
