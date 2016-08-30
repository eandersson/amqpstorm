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


class ApiQueueFunctionalTests(unittest.TestCase):
    def test_api_queue_get(self):
        queue_name = 'test_api_queue_get'
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        try:
            api.queue.declare(queue_name)

            queue = api.queue.get(queue_name)

            self.assertIsInstance(queue, dict)
            self.assertIn('name', queue)
            self.assertIn('auto_delete', queue)
        finally:
            api.queue.delete(queue_name)

    def test_api_queue_list(self):
        queue_name = 'test_api_queue_list'
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        try:
            api.queue.declare(queue_name)

            queues = api.queue.list()

            self.assertIsInstance(queues, list)
            self.assertGreater(len(queues), 0)

            for queue in queues:
                self.assertIsInstance(queue, dict)
                self.assertIn('name', queue)
                self.assertIn('state', queue)
                self.assertIn('auto_delete', queue)
        finally:
            api.queue.delete(queue_name)

    def test_api_queue_list_all(self):
        queue_name = 'test_api_queue_list_all'
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        try:
            api.queue.declare(queue_name)

            queues = api.queue.list(show_all=True)

            self.assertIsInstance(queues, list)
            self.assertGreater(len(queues), 0)

            for queue in queues:
                self.assertIsInstance(queue, dict)
                self.assertIn('name', queue)
                self.assertIn('state', queue)
                self.assertIn('auto_delete', queue)
        finally:
            api.queue.delete(queue_name)

    def test_api_queue_declare(self):
        queue_name = 'test_api_queue_declare'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        try:
            self.assertIsNone(api.queue.declare(queue_name, durable=True))

            result = api.queue.get(queue_name)
            self.assertIsInstance(result, dict)
            self.assertEqual(result['name'], queue_name)
            self.assertEqual(result['auto_delete'], False)
            self.assertEqual(result['durable'], True)
        finally:
            api.queue.delete(queue_name)

    def test_api_queue_declare_passive(self):
        queue = 'test_api_queue_declare_passive'

        expected_error_message = (
            'NOT-FOUND - The client attempted to work '
            'with a server entity that does not exist.'
        )

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        try:
            api.queue.declare(queue, passive=True)
        except ApiError as why:
            self.assertEqual(str(why), expected_error_message)
            self.assertEqual(why.error_type, 'NOT-FOUND')
            self.assertEqual(why.error_code, 404)

    def test_api_queue_delete(self):
        queue_name = 'test_api_queue_delete'
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        try:
            api.queue.declare(queue_name, durable=True)
            self.assertIsInstance(api.queue.get(queue_name), dict)
        finally:
            api.queue.delete(queue_name)

        try:
            api.queue.declare(queue_name, passive=True)
        except ApiError as why:
            self.assertEqual(why.error_code, 404)

    def test_api_queue_purge(self):
        queue_name = 'test_api_queue_purge'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        try:
            api.queue.declare(queue_name)
            self.assertIsNone(api.queue.purge(queue_name))
        finally:
            api.queue.delete(queue_name)

    def test_api_queue_bind(self):
        queue_name = 'test_api_queue_bind'
        exchange_name = 'amq.direct'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        try:
            api.queue.declare(queue_name)

            bindings = len(api.queue.bindings(queue_name))

            self.assertIsNone(api.queue.bind(queue=queue_name,
                                             exchange=exchange_name,
                                             routing_key=queue_name,
                                             arguments=None))

            self.assertEqual(len(api.queue.bindings(queue_name)), bindings + 1)
        finally:
            api.queue.delete(queue_name)

    def test_api_queue_unbind(self):
        queue_name = 'test_api_queue_bind'
        exchange_name = 'amq.direct'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        try:
            api.queue.declare(queue_name)

            bindings = len(api.queue.bindings(queue_name))

            api.queue.bind(queue=queue_name, exchange=exchange_name,
                           routing_key=queue_name, arguments=None)

            self.assertEqual(len(api.queue.bindings(queue_name)), bindings + 1)

            self.assertIsNone(api.queue.unbind(queue=queue_name,
                                               exchange=exchange_name,
                                               routing_key=queue_name))

            self.assertEqual(len(api.queue.bindings(queue_name)), bindings)
        finally:
            api.queue.delete(queue_name)
