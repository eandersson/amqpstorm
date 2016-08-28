import logging
import uuid

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.management import ManagementApi
from amqpstorm.message import Message
from amqpstorm.tests.functional import HTTP_URL
from amqpstorm.tests.functional import USERNAME
from amqpstorm.tests.functional import PASSWORD

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)


class ApiBasicFunctionalTests(unittest.TestCase):
    def test_api_basic_publish(self):
        message = str(uuid.uuid4())
        queue = 'test_api_basic_publish'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        api.queue.declare(queue)
        try:
            self.assertEqual(api.basic.publish(message, queue),
                             {'routed': True})
        finally:
            api.queue.delete(queue)

    def test_api_basic_get_message(self):
        message = str(uuid.uuid4())
        queue = 'test_api_basic_get_message'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        api.queue.declare(queue)
        try:
            self.assertEqual(api.basic.publish(message, queue),
                             {'routed': True})

            result = api.basic.get(queue, requeue=False)
            self.assertIsInstance(result, list)
            self.assertIsInstance(result[0], Message)
            self.assertEqual(result[0].body, message)

        finally:
            api.queue.delete(queue)

    def test_api_basic_get_message_to_dict(self):
        message = str(uuid.uuid4())
        queue = 'test_api_basic_get_message'

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        api.queue.declare(queue)
        try:
            self.assertEqual(api.basic.publish(message, queue),
                             {'routed': True})

            result = api.basic.get(queue, requeue=False, to_dict=True)
            self.assertIsInstance(result, list)
            self.assertIsInstance(result[0], dict)
            self.assertEqual(result[0]['payload'], message)

        finally:
            api.queue.delete(queue)
