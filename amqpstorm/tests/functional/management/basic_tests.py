from amqpstorm.management import ManagementApi
from amqpstorm.message import Message
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.utility import TestFunctionalFramework
from amqpstorm.tests.utility import setup


class ApiBasicFunctionalTests(TestFunctionalFramework):
    @setup(queue=True)
    def test_api_basic_publish(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        api.queue.declare(self.queue_name)
        try:
            self.assertEqual(api.basic.publish(self.message, self.queue_name),
                             {'routed': True})
        finally:
            api.queue.delete(self.queue_name)

    @setup(queue=True)
    def test_api_basic_get_message(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        api.queue.declare(self.queue_name)
        self.assertEqual(api.basic.publish(self.message, self.queue_name),
                         {'routed': True})

        result = api.basic.get(self.queue_name, requeue=False)
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], Message)
        self.assertEqual(result[0].body, self.message)

    @setup(queue=True)
    def test_api_basic_get_message_to_dict(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        api.queue.declare(self.queue_name)
        self.assertEqual(api.basic.publish(self.message, self.queue_name),
                         {'routed': True})

        result = api.basic.get(self.queue_name, requeue=False, to_dict=True)
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], dict)
        self.assertEqual(result[0]['payload'], self.message)
