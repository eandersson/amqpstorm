from amqpstorm.management.basic import Basic

from amqpstorm.tests.utility import FakeHTTPClient
from amqpstorm.tests.utility import TestFramework


class BasicTests(TestFramework):
    def test_basic_get_with_payload(self):
        def on_post_with_payload(name):
            return [{'payload': name}]

        api = Basic(FakeHTTPClient(on_post=on_post_with_payload))

        messages = api.get(queue='test')
        self.assertEqual(messages[0].body, 'queues/%2F/test/get')

    def test_basic_get_with_body(self):
        def on_post_with_body(name):
            return [{'body': name}]

        api = Basic(FakeHTTPClient(on_post=on_post_with_body))

        messages = api.get(queue='test')
        self.assertEqual(messages[0].body, 'queues/%2F/test/get')
