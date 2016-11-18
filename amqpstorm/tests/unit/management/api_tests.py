from amqpstorm.management import ManagementApi

from amqpstorm.tests.utility import FakeHTTPClient
from amqpstorm.tests.utility import TestFramework


class ApiTests(TestFramework):
    def test_api_top(self):
        def on_get(name):
            if name == 'nodes':
                return [{'name': 'node1'}]
            elif name == 'top/node1':
                return {
                    'node': 'node1',
                    'processes': [
                        {
                            'status': u'running'
                        }
                    ]
                }

        api = ManagementApi('url', 'guest', 'guest')
        api.http_client = FakeHTTPClient(on_get)

        top = api.top()
        self.assertIsInstance(top, list)
        self.assertIsInstance(top[0], dict)
        self.assertEqual(top[0]['node'], 'node1')
        self.assertIn('processes', top[0])
