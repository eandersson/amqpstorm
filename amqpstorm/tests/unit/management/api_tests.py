try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.management import ManagementApi


class FakeClient(object):
    """Fake HTTP client for Unit-Testing."""

    def __init__(self, on_get):
        self.on_get = on_get

    def get(self, path):
        return self.on_get(path)


class ApiTests(unittest.TestCase):
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
        api.http_client = FakeClient(on_get)

        top = api.top()
        self.assertIsInstance(top, list)
        self.assertIsInstance(top[0], dict)
        self.assertEqual(top[0]['node'], 'node1')
        self.assertIn('processes', top[0])
