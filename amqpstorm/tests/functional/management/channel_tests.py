import logging

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

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)


class ApiChannelFunctionalTests(unittest.TestCase):
    def test_channel_get(self):
        connection = Connection(HOST, USERNAME, PASSWORD)
        connection.channel()

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        channels = api.channel.list()

        self.assertIsInstance(channels, list)
        self.assertGreater(len(channels), 0)

        for channel in channels:
            self.assertIsInstance(api.channel.get(channel['name']), dict)

        connection.close()

    def test_channel_list(self):
        connection = Connection(HOST, USERNAME, PASSWORD)
        connection.channel()

        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        result = api.channel.list()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], dict)

        connection.close()
