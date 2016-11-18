from amqpstorm.management import ManagementApi
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.utility import TestFunctionalFramework
from amqpstorm.tests.utility import setup


class ApiChannelFunctionalTests(TestFunctionalFramework):
    @setup()
    def test_channel_get(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        channels = api.channel.list()

        self.assertIsInstance(channels, list)
        self.assertGreater(len(channels), 0)

        for channel in channels:
            self.assertIsInstance(api.channel.get(channel['name']), dict)

    @setup()
    def test_channel_list(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        result = api.channel.list()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], dict)
