from amqpstorm.management import ManagementApi
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import retry_function_wrapper
from amqpstorm.tests.functional.utility import setup


class ApiChannelFunctionalTests(TestFunctionalFramework):
    @setup()
    def test_channel_get(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        channels = retry_function_wrapper(api.channel.list)
        self.assertIsNotNone(channels)

        for channel in channels:
            self.assertIsInstance(api.channel.get(channel['name']), dict)

    @setup()
    def test_channel_list(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        channels = retry_function_wrapper(api.channel.list)
        self.assertIsNotNone(channels)

        self.assertIsInstance(channels, list)
        self.assertIsInstance(channels[0], dict)

        self.channel.close()
