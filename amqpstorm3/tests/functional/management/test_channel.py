from amqpstorm3.management import ManagementApi
from amqpstorm3.tests import HTTP_URL
from amqpstorm3.tests import PASSWORD
from amqpstorm3.tests import USERNAME
from amqpstorm3.tests.functional.utility import TestFunctionalFramework
from amqpstorm3.tests.functional.utility import retry_function_wrapper
from amqpstorm3.tests.functional.utility import setup


class ApiChannelFunctionalTests(TestFunctionalFramework):
    @setup()
    def test_channel_list(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        channels = retry_function_wrapper(api.channel.list)
        self.assertIsNotNone(channels)

        self.assertIsInstance(channels, list)
        self.assertIsInstance(channels[0], dict)

        self.channel.close()
