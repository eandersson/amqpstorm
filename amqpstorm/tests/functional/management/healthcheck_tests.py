from amqpstorm.management import ManagementApi
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class ApiHealthchecksFunctionalTests(TestFunctionalFramework):
    @setup()
    def test_healthtests_get(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        result = api.healthchecks.get()
        self.assertIsInstance(result, dict)
        self.assertEqual(result['status'], 'ok')

    @setup()
    def test_healthtests_get_with_node_name(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        node_name = api.overview()['contexts'][0]['node']

        result = api.healthchecks.get(node_name)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['status'], 'ok')
