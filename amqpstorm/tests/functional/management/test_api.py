from amqpstorm.management import ManagementApi
from amqpstorm.management.exception import ApiConnectionError
from amqpstorm.management.exception import ApiError
from amqpstorm.tests import CAFILE
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import HTTPS_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework


class ApiFunctionalTests(TestFunctionalFramework):
    def test_api_url_with_slash(self):
        api = ManagementApi(HTTP_URL + '/', USERNAME, PASSWORD)
        self.assertEqual(api.aliveness_test('/'), {'status': 'ok'})

    def test_api_with_invalid_url(self):
        api = ManagementApi('abc', USERNAME, PASSWORD)
        self.assertRaisesRegex(
            ApiConnectionError,
            'Invalid URL',
            api.aliveness_test, '/'
        )

    def test_api_with_inaccessible(self):
        api = ManagementApi('http://192.168.1.50', USERNAME, PASSWORD,
                            timeout=0.1)
        self.assertRaisesRegex(
            ApiConnectionError,
            'Max retries exceeded with url',
            api.aliveness_test
        )

    def test_api_with_invalid_credentials(self):
        api = ManagementApi(HTTP_URL, 'travis_ci', PASSWORD)

        self.assertRaisesRegex(
            ApiError,
            '401 Client Error: Unauthorized',
            api.aliveness_test
        )

    def test_api_ssl_test(self):
        api = ManagementApi(HTTPS_URL, USERNAME, PASSWORD,
                            verify=CAFILE)
        self.assertEqual(api.aliveness_test(), {'status': 'ok'})

    def test_api_aliveness_test(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        self.assertEqual(api.aliveness_test(), {'status': 'ok'})

    def test_api_context_manager(self):
        with ManagementApi(HTTP_URL, USERNAME, PASSWORD) as api:
            self.assertEqual(api.aliveness_test(), {'status': 'ok'})

    def test_api_overview(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        result = api.overview()

        self.assertIsInstance(result, dict)
        self.assertIn('node', result)
        self.assertIn('management_version', result)

    def test_api_nodes(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        result = api.nodes()

        self.assertIsInstance(result, list)
        self.assertTrue(result)

    def test_api_whoami(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
        result = api.whoami()

        self.assertIsInstance(result, dict)
        self.assertEqual(result['name'], USERNAME)

        # RabbitMQ 3.9.X compatibility
        if isinstance(result['tags'], list):
            tag = result['tags'][0]
        else:
            tag = result['tags']
        self.assertEqual('administrator', tag)
