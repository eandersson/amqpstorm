import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.management import ManagementApi
from amqpstorm.management.exception import ApiError
from amqpstorm.management.exception import ApiConnectionError
from amqpstorm.tests.functional import HTTP_URL
from amqpstorm.tests.functional import USERNAME
from amqpstorm.tests.functional import PASSWORD

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)


class ApiFunctionalTests(unittest.TestCase):
    def test_api_url_with_slash(self):
        api = ManagementApi(HTTP_URL + '/', USERNAME, PASSWORD)
        self.assertEqual(api.aliveness_test('/'), {'status': 'ok'})

    def test_api_with_invalid_url(self):
        api = ManagementApi('abc', USERNAME, PASSWORD)
        expected_error = (
            "Invalid URL"
        )
        self.assertRaisesRegexp(ApiConnectionError, expected_error,
                                api.aliveness_test, '/')

    def test_api_with_inaccessible(self):
        api = ManagementApi('http://192.168.1.50', USERNAME, PASSWORD,
                            timeout=0.1)
        expected_error = (
            "Max retries exceeded with url"
        )
        self.assertRaisesRegexp(ApiConnectionError, expected_error,
                                api.aliveness_test)

    def test_api_with_invalid_credentials(self):
        api = ManagementApi(HTTP_URL, 'travis_ci', PASSWORD)
        expected_error = (
            "401 Client Error: Unauthorized"
        )
        self.assertRaisesRegexp(ApiError, expected_error, api.aliveness_test)

    def test_api_aliveness_test(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)
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
        self.assertEqual(result['name'], 'guest')
        self.assertEqual(result['tags'], 'administrator')
