import requests

from amqpstorm.management.http_client import ApiError
from amqpstorm.management.http_client import HTTPClient
from amqpstorm.tests.utility import TestFramework


class FakeResponse(object):
    """Fake Requests Response for Unit-Testing."""

    def __init__(self, status_code=200, json=None, raises=None):
        self.status_code = status_code
        self._json = json
        self._raises = raises

    def raise_for_status(self):
        if not self._raises:
            return
        raise self._raises

    def json(self):
        if self._raises:
            raise self._raises
        return self._json


class ApiHTTPTests(TestFramework):
    def test_api_valid_json(self):
        fake_payload = {
            'hello': 'travis-ci'
        }

        response = HTTPClient._get_json_output(FakeResponse(json=fake_payload))

        self.assertEqual(response, {
            'hello': 'travis-ci'
        })

    def test_api_invalid_json(self):
        response = HTTPClient._get_json_output(
            FakeResponse(raises=ValueError)
        )
        self.assertIsNone(response)

    def test_api_http_standard_http_error(self):
        fake_payload = {
            'error': 'travis-ci'
        }

        self.assertRaisesRegexp(
            ApiError, 'travis-ci',
            HTTPClient._check_for_errors,
            FakeResponse(raises=requests.HTTPError('travis-ci')),
            fake_payload
        )

    def test_api_http_non_standard_http_error(self):
        fake_payload = {
            'error': 'travis-ci'
        }

        self.assertRaisesRegexp(
            ApiError, 'travis-ci',
            HTTPClient._check_for_errors,
            FakeResponse(),
            fake_payload
        )
