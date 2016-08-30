import logging
import uuid

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.management import ManagementApi
from amqpstorm.tests.functional import HTTP_URL
from amqpstorm.tests.functional import USERNAME
from amqpstorm.tests.functional import PASSWORD

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)


class ApiUserFunctionalTests(unittest.TestCase):
    def test_api_user_get(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        user = api.user.get('guest')
        self.assertIsInstance(user, dict)
        self.assertEqual(user['name'], 'guest')
        self.assertEqual(user['tags'], 'administrator')

    def test_api_user_list(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        users = api.user.list()
        self.assertIsInstance(users, list)
        self.assertGreater(len(users), 0)

        for user in users:
            self.assertIsInstance(user, dict)
            self.assertIn('name', user)
            self.assertIn('password_hash', user)
            self.assertIn('tags', user)

    def test_api_user_create(self):
        username = 'travis_ci'
        password = str(uuid.uuid4())
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        try:
            self.assertIsNone(
                api.user.create(username, password, tags='monitor'))
            user = api.user.get(username)
            self.assertEqual(user['name'], username)
            self.assertEqual(user['tags'], 'monitor')
        finally:
            api.user.delete(username)

    def test_api_user_get_permission(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        permission = api.user.get_permission('guest', '/')
        self.assertIsInstance(permission, dict)
        self.assertEqual(permission['read'], '.*')
        self.assertEqual(permission['write'], '.*')
        self.assertEqual(permission['configure'], '.*')
        self.assertEqual(permission['user'], 'guest')
        self.assertEqual(permission['vhost'], '/')

    def test_api_user_get_permissions(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        permissions = api.user.get_permissions('guest')
        self.assertIsInstance(permissions, list)
        self.assertIsInstance(permissions[0], dict)
        self.assertEqual(permissions[0]['read'], '.*')
        self.assertEqual(permissions[0]['write'], '.*')
        self.assertEqual(permissions[0]['configure'], '.*')
        self.assertEqual(permissions[0]['user'], 'guest')
        self.assertEqual(permissions[0]['vhost'], '/')
