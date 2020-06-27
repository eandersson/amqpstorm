import uuid

from amqpstorm.management import ManagementApi
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework


class ApiUserFunctionalTests(TestFunctionalFramework):
    def test_api_user_get(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        user = api.user.get(USERNAME)
        self.assertIsInstance(user, dict)
        self.assertEqual(user['name'], USERNAME)
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

        permission = api.user.get_permission(USERNAME, '/')
        self.assertIsInstance(permission, dict)
        self.assertEqual(permission['read'], '.*')
        self.assertEqual(permission['write'], '.*')
        self.assertEqual(permission['configure'], '.*')
        self.assertEqual(permission['user'], USERNAME)
        self.assertEqual(permission['vhost'], '/')

    def test_api_user_get_permissions(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        permissions = api.user.get_permissions(USERNAME)
        self.assertIsInstance(permissions, list)
        self.assertIsInstance(permissions[0], dict)
        self.assertEqual(permissions[0]['read'], '.*')
        self.assertEqual(permissions[0]['write'], '.*')
        self.assertEqual(permissions[0]['configure'], '.*')
        self.assertEqual(permissions[0]['user'], USERNAME)
        self.assertEqual(permissions[0]['vhost'], '/')
