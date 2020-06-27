from amqpstorm.management import ManagementApi
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME
from amqpstorm.tests.functional.utility import TestFunctionalFramework


class ApiUserFunctionalTests(TestFunctionalFramework):
    def test_api_virtual_host_get(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        virtual_host = api.virtual_host.get('/')
        self.assertIsInstance(virtual_host, dict)
        self.assertEqual(virtual_host['name'], '/')

    def test_api_virtual_host_list(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        virtual_hosts = api.virtual_host.list()
        self.assertIsInstance(virtual_hosts, list)
        self.assertGreater(len(virtual_hosts), 0)

        for vhost in virtual_hosts:
            self.assertIsInstance(vhost, dict)
            self.assertIn('name', vhost)

    def test_api_virtual_host_create(self):
        vhost_name = 'test_api_virtual_host_create'
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        try:
            self.assertIsNone(api.virtual_host.create(vhost_name))
            virtual_host = api.virtual_host.get(vhost_name)
            self.assertIsInstance(virtual_host, dict)
            self.assertEqual(virtual_host['name'], vhost_name)

            api.user.set_permission(USERNAME, vhost_name)
        finally:
            api.virtual_host.delete(vhost_name)

    def test_api_virtual_host_permissions(self):
        api = ManagementApi(HTTP_URL, USERNAME, PASSWORD)

        permissions = api.virtual_host.get_permissions('/')
        self.assertIsInstance(permissions, list)
        self.assertIsInstance(permissions[0], dict)
        self.assertEqual(permissions[0]['read'], '.*')
        self.assertEqual(permissions[0]['write'], '.*')
        self.assertEqual(permissions[0]['configure'], '.*')
        self.assertEqual(permissions[0]['user'], USERNAME)
        self.assertEqual(permissions[0]['vhost'], '/')
