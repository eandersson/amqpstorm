import ssl

from amqpstorm import UriConnection
from amqpstorm.connection import DEFAULT_HEARTBEAT_INTERVAL
from amqpstorm.connection import DEFAULT_SOCKET_TIMEOUT
from amqpstorm.connection import DEFAULT_VIRTUAL_HOST
from amqpstorm.tests.utility import TestFramework


class UriConnectionTests(TestFramework):
    def test_uri_default(self):
        connection = UriConnection(
            'amqp://guest:guest@localhost:5672/%2F', lazy=True
        )

        self.assertEqual(connection.parameters['hostname'], 'localhost')
        self.assertEqual(connection.parameters['username'], 'guest')
        self.assertEqual(connection.parameters['password'], 'guest')
        self.assertEqual(connection.parameters['virtual_host'],
                         DEFAULT_VIRTUAL_HOST)
        self.assertEqual(connection.parameters['port'], 5672)
        self.assertEqual(connection.parameters['heartbeat'],
                         DEFAULT_HEARTBEAT_INTERVAL)
        self.assertEqual(connection.parameters['timeout'],
                         DEFAULT_SOCKET_TIMEOUT)
        self.assertFalse(connection.parameters['ssl'])

    def test_uri_ssl(self):
        connection = UriConnection(
            'amqps://guest:guest@localhost:5672/%2F', lazy=True
        )

        self.assertTrue(connection.parameters['ssl'])

    def test_uri_simple(self):
        connection = UriConnection(
            'amqps://localhost:5672/%2F', lazy=True
        )

        self.assertEqual(connection.parameters['hostname'], 'localhost')
        self.assertEqual(connection.parameters['username'], 'guest')
        self.assertEqual(connection.parameters['password'], 'guest')

    def test_uri_set_hostname(self):
        connection = UriConnection(
            'amqps://guest:guest@my-server:5672/%2F?'
            'heartbeat=360', lazy=True
        )

        self.assertIsInstance(connection.parameters['hostname'], str)
        self.assertEqual(connection.parameters['hostname'], 'my-server')

    def test_uri_set_username(self):
        connection = UriConnection(
            'amqps://username:guest@localhost:5672/%2F?'
            'heartbeat=360', lazy=True
        )

        self.assertIsInstance(connection.parameters['username'], str)
        self.assertEqual(connection.parameters['username'], 'username')

    def test_uri_set_password(self):
        connection = UriConnection(
            'amqps://guest:password@localhost:5672/%2F?'
            'heartbeat=360', lazy=True
        )

        self.assertIsInstance(connection.parameters['password'], str)
        self.assertEqual(connection.parameters['password'], 'password')

    def test_uri_set_port(self):
        connection = UriConnection(
            'amqps://guest:guest@localhost:5672/%2F', lazy=True
        )

        self.assertIsInstance(connection.parameters['port'], int)
        self.assertEqual(connection.parameters['port'], 5672)

    def test_uri_set_heartbeat(self):
        connection = UriConnection(
            'amqps://guest:lazy=True@localhost:5672/%2F?'
            'heartbeat=360', lazy=True
        )

        self.assertIsInstance(connection.parameters['heartbeat'], int)
        self.assertEqual(connection.parameters['heartbeat'], 360)

    def test_uri_set_timeout(self):
        connection = UriConnection(
            'amqps://guest:guest@localhost:5672/%2F?'
            'timeout=360', lazy=True
        )

        self.assertIsInstance(connection.parameters['timeout'], int)
        self.assertEqual(connection.parameters['timeout'], 360)

    def test_uri_set_virtual_host(self):
        connection = UriConnection(
            'amqps://guest:guest@localhost:5672/travis', lazy=True
        )

        self.assertIsInstance(connection.parameters['virtual_host'], str)
        self.assertEqual(connection.parameters['virtual_host'], 'travis')

    def test_uri_set_ssl(self):
        connection = UriConnection(
            'amqps://guest:guest@localhost:5671/%2F?'
            'ssl_version=protocol_tlsv1&cert_reqs=cert_required&'
            'keyfile=file.key&certfile=file.crt&'
            'ca_certs=travis-ci', lazy=True
        )

        self.assertTrue(connection.parameters['ssl'])
        self.assertEqual(connection.parameters['ssl_options']['ssl_version'],
                         ssl.PROTOCOL_TLSv1)
        self.assertEqual(connection.parameters['ssl_options']['cert_reqs'],
                         ssl.CERT_REQUIRED)
        self.assertEqual(connection.parameters['ssl_options']['keyfile'],
                         'file.key')
        self.assertEqual(connection.parameters['ssl_options']['certfile'],
                         'file.crt')
        self.assertEqual(connection.parameters['ssl_options']['ca_certs'],
                         'travis-ci')

    def test_uri_get_ssl_version(self):
        connection = UriConnection(
            'amqp://guest:guest@localhost:5672/%2F', lazy=True
        )

        self.assertEqual(ssl.PROTOCOL_TLSv1,
                         connection._get_ssl_version('protocol_tlsv1'))

    def test_uri_get_ssl_validation(self):
        connection = UriConnection(
            'amqps://guest:guest@localhost:5672/%2F', lazy=True
        )

        self.assertEqual(ssl.CERT_REQUIRED,
                         connection._get_ssl_validation('cert_required'))

    def test_uri_get_ssl_options(self):
        connection = UriConnection(
            'amqps://guest:guest@localhost:5671/%2F', lazy=True
        )
        ssl_kwargs = {
            'cert_reqs': ['cert_required'],
            'ssl_version': ['protocol_tlsv1'],
            'keyfile': ['file.key'],
            'certfile': ['file.crt']
        }
        ssl_options = connection._parse_ssl_options(ssl_kwargs)

        self.assertEqual(ssl_options['cert_reqs'], ssl.CERT_REQUIRED)
        self.assertEqual(ssl_options['ssl_version'], ssl.PROTOCOL_TLSv1)
        self.assertEqual(ssl_options['keyfile'], 'file.key')
        self.assertEqual(ssl_options['certfile'], 'file.crt')

    def test_uri_get_ssl_options_new_method(self):
        ssl_kwargs = {
            'cert_reqs': ssl.CERT_REQUIRED,
            'ssl_version': ssl.PROTOCOL_TLSv1,
            'keyfile': 'file.key',
            'certfile': 'file.crt'
        }
        connection = UriConnection(
            'amqps://guest:guest@localhost:5671/%2F?'
            'server_hostname=rmq.eandersson.net&certfile=file.crt',
            ssl_options=ssl_kwargs,
            lazy=True
        )

        ssl_options = connection.parameters.get('ssl_options')

        self.assertEqual(ssl_options['server_hostname'], 'rmq.eandersson.net')
        self.assertEqual(ssl_options['cert_reqs'], ssl.CERT_REQUIRED)
        self.assertEqual(ssl_options['ssl_version'], ssl.PROTOCOL_TLSv1)
        self.assertEqual(ssl_options['keyfile'], 'file.key')
        self.assertEqual(ssl_options['certfile'], 'file.crt')

    def test_uri_set_client_properties(self):
        cp = {'platform': 'Atari', 'license': 'MIT'}
        connection = UriConnection(
            'amqp://guest:guest@localhost:5672/%2F', lazy=True,
            client_properties=cp
        )

        self.assertIsInstance(connection.parameters['client_properties'], dict)
        self.assertEqual(connection.parameters['client_properties'], cp)
