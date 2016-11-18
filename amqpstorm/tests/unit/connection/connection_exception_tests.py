import ssl

from amqpstorm import AMQPInvalidArgument
from amqpstorm import Connection
from amqpstorm.tests.utility import TestFramework


class ConnectionExceptionTests(TestFramework):
    def test_connection_set_hostname(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertEqual(connection.parameters['username'], 'guest')

    def test_connection_set_username(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertEqual(connection.parameters['username'], 'guest')

    def test_connection_set_password(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertEqual(connection.parameters['username'], 'guest')

    def test_connection_set_parameters(self):
        connection = Connection(
            '127.0.0.1', 'guest', 'guest',
            virtual_host='travis',
            heartbeat=120,
            timeout=180,
            ssl=True,
            ssl_options={
                'ssl_version': ssl.PROTOCOL_TLSv1
            },
            lazy=True
        )

        self.assertEqual(connection.parameters['virtual_host'], 'travis')
        self.assertEqual(connection.parameters['heartbeat'], 120)
        self.assertEqual(connection.parameters['timeout'], 180)
        self.assertEqual(connection.parameters['ssl'], True)
        self.assertEqual(connection.parameters['ssl_options']['ssl_version'],
                         ssl.PROTOCOL_TLSv1)

    def test_connection_invalid_hostname(self):
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'hostname should be a string',
            Connection, 1, 'guest', 'guest', lazy=True
        )

    def test_connection_invalid_username(self):
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'username should be a string',
            Connection, '127.0.0.1', 2, 'guest', lazy=True
        )
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'username should be a string',
            Connection, '127.0.0.1', None, 'guest', lazy=True
        )

    def test_connection_invalid_password(self):
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'password should be a string',
            Connection, '127.0.0.1', 'guest', 3, lazy=True
        )
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'password should be a string',
            Connection, '127.0.0.1', 'guest', None, lazy=True
        )

    def test_connection_invalid_virtual_host(self):
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'virtual_host should be a string',
            Connection, '127.0.0.1', 'guest', 'guest', virtual_host=4,
            lazy=True
        )
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'virtual_host should be a string',
            Connection, '127.0.0.1', 'guest', 'guest', virtual_host=None,
            lazy=True
        )

    def test_connection_invalid_port(self):
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'port should be an integer',
            Connection, '127.0.0.1', 'guest', 'guest', port='', lazy=True
        )
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'port should be an integer',
            Connection, '127.0.0.1', 'guest', 'guest', port=None, lazy=True
        )

    def test_connection_invalid_heartbeat(self):
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'heartbeat should be an integer',
            Connection, '127.0.0.1', 'guest', 'guest', heartbeat='5',
            lazy=True
        )
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'heartbeat should be an integer',
            Connection, '127.0.0.1', 'guest', 'guest', heartbeat=None,
            lazy=True
        )

    def test_connection_invalid_timeout(self):
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'timeout should be an integer or float',
            Connection, '127.0.0.1', 'guest', 'guest', timeout='6', lazy=True
        )
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'timeout should be an integer or float',
            Connection, '127.0.0.1', 'guest', 'guest', timeout=None, lazy=True
        )

    def test_connection_invalid_timeout_on_channel(self):
        connection = Connection(
            '127.0.0.1', 'guest', 'guest', timeout=0.1,
            lazy=True
        )

        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'rpc_timeout should be an integer',
            connection.channel, None
        )
