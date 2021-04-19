from pamqp.heartbeat import Heartbeat
from pamqp.commands import Connection

import amqpstorm
from amqpstorm import AMQPConnectionError
from amqpstorm.channel0 import Channel0
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import FakeFrame
from amqpstorm.tests.utility import TestFramework


class Channel0FrameTests(TestFramework):
    def configure(self):
        self.connection = amqpstorm.Connection('localhost', 'guest', 'guest',
                                               lazy=True)

    def test_channel0_heartbeat(self):
        channel = Channel0(self.connection)
        self.assertIsNone(channel.on_frame(Heartbeat()))

    def test_channel0_on_close_frame(self):
        self.connection.set_state(self.connection.OPEN)
        channel = Channel0(self.connection)

        self.assertFalse(self.connection.exceptions)

        channel.on_frame(Connection.Close())

        self.assertTrue(self.connection.exceptions)
        self.assertTrue(self.connection.is_closed)
        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection was closed by remote server: ',
            self.connection.check_for_errors
        )

    def test_channel0_on_close_ok_frame(self):
        self.connection.set_state(self.connection.OPEN)
        channel = Channel0(self.connection)

        self.assertFalse(self.connection.is_closed)

        channel.on_frame(Connection.CloseOk())

        self.assertTrue(self.connection.is_closed)

    def test_channel0_is_blocked(self):
        channel = Channel0(self.connection)

        self.assertFalse(channel.is_blocked)

        channel.on_frame(Connection.Blocked('travis-ci'))

        self.assertTrue(channel.is_blocked)
        self.assertEqual(self.get_last_log(),
                         'Connection is blocked by remote server: travis-ci')

    def test_channel0_unblocked(self):
        channel = Channel0(self.connection)

        channel.on_frame(Connection.Blocked())

        self.assertTrue(channel.is_blocked)

        channel.on_frame(Connection.Unblocked())

        self.assertFalse(channel.is_blocked)
        self.assertEqual(self.get_last_log(),
                         'Connection is blocked by remote server: ')

    def test_channel0_open_ok_frame(self):
        channel = Channel0(self.connection)

        self.assertFalse(self.connection.is_open)

        channel.on_frame(Connection.OpenOk())

        self.assertTrue(self.connection.is_open)

    def test_channel0_start_frame(self):
        connection = FakeConnection()
        connection.parameters['username'] = 'guest'
        connection.parameters['password'] = 'guest'
        channel = Channel0(connection)

        properties = {
            'version': 0
        }

        channel.on_frame(Connection.Start(server_properties=properties))

        self.assertEqual(channel.server_properties, properties)
        self.assertIsInstance(connection.get_last_frame(), Connection.StartOk)

    def test_channel0_start_invalid_auth_frame(self):
        connection = FakeConnection()
        connection.parameters['username'] = 'guest'
        connection.parameters['password'] = 'guest'
        channel = Channel0(connection)

        channel.on_frame(Connection.Start(mechanisms='invalid'))

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Unsupported Security Mechanism\(s\): invalid',
            connection.check_for_errors
        )

    def test_channel0_tune_frame(self):
        connection = FakeConnection()
        connection.parameters['virtual_host'] = '/'
        channel = Channel0(connection)

        channel.on_frame(Connection.Tune())

        self.assertIsInstance(connection.get_last_frame(), Connection.TuneOk)
        self.assertIsInstance(connection.get_last_frame(), Connection.Open)

    def test_channel0_unhandled_frame(self):
        channel = Channel0(self.connection)

        channel.on_frame(FakeFrame())

        self.assertEqual(self.get_last_log(),
                         "[Channel0] Unhandled Frame: FakeFrame")
