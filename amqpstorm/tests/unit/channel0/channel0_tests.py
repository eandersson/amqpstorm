import platform

from pamqp.heartbeat import Heartbeat
from pamqp.commands import Connection

import amqpstorm
from amqpstorm import AMQPConnectionError
from amqpstorm.base import MAX_CHANNELS
from amqpstorm.base import MAX_FRAME_SIZE
from amqpstorm.channel0 import Channel0
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class Channel0Tests(TestFramework):
    def test_channel0_client_properties(self):
        channel = Channel0(FakeConnection())
        result = channel._client_properties()

        information = 'See https://github.com/eandersson/amqpstorm'
        python_version = 'Python %s (%s)' % (platform.python_version(),
                                             platform.python_implementation())

        self.assertIsInstance(result, dict)
        self.assertTrue(result['capabilities']['authentication_failure_close'])
        self.assertTrue(result['capabilities']['consumer_cancel_notify'])
        self.assertTrue(result['capabilities']['publisher_confirms'])
        self.assertTrue(result['capabilities']['connection.blocked'])
        self.assertTrue(result['capabilities']['basic.nack'])
        self.assertEqual(result['information'], information)
        self.assertEqual(result['platform'], python_version)

    def test_channel0_credentials(self):
        connection = FakeConnection()
        connection.parameters['username'] = 'guest'
        connection.parameters['password'] = 'password'
        channel = Channel0(connection)
        credentials = channel._plain_credentials()

        self.assertEqual(credentials, '\0guest\0password')

    def test_channel0_close_connection(self):
        connection = FakeConnection()
        connection.set_state(connection.OPEN)
        channel = Channel0(connection)

        self.assertTrue(connection.is_open)

        channel._close_connection(
            Connection.Close(reply_text=b'',
                             reply_code=200)
        )

        self.assertFalse(connection.exceptions)
        self.assertTrue(connection.is_closed)

    def test_channel0_forcefully_closed_connection(self):
        connection = amqpstorm.Connection('localhost', 'guest', 'guest',
                                          lazy=True)
        connection.set_state(connection.OPEN)
        channel = Channel0(connection)
        channel._close_connection(
            Connection.Close(reply_text=b'',
                             reply_code=500)
        )

        self.assertTrue(connection.is_closed)
        self.assertRaises(AMQPConnectionError, connection.check_for_errors)

    def test_channel0_send_start_ok_plain(self):
        connection = FakeConnection()
        connection.parameters['username'] = 'guest'
        connection.parameters['password'] = 'password'
        channel = Channel0(connection)
        channel._send_start_ok(Connection.Start(mechanisms=b'PLAIN'))

        self.assertTrue(connection.frames_out)

        channel_id, frame_out = connection.frames_out.pop()

        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Connection.StartOk)
        self.assertNotEqual(frame_out.locale, '')
        self.assertIsNotNone(frame_out.locale)

    def test_channel0_send_start_ok_external(self):
        connection = FakeConnection()
        channel = Channel0(connection)
        channel._send_start_ok(Connection.Start(mechanisms=b'EXTERNAL'))

        self.assertTrue(connection.frames_out)

        channel_id, frame_out = connection.frames_out.pop()

        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Connection.StartOk)
        self.assertNotEqual(frame_out.locale, '')
        self.assertIsNotNone(frame_out.locale)

    def test_channel0_send_tune_ok(self):
        connection = FakeConnection()
        channel = Channel0(connection)
        channel._send_tune_ok(Connection.Tune())

        self.assertTrue(connection.frames_out)

        channel_id, frame_out = connection.frames_out.pop()

        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Connection.TuneOk)

        self.assertEqual(frame_out.channel_max, MAX_CHANNELS)
        self.assertEqual(frame_out.frame_max, MAX_FRAME_SIZE)

    def test_channel0_send_tune_ok_negotiate(self):
        channel = Channel0(FakeConnection())
        channel._send_tune_ok(Connection.Tune(frame_max=MAX_FRAME_SIZE,
                                              channel_max=MAX_CHANNELS))

        self.assertEqual(channel.max_frame_size, MAX_FRAME_SIZE)
        self.assertEqual(channel.max_allowed_channels, MAX_CHANNELS)

    def test_channel0_send_tune_ok_negotiate_use_max(self):
        """Test to make sure that we use the highest acceptable value when
        the server returns zero.
        """
        channel = Channel0(FakeConnection())
        channel._send_tune_ok(Connection.Tune())

        self.assertEqual(channel.max_frame_size, MAX_FRAME_SIZE)
        self.assertEqual(channel.max_allowed_channels, MAX_CHANNELS)

    def test_channel0_send_tune_ok_negotiate_use_server(self):
        """Test to make sure that we use the highest acceptable value from
        the servers perspective.
        """
        channel = Channel0(FakeConnection())
        channel._send_tune_ok(Connection.Tune(
            frame_max=16384,
            channel_max=200
        ))

        self.assertEqual(channel.max_frame_size, 16384)
        self.assertEqual(channel.max_allowed_channels, 200)

    def test_channel0_send_heartbeat(self):
        connection = FakeConnection()
        channel = Channel0(connection)
        channel.send_heartbeat()

        self.assertTrue(connection.frames_out)

        channel_id, frame_out = connection.frames_out.pop()

        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Heartbeat)

    def test_channel0_do_not_send_heartbeat_when_connection_closed(self):
        connection = FakeConnection(state=FakeConnection.CLOSED)
        channel = Channel0(connection)
        channel.send_heartbeat()

        self.assertFalse(connection.frames_out)

    def test_channel0_send_close_connection(self):
        connection = FakeConnection()
        channel = Channel0(connection)
        channel.send_close_connection()

        self.assertTrue(connection.frames_out)

        channel_id, frame_out = connection.frames_out.pop()

        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Connection.Close)

    def test_channel0_invalid_authentication_mechanism(self):
        connection = amqpstorm.Connection('localhost', 'guest', 'guest',
                                          lazy=True)
        channel = Channel0(connection)
        channel._send_start_ok(
            Connection.Start(mechanisms='CRAM-MD5 SCRAM-SHA-1 SCRAM-SHA-256')
        )

        self.assertRaises(AMQPConnectionError, connection.check_for_errors)

    def test_channel0_override_client_credentials(self):
        client_properties = {'platform': 'Atari', 'license': 'MIT'}
        channel = Channel0(FakeConnection(), client_properties)
        result = channel._client_properties()

        information = 'See https://github.com/eandersson/amqpstorm'

        self.assertIsInstance(result, dict)

        self.assertTrue(result['capabilities']['authentication_failure_close'])
        self.assertTrue(result['capabilities']['consumer_cancel_notify'])
        self.assertTrue(result['capabilities']['publisher_confirms'])
        self.assertTrue(result['capabilities']['connection.blocked'])
        self.assertTrue(result['capabilities']['basic.nack'])
        self.assertEqual(result['information'], information)
        self.assertEqual(result['platform'], 'Atari')
        self.assertEqual(result['license'], 'MIT')
