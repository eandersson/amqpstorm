import platform

from pamqp.heartbeat import Heartbeat
from pamqp.specification import Connection

import amqpstorm
from amqpstorm import AMQPConnectionError
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

    def test_channel0_send_start_ok(self):
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

    def test_channel0_send_tune_ok(self):
        connection = FakeConnection()
        channel = Channel0(connection)
        channel._send_tune_ok()

        self.assertTrue(connection.frames_out)

        channel_id, frame_out = connection.frames_out.pop()

        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Connection.TuneOk)

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
