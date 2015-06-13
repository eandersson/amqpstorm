__author__ = 'eandersson'

import logging
import platform

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pamqp.specification import Connection

from amqpstorm.channel0 import Channel0
from amqpstorm.exception import AMQPConnectionError

from tests.utility import FakeConnection


logging.basicConfig(level=logging.DEBUG)


class BasicChannel0Tests(unittest.TestCase):
    def test_client_properties(self):
        channel = Channel0(FakeConnection())
        result = channel._client_properties()

        information = 'See https://github.com/eandersson/amqp-storm'
        python_version = 'Python %s' % platform.python_version()

        self.assertIsInstance(result, dict)
        self.assertTrue(result['capabilities']['authentication_failure_close'])
        self.assertTrue(result['capabilities']['consumer_cancel_notify'])
        self.assertTrue(result['capabilities']['publisher_confirms'])
        self.assertTrue(result['capabilities']['connection.blocked'])
        self.assertTrue(result['capabilities']['basic.nack'])
        self.assertEqual(result['information'], information)
        self.assertEqual(result['platform'], python_version)

    def test_credentials(self):
        connection = FakeConnection()
        connection.parameters['username'] = 'guest'
        connection.parameters['password'] = 'password'
        channel = Channel0(connection)
        credentials = channel._credentials()

        self.assertEqual(credentials, '\0guest\0password')

    def test_close_connection(self):
        connection = FakeConnection()
        connection.set_state(connection.OPEN)
        channel = Channel0(connection)

        self.assertTrue(connection.is_open)
        channel._close_connection(
            Connection.Close(reply_text=b'',
                             reply_code=200)
        )
        self.assertEqual(connection.exceptions, [])
        self.assertTrue(connection.is_closed)

    def test_forcefully_closed_connection(self):
        connection = FakeConnection()
        connection.set_state(connection.OPEN)
        channel = Channel0(connection)
        channel._close_connection(
            Connection.Close(reply_text=b'',
                             reply_code=500)
        )
        self.assertTrue(connection.is_closed)
        self.assertRaises(AMQPConnectionError, connection.check_for_errors)

    def test_send_start_ok_frame(self):
        connection = FakeConnection()
        connection.parameters['username'] = 'guest'
        connection.parameters['password'] = 'password'
        channel = Channel0(connection)
        channel._send_start_ok_frame()

        self.assertNotEqual(connection.frames_out, [])
        channel_id, frame_out = connection.frames_out.pop()
        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Connection.StartOk)
        self.assertNotEqual(frame_out.locale, '')
        self.assertIsNotNone(frame_out.locale)

    def test_send_tune_ok_frame(self):
        connection = FakeConnection()
        channel = Channel0(connection)
        channel._send_tune_ok_frame()

        self.assertNotEqual(connection.frames_out, [])
        channel_id, frame_out = connection.frames_out.pop()
        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Connection.TuneOk)

    def test_send_close_connection_frame(self):
        connection = FakeConnection()
        channel = Channel0(connection)
        channel.send_close_connection_frame()

        self.assertNotEqual(connection.frames_out, [])
        channel_id, frame_out = connection.frames_out.pop()
        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_out, Connection.Close)
