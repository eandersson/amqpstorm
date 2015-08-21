__author__ = 'eandersson'

import ssl
import socket
import logging
import threading

from mock import MagicMock

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.io import IO
from amqpstorm import Connection
from amqpstorm.exception import *

from pamqp.body import ContentBody
from pamqp.specification import Basic as spec_basic

logging.basicConfig(level=logging.DEBUG)


class ConnectionTests(unittest.TestCase):
    def test_set_hostname(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection.parameters['username'], 'guest')

    def test_set_username(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection.parameters['username'], 'guest')

    def test_set_password(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection.parameters['username'], 'guest')

    def test_set_parameters(self):
        connection = Connection('127.0.0.1', 'guest', 'guest',
                                virtual_host='travis',
                                heartbeat=120,
                                timeout=180,
                                ssl=True,
                                ssl_options={
                                    'ssl_version': ssl.PROTOCOL_TLSv1
                                },
                                lazy=True)
        self.assertEqual(connection.parameters['virtual_host'], 'travis')
        self.assertEqual(connection.parameters['heartbeat'], 120)
        self.assertEqual(connection.parameters['timeout'], 180)
        self.assertEqual(connection.parameters['ssl'], True)
        self.assertEqual(connection.parameters['ssl_options']['ssl_version'],
                         ssl.PROTOCOL_TLSv1)

    def test_invalid_hostname(self):
        self.assertRaises(AMQPInvalidArgument, Connection, 1,
                          'guest', 'guest', lazy=True)

    def test_invalid_username(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          2, 'guest', lazy=True)
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          None, 'guest', lazy=True)

    def test_invalid_password(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 3, lazy=True)
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', None, lazy=True)

    def test_invalid_virtual_host(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', virtual_host=4, lazy=True)
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', virtual_host=None, lazy=True)

    def test_invalid_port(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', port='', lazy=True)
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', port=None, lazy=True)

    def test_invalid_heartbeat(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', heartbeat='5', lazy=True)
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', heartbeat=None, lazy=True)

    def test_invalid_timeout(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', timeout='6', lazy=True)
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', timeout=None, lazy=True)

    def test_server_is_blocked_default_value(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection.is_blocked, False)

    def test_server_properties_default_value(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection.server_properties, {})

    def test_fileno_property(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPENING)
        io = IO(connection.parameters)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        connection.io = io
        io.socket.fileno.return_value = 5
        self.assertEqual(connection.fileno, 5)

    def test_fileno_none_when_connection_closed(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertIsNone(connection.fileno)

    def test_close_state(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(Connection.OPEN)
        connection.close()
        self.assertTrue(connection.is_closed)

    def test_open_channel_on_closed_connection(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertRaises(AMQPConnectionError, connection.channel)

    def test_basic_read_buffer(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        cancel_ok_frame = spec_basic.CancelOk().marshal()

        self.assertEqual(connection._read_buffer(cancel_ok_frame), b'\x00')

    def test_handle_read_buffer_none_returns_none(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertIsNone(connection._read_buffer(None))

    def test_basic_handle_amqp_frame(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        cancel_ok_frame = spec_basic.CancelOk().marshal()

        self.assertEqual(connection._handle_amqp_frame(cancel_ok_frame),
                         (b'\x00', None, None))

    def test_handle_amqp_frame_none_returns_none(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        result = connection._handle_amqp_frame('')
        self.assertEqual(result[0], '')
        self.assertIsNone(result[1])
        self.assertIsNone(result[2])

    def test_wait_for_connection(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', timeout=5,
                                lazy=True)
        connection.set_state(connection.OPENING)
        io = IO(connection.parameters)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        connection.io = io

        def func(conn):
            conn.set_state(conn.OPEN)

        threading.Timer(function=func, interval=1, args=(connection,)).start()
        connection._wait_for_connection_to_open()

    def test_wait_for_connection_raises_on_timeout(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', timeout=1,
                                lazy=True)
        connection.set_state(connection.OPENING)
        io = IO(connection.parameters)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        connection.io = io
        self.assertRaises(AMQPConnectionError,
                          connection._wait_for_connection_to_open)
