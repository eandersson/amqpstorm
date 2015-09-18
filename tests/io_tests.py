__author__ = 'eandersson'

import ssl
import socket
import logging

from mock import MagicMock

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import amqpstorm.io
from tests.utility import FakeConnection
from amqpstorm.io import IO
from amqpstorm.exception import *

logging.basicConfig(level=logging.DEBUG)


class IOTests(unittest.TestCase):
    def test_socket_close(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        io.set_state(IO.OPEN)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        io.close()

        self.assertIsNone(io.socket)
        self.assertTrue(io.is_closed)

    def test_create_socket(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        addresses = io._get_socket_addresses('localhost', 5672)
        sock_address_tuple = addresses[0]
        sock = io._create_socket(socket_family=sock_address_tuple[0])

        if hasattr(socket, 'socket'):
            self.assertIsInstance(sock, socket.socket)
        elif hasattr(socket, '_socketobject'):
            self.assertIsInstance(sock, socket._socketobject)

    def test_get_socket_address(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        addresses = io._get_socket_addresses('127.0.0.1', 5672)
        sock_address_tuple = addresses[0]

        self.assertEqual(sock_address_tuple[4],
                         ('127.0.0.1', 5672))

    def test_simple_receive(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        io.socket.recv.return_value = '12345'

        self.assertEqual(io._receive(), '12345')

    def test_receive_raises_socket_error(self):
        connection = FakeConnection()

        global result
        result = None

        def on_error(why):
            global result
            result = why

        io = IO(connection.parameters, on_error=on_error)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        io.socket.recv.side_effect = socket.error('error')
        io._receive()

        self.assertIsInstance(result, socket.error)

    def test_receive_raises_socket_timeout(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        io.socket.recv.side_effect = socket.timeout('timeout')
        io._receive()

    def test_simple_send_with_error(self):
        connection = FakeConnection()

        global result
        result = None

        def on_error(why):
            global result
            result = why

        io = IO(connection.parameters, on_error=on_error)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        io.poller = MagicMock(name='poller', spec=amqpstorm.io.Poller)
        io.socket.send.side_effect = socket.error('error')
        io.write_to_socket('12345')

        self.assertIsInstance(result, socket.error)

    def test_simple_send_zero_bytes_sent(self):
        connection = FakeConnection()

        global result
        result = None

        def on_error(why):
            global result
            result = why

        io = IO(connection.parameters, on_error=on_error)
        io.socket = MagicMock(name='socket', spec=socket.socket)
        io.poller = MagicMock(name='poller', spec=amqpstorm.io.Poller)
        io.socket.send.return_value = 0
        io.write_to_socket('afasffa')

        self.assertIsInstance(result, socket.error)

    def test_ssl_is_set(self):
        self.assertIsNotNone(amqpstorm.io.ssl)

    def test_default_ssl_version(self):
        if hasattr(ssl, 'PROTOCOL_TLSv1_2'):
            self.assertEqual(amqpstorm.io.DEFAULT_SSL_VERSION,
                             ssl.PROTOCOL_TLSv1_2)
        else:
            self.assertEqual(amqpstorm.io.DEFAULT_SSL_VERSION,
                             ssl.PROTOCOL_TLSv1)

    def test_ssl_connection_without_ssl_library(self):
        amqpstorm.io.ssl = None
        try:
            connection = FakeConnection()
            parameters = connection.parameters
            parameters['ssl'] = True
            io = IO(parameters)
            self.assertRaisesRegexp(AMQPConnectionError,
                                    'Python not compiled with SSL support',
                                    io.open, 'localhost', 1234)
        finally:
            amqpstorm.io.ssl = ssl

    def test_normal_connection_without_ssl_library(self):
        amqpstorm.io.ssl = None
        try:
            connection = FakeConnection()
            parameters = connection.parameters
            io = IO(parameters)
            self.assertRaisesRegexp(AMQPConnectionError,
                                    'Could not connect to localhost:1234',
                                    io.open, 'localhost', 1234)
        finally:
            amqpstorm.io.ssl = ssl
