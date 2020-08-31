import socket
import ssl

from mock import Mock

import amqpstorm.io
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.io import IO
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class IOTests(TestFramework):

    def test_io_socket_close(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        io.socket = Mock(name='socket', spec=socket.socket)
        io.close()

        self.assertIsNone(io.socket)

    def test_io_use_ssl_false(self):
        connection = FakeConnection()
        io = IO(connection.parameters)

        self.assertFalse(io.use_ssl)

    def test_io_use_ssl_true(self):
        connection = FakeConnection()
        connection.parameters['ssl'] = True
        io = IO(connection.parameters)

        self.assertTrue(io.use_ssl)

    def test_io_create_socket(self):
        connection = FakeConnection()
        io = IO(connection.parameters)

        self.assertFalse(io.use_ssl)

        addresses = io._get_socket_addresses()
        sock_address_tuple = addresses[0]
        sock = io._create_socket(socket_family=sock_address_tuple[0])

        if hasattr(socket, 'socket'):
            self.assertIsInstance(sock, socket.socket)

    def test_io_create_ssl_socket(self):
        connection = FakeConnection()
        connection.parameters['ssl'] = True
        io = IO(connection.parameters)

        self.assertTrue(io.use_ssl)

        addresses = io._get_socket_addresses()
        sock_address_tuple = addresses[0]
        sock = io._create_socket(socket_family=sock_address_tuple[0])

        if hasattr(socket, 'socket'):
            self.assertIsInstance(sock, socket.socket)
        if hasattr(ssl, 'SSLSocket'):
            self.assertIsInstance(sock, ssl.SSLSocket)

    def test_io_get_socket_address(self):
        connection = FakeConnection()
        connection.parameters['hostname'] = '127.0.0.1'
        connection.parameters['port'] = 5672
        io = IO(connection.parameters)
        addresses = io._get_socket_addresses()
        sock_address_tuple = addresses[0]

        self.assertEqual(sock_address_tuple[4],
                         ('127.0.0.1', 5672))

    def test_io_simple_receive(self):
        connection = FakeConnection()
        io = IO(connection.parameters)

        self.assertFalse(io.use_ssl)

        io.socket = Mock(name='socket', spec=socket.socket)
        io.socket.recv.return_value = '12345'

        self.assertEqual(io._receive(), '12345')

    def test_io_simple_ssl_receive(self):
        connection = FakeConnection()
        connection.parameters['ssl'] = True
        io = IO(connection.parameters)

        self.assertTrue(io.use_ssl)

        if hasattr(ssl, 'SSLObject'):
            io.socket = Mock(name='socket', spec=ssl.SSLObject)
        elif hasattr(ssl, 'SSLSocket'):
            io.socket = Mock(name='socket', spec=ssl.SSLSocket)

        io.socket.read.return_value = '12345'

        self.assertEqual(io._receive(), '12345')

    def test_io_simple_send_zero_bytes_sent(self):
        connection = FakeConnection()

        io = IO(connection.parameters, exceptions=connection.exceptions)
        io.socket = Mock(name='socket', spec=socket.socket)
        io.socket.send.return_value = 0
        io.write_to_socket(self.message)

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'connection/socket error',
            connection.check_for_errors
        )

    def test_io_set_ssl_context(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'context': ssl.create_default_context(),
            'server_hostname': 'localhost',
        }

        io = IO(connection.parameters)
        self.assertTrue(io._ssl_wrap_socket(socket.socket()))

    def test_io_set_ssl_verify_req(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'verify_mode': 'required'
        }

        io = IO(connection.parameters)
        sock = io._ssl_wrap_socket(socket.socket())
        self.assertEqual(sock.context.verify_mode, ssl.CERT_REQUIRED)

    def test_io_set_ssl_context_no_hostname_provided(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'context': ssl.create_default_context(),
        }

        io = IO(connection.parameters)
        self.assertRaises(ValueError, io._ssl_wrap_socket, socket.socket())

    def test_io_has_ipv6(self):
        restore_func = socket.getaddrinfo

        def mock_getaddrinfo(hostname, port, family, socktype):
            return [hostname, port, family, socktype]

        try:
            amqpstorm.io.socket.getaddrinfo = mock_getaddrinfo
            connection = FakeConnection()
            connection.parameters['hostname'] = 'localhost'
            connection.parameters['port'] = 1234
            parameters = connection.parameters
            io = IO(parameters)

            result = io._get_socket_addresses()
            self.assertEqual(result[2], socket.AF_UNSPEC)
            self.assertEqual(result[3], socket.SOCK_STREAM)
        finally:
            amqpstorm.io.socket.getaddrinfo = restore_func

    def test_io_has_ipv6_is_false(self):
        restore_func = socket.getaddrinfo
        restore_has_ipv6 = amqpstorm.io.socket.has_ipv6

        def mock_getaddrinfo(hostname, port, family, socktype):
            return [hostname, port, family, socktype]

        try:
            amqpstorm.io.socket.getaddrinfo = mock_getaddrinfo
            amqpstorm.io.socket.has_ipv6 = False
            connection = FakeConnection()
            connection.parameters['hostname'] = 'localhost'
            connection.parameters['port'] = 1234
            parameters = connection.parameters
            io = IO(parameters)

            result = io._get_socket_addresses()
            self.assertEqual(result[2], socket.AF_INET)
            self.assertEqual(result[3], socket.SOCK_STREAM)
        finally:
            amqpstorm.io.socket.getaddrinfo = restore_func
            amqpstorm.io.socket.has_ipv6 = restore_has_ipv6
