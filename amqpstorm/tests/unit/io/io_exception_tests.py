import select
import socket
from errno import EINTR
from errno import EWOULDBLOCK

import mock

from amqpstorm import AMQPConnectionError
from amqpstorm import compatibility
from amqpstorm.io import IO
from amqpstorm.io import Poller
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class IOExceptionTests(TestFramework):
    def test_io_shutdown_with_io_error(self):
        connection = FakeConnection()

        io = IO(connection.parameters)
        io._exceptions = []
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.shutdown.side_effect = OSError()
        io._close_socket()

    def test_io_receive_raises_socket_error(self):
        connection = FakeConnection()

        io = IO(connection.parameters, exceptions=connection.exceptions)
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.recv.side_effect = socket.error('travis-ci')
        io._receive()
        self.assertRaisesRegexp(
            AMQPConnectionError,
            'travis-ci',
            connection.check_for_errors
        )

    def test_io_receive_raises_ssl_want_read_error(self):
        connection = FakeConnection()

        io = IO(connection.parameters, exceptions=connection.exceptions)
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.recv.side_effect = compatibility.SSLWantReadError()
        io._receive()
        self.assertIsNone(connection.check_for_errors())

    def test_io_receive_does_not_raise_on_block(self):
        connection = FakeConnection()

        io = IO(connection.parameters, exceptions=connection.exceptions)
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.recv.side_effect = socket.error(EWOULDBLOCK)
        io._receive()
        self.assertIsNone(connection.check_for_errors())

    def test_io_receive_raises_socket_timeout(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.recv.side_effect = socket.timeout('timeout')
        io._receive()
        self.assertIsNone(connection.check_for_errors())

    def test_io_simple_send_with_error(self):
        connection = FakeConnection()

        io = IO(connection.parameters)
        io._exceptions = []
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.send.side_effect = socket.error('error')
        io.write_to_socket(self.message)

        self.assertIsInstance(io._exceptions[0], AMQPConnectionError)

    def test_io_simple_send_with_recoverable_error(self):
        connection = FakeConnection()
        self.raised = False

        def custom_raise(*_):
            if self.raised:
                return 1
            self.raised = True
            raise socket.error(EWOULDBLOCK)

        io = IO(connection.parameters)
        io._exceptions = []
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.send.side_effect = custom_raise
        io.write_to_socket(self.message)

        self.assertTrue(self.raised)
        self.assertFalse(io._exceptions)

    def test_io_simple_send_with_timeout_error(self):
        connection = FakeConnection()
        self.raised = False

        def custom_raise(*_):
            if self.raised:
                return 1
            self.raised = True
            raise socket.timeout()

        io = IO(connection.parameters)
        io._exceptions = []
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.send.side_effect = custom_raise
        io.write_to_socket(self.message)

        self.assertTrue(self.raised)
        self.assertFalse(io._exceptions)

    def test_io_simple_send_with_io_error(self):
        connection = FakeConnection()

        io = IO(connection.parameters)
        io._exceptions = []
        io.socket = None
        io.write_to_socket(self.message)

        self.assertTrue(io._exceptions)

    def test_io_ssl_connection_without_ssl_library(self):
        compatibility.SSL_SUPPORTED = False
        try:
            parameters = FakeConnection().parameters
            parameters['ssl'] = True
            io = IO(parameters)
            self.assertRaisesRegexp(
                AMQPConnectionError,
                'Python not compiled with support for TLSv1 or higher',
                io.open
            )
        finally:
            compatibility.SSL_SUPPORTED = True

    @mock.patch('amqpstorm.compatibility.SSL_SUPPORTED',
                return_value=False)
    def test_io_normal_connection_without_ssl_library(self, _):
        connection = FakeConnection()
        connection.parameters['hostname'] = 'localhost'
        connection.parameters['port'] = 1234
        parameters = connection.parameters
        io = IO(parameters)
        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Could not connect to localhost:1234 error: Connection refused',
            io.open
        )

    @mock.patch('socket.getaddrinfo',
                side_effect=socket.gaierror('could not connect'))
    def test_io_raises_gaierror(self, _):
        connection = FakeConnection()
        connection.parameters['hostname'] = 'localhost'
        connection.parameters['port'] = 1234
        parameters = connection.parameters
        io = IO(parameters)
        self.assertRaisesRegexp(
            AMQPConnectionError,
            'could not connect',
            io._get_socket_addresses
        )

    @mock.patch('amqpstorm.io.select.select',
                side_effect=select.error('travis-ci'))
    def test_io_poller_raises(self, _):
        exceptions = []
        poller = Poller(0, exceptions, 30)
        self.assertFalse(poller.is_ready)
        self.assertTrue(exceptions)

    @mock.patch('amqpstorm.io.select.select', side_effect=select.error(EINTR))
    def test_io_poller_eintr(self, _):
        exceptions = []
        poller = Poller(0, exceptions, 30)
        self.assertFalse(poller.is_ready)
        self.assertFalse(exceptions)

    def test_io_simple_receive_when_socket_not_set(self):
        connection = FakeConnection()
        io = IO(connection.parameters, exceptions=connection.exceptions)

        self.assertFalse(io.use_ssl)

        self.assertEqual(io._receive(), bytes())
        self.assertRaisesRegexp(
            AMQPConnectionError,
            'connection/socket error',
            connection.check_for_errors
        )

    def test_io_socket_read_fails(self):
        connection = FakeConnection()
        parameters = FakeConnection().parameters
        parameters['ssl'] = False
        io = IO(parameters, exceptions=connection.exceptions)

        self.assertFalse(io.use_ssl)

        self.assertRaisesRegexp(
            socket.error,
            'connection/socket error',
            io._read_from_socket
        )

    def test_io_socket_read_fails_with_ssl(self):
        connection = FakeConnection()
        parameters = FakeConnection().parameters
        parameters['ssl'] = True
        io = IO(parameters, exceptions=connection.exceptions)

        self.assertTrue(io.use_ssl)

        self.assertRaisesRegexp(
            socket.error,
            'connection/socket error',
            io._read_from_socket
        )
