import select
import socket
import ssl

from unittest import mock

import amqpstorm.io
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.io import IO
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class IOTests(TestFramework):

    def test_io_socket_close(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        io.socket = mock.Mock(name='socket', spec=socket.socket)
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

        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.recv.return_value = '12345'

        self.assertEqual(io._receive(), '12345')

    def test_io_simple_ssl_receive(self):
        connection = FakeConnection()
        connection.parameters['ssl'] = True
        io = IO(connection.parameters)

        self.assertTrue(io.use_ssl)

        if hasattr(ssl, 'SSLObject'):
            io.socket = mock.Mock(name='socket', spec=ssl.SSLObject)
        elif hasattr(ssl, 'SSLSocket'):
            io.socket = mock.Mock(name='socket', spec=ssl.SSLSocket)

        io.socket.read.return_value = '12345'

        self.assertEqual(io._receive(), '12345')

    def test_io_simple_send_zero_bytes_sent(self):
        connection = FakeConnection()

        io = IO(connection.parameters, exceptions=connection.exceptions)
        io.socket = mock.Mock(name='socket', spec=socket.socket)
        io.socket.send.return_value = 0
        io.write_to_socket(self.message.encode('utf-8'))

        self.assertRaisesRegex(
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

    def test_io_set_ssl_verify_req_enum(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'verify_mode': ssl.CERT_REQUIRED
        }

        io = IO(connection.parameters)
        sock = io._ssl_wrap_socket(socket.socket())
        self.assertEqual(sock.context.verify_mode, ssl.CERT_REQUIRED)

    def test_io_set_ssl_verify_optional(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'verify_mode': 'optional'
        }

        io = IO(connection.parameters)
        sock = io._ssl_wrap_socket(socket.socket())
        self.assertEqual(sock.context.verify_mode, ssl.CERT_OPTIONAL)

    def test_io_set_ssl_verify_optional_enum(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'verify_mode': ssl.CERT_OPTIONAL
        }

        io = IO(connection.parameters)
        sock = io._ssl_wrap_socket(socket.socket())
        self.assertEqual(sock.context.verify_mode, ssl.CERT_OPTIONAL)

    def test_io_ssl_defaults_are_secure(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {}

        io = IO(connection.parameters)
        sock = io._ssl_wrap_socket(socket.socket())
        self.assertEqual(sock.context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(sock.context.check_hostname)

    def test_io_set_ssl_verify_none_disables_hostname_check(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'verify_mode': 'none'
        }

        io = IO(connection.parameters)
        sock = io._ssl_wrap_socket(socket.socket())
        self.assertEqual(sock.context.verify_mode, ssl.CERT_NONE)
        self.assertFalse(sock.context.check_hostname)

    def test_io_set_ssl_check_hostname_explicit_false(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'check_hostname': False,
        }

        io = IO(connection.parameters)
        sock = io._ssl_wrap_socket(socket.socket())
        self.assertFalse(sock.context.check_hostname)

    def test_io_set_ssl_check_hostname_string_true(self):
        connection = FakeConnection()
        connection.parameters['ssl_options'] = {
            'check_hostname': 'true',
        }

        io = IO(connection.parameters)
        sock = io._ssl_wrap_socket(socket.socket())
        self.assertTrue(sock.context.check_hostname)

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

    def test_io_backpressure_callback_defaults_to_none(self):
        connection = FakeConnection()
        io = IO(connection.parameters)
        self.assertIsNone(io._check_backpressure)

    def test_io_backpressure_sleep_constant_is_short(self):
        self.assertLess(amqpstorm.io.INBOUND_BACKPRESSURE_SLEEP, 0.1)
        self.assertGreater(amqpstorm.io.INBOUND_BACKPRESSURE_SLEEP, 0)

    def test_io_backpressure_callback_handed_to_select_poller(self):
        from amqpstorm.io import SelectPoller

        connection = FakeConnection()
        check = lambda: False  # noqa: E731
        io = IO(connection.parameters,
                on_read_impl=lambda data: data,
                check_backpressure=check)
        connection.parameters['poller'] = 'select'
        io.poller_type = 'select'

        with mock.patch.object(io, '_get_socket_addresses',
                               return_value=[]), \
                mock.patch.object(io, '_find_address_and_connect',
                                  return_value=mock.Mock(spec=socket.socket)), \
                mock.patch.object(io, '_create_inbound_thread'):
            io.open()

        self.assertIsInstance(io.poller, SelectPoller)
        self.assertIs(io.poller._check_backpressure, check)
        io._running.clear()


class PollerBackpressureTests(TestFramework):
    """Cover the back-pressure path on both poller implementations."""

    def _make_select_poller(self, check):
        from amqpstorm.io import SelectPoller
        return SelectPoller(fileno=0, exceptions=[], check_backpressure=check)

    def test_select_poller_no_callback_polls_normally(self):
        poller = self._make_select_poller(None)
        with mock.patch('amqpstorm.io.select.select',
                        return_value=([0], [], [])) as mock_select, \
                mock.patch('amqpstorm.io.time.sleep') as mock_sleep:
            self.assertTrue(poller.is_ready)
        mock_select.assert_called_once()
        mock_sleep.assert_not_called()

    def test_select_poller_callback_false_polls_normally(self):
        poller = self._make_select_poller(lambda: False)
        with mock.patch('amqpstorm.io.select.select',
                        return_value=([0], [], [])) as mock_select, \
                mock.patch('amqpstorm.io.time.sleep') as mock_sleep:
            self.assertTrue(poller.is_ready)
        mock_select.assert_called_once()
        mock_sleep.assert_not_called()

    def test_select_poller_callback_true_pauses_and_skips(self):
        poller = self._make_select_poller(lambda: True)
        with mock.patch('amqpstorm.io.select.select') as mock_select, \
                mock.patch('amqpstorm.io.time.sleep') as mock_sleep:
            self.assertFalse(poller.is_ready)
        mock_select.assert_not_called()
        mock_sleep.assert_called_once_with(
            amqpstorm.io.INBOUND_BACKPRESSURE_SLEEP,
        )

    def _make_poll_poller(self, check):
        from amqpstorm.io import Poller
        if not hasattr(select, 'poll'):
            self.skipTest('select.poll not available')
        with mock.patch('amqpstorm.io.select.poll') as mock_poll:
            mock_poll.return_value = mock.Mock()
            return Poller(fileno=0, exceptions=[], check_backpressure=check)

    def test_poll_poller_no_callback_polls_normally(self):
        poller = self._make_poll_poller(None)
        poller.poller.poll.return_value = [(0, 1)]
        with mock.patch('amqpstorm.io.time.sleep') as mock_sleep:
            self.assertTrue(poller.is_ready)
        poller.poller.poll.assert_called_once()
        mock_sleep.assert_not_called()

    def test_poll_poller_callback_true_pauses_and_skips(self):
        poller = self._make_poll_poller(lambda: True)
        with mock.patch('amqpstorm.io.time.sleep') as mock_sleep:
            self.assertFalse(poller.is_ready)
        poller.poller.poll.assert_not_called()
        mock_sleep.assert_called_once_with(
            amqpstorm.io.INBOUND_BACKPRESSURE_SLEEP,
        )
