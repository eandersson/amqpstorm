import socket
import threading

from mock import Mock
from pamqp import frame as pamqp_frame
from pamqp import specification
from pamqp.specification import Basic as spec_basic

from amqpstorm import Channel
from amqpstorm import Connection
from amqpstorm import compatibility
from amqpstorm.base import MAX_CHANNELS
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.io import IO
from amqpstorm.tests.utility import TestFramework


class ConnectionTests(TestFramework):
    def test_connection_with_statement(self):
        with Connection('127.0.0.1', 'guest', 'guest', lazy=True) as con:
            self.assertIsInstance(con, Connection)

    def test_connection_with_statement_when_failing(self):
        try:
            with Connection('127.0.0.1', 'guest', 'guest', lazy=True) as con:
                con.exceptions.append(AMQPConnectionError('travis-ci'))
                con.check_for_errors()
        except AMQPConnectionError as why:
            self.assertIsInstance(why, AMQPConnectionError)

        self.assertEqual(self.get_last_log(),
                         'Closing connection due to an unhandled exception: '
                         'travis-ci')

    def test_connection_server_is_blocked_default_value(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertEqual(connection.is_blocked, False)

    def test_connection_server_properties_default_value(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertEqual(connection.server_properties, {})

    def test_connection_socket_property(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection._io.socket = 'FakeSocket'
        self.assertEqual(connection.socket, 'FakeSocket')

    def test_connection_socket_none_when_closed(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertFalse(connection.socket)

    def test_connection_fileno_property(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPENING)
        io = IO(connection.parameters, [])
        io.socket = Mock(name='socket', spec=socket.socket)
        connection._io = io
        io.socket.fileno.return_value = 5

        self.assertEqual(connection.fileno, 5)

    def test_connection_fileno_none_when_closed(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertIsNone(connection.fileno)

    def test_connection_close_state(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(Connection.OPEN)
        connection.close()

        self.assertTrue(connection.is_closed)

    def test_connection_open_channel_on_closed_connection(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertRaises(AMQPConnectionError, connection.channel)

    def test_connection_basic_read_buffer(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        cancel_ok_frame = spec_basic.CancelOk().marshal()

        self.assertEqual(connection._read_buffer(cancel_ok_frame), b'\x00')

    def test_connection_send_handshake(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        def on_write_to_socket(message):
            self.assertEqual(message, b'AMQP\x00\x00\t\x01')

        connection._io.write_to_socket = on_write_to_socket

        self.assertIsNone(connection._send_handshake())

    def test_connection_handle_read_buffer_none_returns_none(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        self.assertIsNone(connection._read_buffer(None))

    def test_connection_basic_handle_amqp_frame(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        payload = (
            b'\x01\x00\x00\x00\x00\x00\x0c\x00\n\x00\x1e\x00\x00\x00'
            b'\x02\x00\x00\x00<\xce'
        )

        data_in, channel_id, frame_in = connection._handle_amqp_frame(payload)

        self.assertEqual(data_in, b'')
        self.assertEqual(channel_id, 0)
        self.assertIsInstance(frame_in, specification.Connection.Tune)

    def test_connection_handle_amqp_frame_none_returns_none(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        result = connection._handle_amqp_frame('')

        self.assertEqual(result[0], '')
        self.assertIsNone(result[1])
        self.assertIsNone(result[2])

    def test_connection_handle_amqp_frame_error(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        def throw_error(*_):
            raise specification.AMQPFrameError()

        restore_func = pamqp_frame.unmarshal
        try:
            pamqp_frame.unmarshal = throw_error

            result = connection._handle_amqp_frame('travis-ci')

            self.assertEqual(result[0], 'travis-ci')
            self.assertIsNone(result[1])
            self.assertIsNone(result[2])
        finally:
            pamqp_frame.unmarshal = restore_func

        self.assertEqual(self.get_last_log(),
                         'AMQPFrameError: AMQPFrameError()')

    def test_connection_handle_unicode_error(self):
        """This test covers an unlikely issue triggered by network corruption.

            pamqp.decode._maybe_utf8 raises:
                UnicodeDecodeError: 'utf8' codec can't
                decode byte 0xc5 in position 1: invalid continuation byte

            The goal here is not to fix issues caused by network corruption,
            but rather to make sure that the exceptions raised when
            connections do fail are always predictable.

            Fail fast and reliably!

        :return:
        """
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        def throw_error(_):
            raise UnicodeDecodeError(str(), bytes(), 1, 1, str())

        restore_func = pamqp_frame.unmarshal
        try:
            pamqp_frame.unmarshal = throw_error

            result = connection._handle_amqp_frame('travis-ci')

            self.assertEqual(result[0], 'travis-ci')
            self.assertIsNone(result[1])
            self.assertIsNone(result[2])
        finally:
            pamqp_frame.unmarshal = restore_func

        self.assertEqual(self.get_last_log(),
                         "'' codec can't decode bytes in position 1-0: ")

    def test_connection_handle_value_error(self):
        """This test covers an unlikely issue triggered by network corruption.

            pamqp.decode._embedded_value raises:
                ValueError: Unknown type: b'\x13'

            The goal here is not to fix issues caused by network corruption,
            but rather to make sure that the exceptions raised when
            connections do fail are always predictable.

            Fail fast and reliably!

        :return:
        """
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        def throw_error(_):
            raise ValueError("Unknown type: b'\x13'")

        restore_func = pamqp_frame.unmarshal
        try:
            pamqp_frame.unmarshal = throw_error

            result = connection._handle_amqp_frame('travis-ci')

            self.assertEqual(result[0], 'travis-ci')
            self.assertIsNone(result[1])
            self.assertIsNone(result[2])
        finally:
            pamqp_frame.unmarshal = restore_func

        self.assertEqual(self.get_last_log(),
                         "Unknown type: b'\x13'")

    def test_connection_wait_for_connection(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPENING)
        io = IO(connection.parameters, [])
        io.socket = Mock(name='socket', spec=socket.socket)
        connection._io = io

        self.assertFalse(connection.is_open)

        def set_state_to_open(conn):
            conn.set_state(conn.OPEN)

        threading.Timer(function=set_state_to_open,
                        interval=0.1, args=(connection,)).start()
        connection._wait_for_connection_state(connection.OPEN)

        self.assertTrue(connection.is_open)

    def test_connection_wait_for_connection_does_raise_on_error(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPENING)

        connection.exceptions.append(AMQPConnectionError('travis-ci'))

        self.assertRaises(
            AMQPConnectionError, connection._wait_for_connection_state,
            connection.OPEN, 0.1
        )

    def test_connection_wait_for_connection_raises_on_timeout(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', timeout=0.1,
                                lazy=True)
        connection.set_state(connection.OPENING)
        io = IO(connection.parameters, [])
        io.socket = Mock(name='socket', spec=socket.socket)
        connection._io = io

        self.assertRaises(
            AMQPConnectionError,
            connection._wait_for_connection_state,
            connection.OPEN, 0.1
        )

    def test_connection_open(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        io = IO(connection.parameters, [])
        io.socket = Mock(name='socket', spec=socket.socket)
        connection._io = io

        def open():
            pass

        def on_write_to_socket(_):
            connection.set_state(connection.OPEN)

        connection._io.open = open
        connection._io.write_to_socket = on_write_to_socket

        self.assertTrue(connection.is_closed)

        connection.open()

        self.assertTrue(connection.is_open)

    def test_connection_close(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)
        io = IO(connection.parameters, [])
        io.socket = Mock(name='socket', spec=socket.socket)
        connection._io = io

        # Create some fake channels.
        for index in range(10):
            connection._channels[index + 1] = Channel(
                index + 1, connection, 360)

        def on_write(frame_out):
            self.assertIsInstance(frame_out, specification.Connection.Close)
            connection._channel0._close_connection_ok()

        connection._channel0._write_frame = on_write

        self.assertFalse(connection.is_closed)

        connection.close()

        # Make sure all the fake channels were closed as well.
        for index in range(10):
            self.assertNotIn(index + 1, connection._channels)

        self.assertTrue(connection.is_closed)

    def test_connection_close_when_already_closed(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)
        io = IO(connection.parameters, [])
        io.socket = Mock(name='socket', spec=socket.socket)
        connection._io = io

        connection.set_state(connection.CLOSED)

        # Create some fake channels.
        for index in range(10):
            connection._channels[index + 1] = Channel(
                index + 1, connection, 360)

        def state_set(state):
            self.assertEqual(state, connection.CLOSED)

        connection.set_state = state_set

        self.assertTrue(connection.is_closed)

        connection.close()

        # Make sure all the fake channels were closed as well.
        for index in range(10):
            self.assertNotIn(index + 1, connection._channels)

        self.assertFalse(connection._channels)
        self.assertTrue(connection.is_closed)

    def test_connection_close_handles_raise_on_write(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)
        io = IO(connection.parameters, [])
        io.socket = Mock(name='socket', spec=socket.socket)
        connection._io = io

        # Create some fake channels.
        for index in range(10):
            connection._channels[index + 1] = Channel(
                index + 1, connection, 360)

        def raise_on_write(_):
            raise AMQPConnectionError('travis-ci')

        connection._channel0._write_frame = raise_on_write

        self.assertFalse(connection.is_closed)

        connection.close()

        # Make sure all the fake channels were closed as well.
        for index in range(10):
            self.assertNotIn(index + 1, connection._channels)

        self.assertFalse(connection._channels)
        self.assertTrue(connection.is_closed)

    def test_connection_close_channels(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        channel_1 = Channel(1, connection, 360)
        channel_2 = Channel(2, connection, 360)
        channel_3 = Channel(3, connection, 360)

        channel_1.set_state(Channel.CLOSED)
        channel_2.set_state(Channel.CLOSED)
        channel_2.set_state(Channel.OPEN)

        connection._channels[1] = channel_1
        connection._channels[2] = channel_2
        connection._channels[3] = channel_3

        connection._close_remaining_channels()

        self.assertTrue(channel_1.is_closed)
        self.assertTrue(channel_2.is_closed)
        self.assertTrue(channel_3.is_closed)

        self.assertFalse(connection._channels)

    def test_connection_closed_on_exception(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)
        connection.exceptions.append(AMQPConnectionError('travis-ci'))

        self.assertTrue(connection.is_open)
        self.assertRaises(AMQPConnectionError, connection.check_for_errors)
        self.assertTrue(connection.is_closed)

    def test_connection_heartbeat_stopped_on_close(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)
        connection.heartbeat.start(connection.exceptions)
        connection.exceptions.append(AMQPConnectionError('travis-ci'))

        self.assertTrue(connection.heartbeat._running.is_set())

        self.assertRaises(AMQPConnectionError, connection.check_for_errors)

        self.assertFalse(connection.heartbeat._running.is_set())

    def test_connection_open_new_channel(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)

        def on_open_ok(_, frame_out):
            self.assertIsInstance(frame_out, specification.Channel.Open)
            connection._channels[1].on_frame(specification.Channel.OpenOk())

        connection.write_frame = on_open_ok

        connection.channel()

    def test_connection_get_first_channel_id(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection._last_channel_id, None)
        self.assertEqual(
            connection._get_next_available_channel_id(), 1
        )
        self.assertEqual(connection._last_channel_id, 1)

    def test_connection_get_channel_ids(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection._last_channel_id, None)
        connection._channels[1] = None
        for index in range(2, 100):
            channel_id = connection._get_next_available_channel_id()
            connection._channels[channel_id] = None
            self.assertEqual(
                channel_id, index
            )
            self.assertEqual(connection._last_channel_id, index)

    def test_connection_avoid_conflicts_with_channel_ids(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)

        for index in compatibility.RANGE(1, 301):
            connection._channels[index] = None
        for index in compatibility.RANGE(302, 65535):
            connection._channels[index] = None

        last_channel_id = int(connection.channel(lazy=True))

        self.assertEqual(
            last_channel_id, 301
        )
        self.assertEqual(connection._last_channel_id, 301)

        last_channel_id = int(connection.channel(lazy=True))

        self.assertEqual(
            last_channel_id, 65535
        )
        self.assertEqual(connection._last_channel_id, 65535)

    def test_connection_close_old_ids_with_channel(self):
        max_channels = 1024
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection._channel0.max_allowed_channels = max_channels
        connection.set_state(connection.OPEN)

        for _ in compatibility.RANGE(max_channels):
            connection.channel(lazy=True)

        ids_to_close = [2, 8, 16, 32, 64, 128, 256, 512, 768, 1024]

        for _ in range(100):
            for index in ids_to_close:
                del connection._channels[index]

            self.assertEqual(len(connection._channels),
                             max_channels - len(ids_to_close))

            for _ in ids_to_close:
                self.assertIn(
                    int(connection.channel(lazy=True)), ids_to_close
                )

            self.assertEqual(len(connection._channels), max_channels)

    def test_connection_open_many_channels(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)

        for index in compatibility.RANGE(MAX_CHANNELS - 1):
            self.assertEqual(int(connection.channel(lazy=True)), index + 1)

    def test_connection_maximum_channels_reached(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(connection.OPEN)

        for index in compatibility.RANGE(MAX_CHANNELS):
            channel_id = connection._get_next_available_channel_id()
            connection._channels[channel_id] = None
            self.assertEqual(connection._last_channel_id, index + 1)

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'reached the maximum number of channels %d' % MAX_CHANNELS,
            connection.channel, lazy=True)

    def test_connection_cleanup_one_channel(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection._channels[1] = Channel(1, connection, 0.1)

        connection._cleanup_channel(1)

        self.assertFalse(connection._channels)

    def test_connection_cleanup_multiple_channels(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)

        for index in range(1, 10):
            connection._channels[index] = Channel(index, connection, 0.1)

        for index in range(1, 10):
            connection._cleanup_channel(index)

        self.assertFalse(connection._channels)

    def test_connection_cleanup_channel_does_not_exist(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection._channels[1] = Channel(1, connection, 0.1)

        connection._cleanup_channel(2)

        self.assertEqual(len(connection._channels), 1)
