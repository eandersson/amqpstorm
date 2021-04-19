import mock
from mock import Mock
from pamqp import commands

import amqpstorm
from amqpstorm import AMQPChannelError
from amqpstorm import AMQPConnectionError
from amqpstorm import AMQPInvalidArgument
from amqpstorm import AMQPMessageError
from amqpstorm import Channel
from amqpstorm import exception
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class ChannelExceptionTests(TestFramework):
    def test_chanel_invalid_close_parameter(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'reply_code should be an integer',
            channel.close, 'travis-ci', 'travis-ci'
        )
        self.assertRaisesRegexp(
            AMQPInvalidArgument,
            'reply_text should be a string',
            channel.close, 200, 200
        )

    def test_chanel_callback_not_set(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        self.assertRaisesRegexp(
            AMQPChannelError,
            'no consumer callback defined',
            channel.process_data_events
        )

    def test_channel_throw_exception_check_for_error(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)
        channel.exceptions.append(AMQPConnectionError('travis-ci'))

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'travis-ci',
            channel.check_for_errors
        )

    def test_channel_check_error_no_exception(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        self.assertIsNone(channel.check_for_errors())

    def test_channel_check_error_when_closed(self):
        channel = Channel(0, FakeConnection(), 360)

        self.assertRaisesRegexp(
            exception.AMQPChannelError,
            'channel was closed',
            channel.check_for_errors
        )

    def test_channel_check_error_connection_closed(self):
        channel = Channel(0, FakeConnection(FakeConnection.CLOSED), 360)

        self.assertRaisesRegexp(
            exception.AMQPConnectionError,
            'connection was closed',
            channel.check_for_errors
        )

    def test_channel_raises_when_closed(self):
        channel = Channel(0, FakeConnection(FakeConnection.OPEN), 360)
        channel.set_state(channel.CLOSED)

        self.assertFalse(channel.is_open)
        self.assertRaisesRegexp(
            exception.AMQPChannelError,
            'channel was closed',
            channel.check_for_errors
        )
        self.assertTrue(channel.is_closed)

    def test_channel_closed_after_connection_closed(self):
        channel = Channel(0, FakeConnection(FakeConnection.CLOSED), 360)
        channel.set_state(channel.OPEN)

        self.assertTrue(channel.is_open)
        self.assertRaisesRegexp(
            exception.AMQPConnectionError,
            'connection was closed',
            channel.check_for_errors
        )
        self.assertTrue(channel.is_closed)

    def test_channel_closed_after_connection_exception(self):
        connection = amqpstorm.Connection('localhost', 'guest', 'guest',
                                          lazy=True)
        channel = Channel(0, connection, 360)
        connection.exceptions.append(AMQPConnectionError('travis-ci'))
        channel.set_state(channel.OPEN)

        self.assertTrue(connection.is_closed)
        self.assertTrue(channel.is_open)
        self.assertRaisesRegexp(
            exception.AMQPConnectionError,
            'travis-ci',
            channel.check_for_errors
        )
        self.assertTrue(channel.is_closed)

    def test_channel_consume_exception_when_recoverable(self):
        connection = amqpstorm.Connection('localhost', 'guest', 'guest',
                                          lazy=True)
        connection.set_state(connection.OPEN)
        channel = Channel(0, connection, 360)
        channel.set_state(channel.OPEN)
        channel.exceptions.append(AMQPChannelError('no-route'))

        self.assertTrue(connection.is_open)
        self.assertTrue(channel.is_open)

        self.assertRaisesRegexp(
            exception.AMQPChannelError,
            'no-route',
            channel.check_for_errors
        )

        self.assertTrue(channel.is_open)

        channel.check_for_errors()

    @mock.patch('amqpstorm.Channel._build_message',
                side_effect=AMQPChannelError())
    def test_channel_build_inbound_raises(self, _):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)

        generator = channel.build_inbound_messages(break_on_empty=False)
        if hasattr(generator, 'next'):
            self.assertRaises(AMQPChannelError, generator.next)
        else:
            self.assertRaises(AMQPChannelError, generator.__next__)

    def test_channel_build_inbound_raises_in_loop(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        self.first = True

        def raise_after_one(**_):
            if not self.first:
                channel.exceptions.append(AMQPChannelError())
            self.first = False
            return None

        with mock.patch('amqpstorm.Channel._build_message',
                        side_effect=raise_after_one):
            generator = channel.build_inbound_messages(break_on_empty=False)
            if hasattr(generator, 'next'):
                self.assertRaises(AMQPChannelError, generator.next)
            else:
                self.assertRaises(AMQPChannelError, generator.__next__)

    def test_channel_raises_with_return_reply_code_500(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        basic_return = commands.Basic.Return(
            reply_code=500,
            reply_text='Error'
        )
        channel._basic_return(basic_return)

        self.assertRaisesRegexp(
            AMQPMessageError,
            "Message not delivered: Error \(500\) to queue "
            "'' from exchange ''",
            channel.check_for_errors
        )

    def test_channel_raise_with_close_reply_code_500(self):
        connection = FakeConnection()
        channel = Channel(0, connection, 360)

        # Set up Fake Channel.
        channel._inbound = [1, 2, 3]
        channel.set_state(channel.OPEN)
        channel._consumer_tags = [4, 5, 6]

        close_frame = commands.Channel.Close(
            reply_code=500,
            reply_text='travis-ci'
        )
        channel._close_channel(close_frame)

        self.assertEqual(channel._inbound, [])
        self.assertEqual(channel._consumer_tags, [])
        self.assertEqual(channel._state, channel.CLOSED)

        self.assertRaisesRegexp(
            AMQPChannelError,
            'Channel 0 was closed by remote server: travis-ci',
            channel.check_for_errors
        )
