from mock import Mock
from pamqp import ContentHeader
from pamqp import specification
from pamqp.body import ContentBody

import amqpstorm
from amqpstorm import Channel
from amqpstorm.exception import AMQPChannelError
from amqpstorm.exception import AMQPConnectionError
from amqpstorm.exception import AMQPMessageError
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import FakeFrame
from amqpstorm.tests.utility import TestFramework


class ChannelFrameTests(TestFramework):
    def test_channel_content_frames(self):
        channel = Channel(0, FakeConnection(), rpc_timeout=1)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel.on_frame(deliver)
        channel.on_frame(header)
        channel.on_frame(body)

        for msg in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)

    def test_channel_basic_cancel_frame(self):
        connection = amqpstorm.Connection('localhost', 'guest', 'guest',
                                          lazy=True)
        channel = Channel(0, connection, rpc_timeout=1)

        channel.on_frame(specification.Basic.Cancel('travis-ci'))

        self.assertEqual(
            self.get_last_log(),
            'Received Basic.Cancel on consumer_tag: travis-ci'
        )

    def test_channel_cancel_ok_frame(self):
        tag = 'travis-ci'
        channel = Channel(0, Mock(name='Connection'), rpc_timeout=1)
        channel.add_consumer_tag(tag)

        channel.on_frame(specification.Basic.CancelOk(tag))

        self.assertFalse(channel.consumer_tags)

    def test_channel_consume_ok_frame(self):
        tag = 'travis-ci'
        channel = Channel(0, Mock(name='Connection'), rpc_timeout=1)

        channel.on_frame(specification.Basic.ConsumeOk(tag))

        self.assertEqual(channel.consumer_tags[0], tag)

    def test_channel_basic_return_frame(self):
        connection = amqpstorm.Connection('localhost', 'guest', 'guest',
                                          lazy=True)
        connection.set_state(connection.OPEN)
        channel = Channel(0, connection, rpc_timeout=1)
        channel.set_state(channel.OPEN)

        channel.on_frame(
            specification.Basic.Return(
                reply_code=500,
                reply_text='travis-ci',
                exchange='exchange',
                routing_key='routing_key'
            )
        )

        self.assertRaisesRegexp(
            AMQPMessageError,
            "Message not delivered: travis-ci \(500\) to queue "
            "'routing_key' from exchange 'exchange'",
            channel.check_for_errors
        )

    def test_channel_close_frame(self):
        connection = FakeConnection(state=FakeConnection.OPEN)
        channel = Channel(0, connection, rpc_timeout=1)
        channel.set_state(channel.OPEN)

        channel.on_frame(
            specification.Channel.Close(
                reply_code=500,
                reply_text='travis-ci'
            )
        )

        self.assertRaisesRegexp(
            AMQPChannelError,
            'Channel 0 was closed by remote server: travis-ci',
            channel.check_for_errors
        )

    def test_channel_close_frame_when_connection_closed(self):
        connection = FakeConnection(state=FakeConnection.CLOSED)
        channel = Channel(0, connection, rpc_timeout=1)
        channel.set_state(channel.OPEN)

        channel.on_frame(
            specification.Channel.Close(
                reply_code=500,
                reply_text='travis-ci'
            )
        )

        self.assertIsNone(connection.get_last_frame())
        self.assertEqual(
            str(channel.exceptions[0]),
            'Channel 0 was closed by remote server: travis-ci'
        )

    def test_channel_close_frame_socket_write_fail_silently(self):
        connection = FakeConnection(state=FakeConnection.OPEN)
        channel = Channel(0, connection, rpc_timeout=1)
        channel.set_state(channel.OPEN)

        def raise_on_write(*_):
            raise AMQPConnectionError('travis-ci')

        connection.write_frame = raise_on_write

        channel.on_frame(
            specification.Channel.Close(
                reply_code=500,
                reply_text='travis-ci'
            )
        )

        self.assertIsNone(connection.get_last_frame())
        self.assertEqual(
            str(channel.exceptions[0]),
            'Channel 0 was closed by remote server: travis-ci'
        )

    def test_channel_flow_frame(self):
        connection = FakeConnection()
        connection.set_state(connection.OPEN)
        channel = Channel(0, connection, rpc_timeout=1)
        channel.set_state(channel.OPEN)

        channel.on_frame(specification.Channel.Flow())

        self.assertIsInstance(
            connection.get_last_frame(),
            specification.Channel.FlowOk
        )

    def test_channel_unhandled_frame(self):
        connection = amqpstorm.Connection('localhost', 'guest', 'guest',
                                          lazy=True)
        channel = Channel(0, connection, rpc_timeout=1)

        channel.on_frame(FakeFrame())

        self.assertEqual(
            self.get_last_log(),
            "[Channel0] Unhandled Frame: FakeFrame -- "
            "{'data_1': 'hello world'}"
        )
