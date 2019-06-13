from mock import Mock
from pamqp import specification

from amqpstorm import Channel
from amqpstorm.basic import Basic
from amqpstorm.exception import AMQPChannelError
from amqpstorm.exchange import Exchange
from amqpstorm.queue import Queue
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework
from amqpstorm.tx import Tx


class ChannelTests(TestFramework):
    def test_channel_with_statement_when_closed(self):
        with Channel(0, Mock(name='Connection'), 360) as channel:
            self.assertIsInstance(channel, Channel)

    def test_channel_with_statement_when_open(self):
        connection = FakeConnection(FakeConnection.CLOSED)
        with Channel(0, connection, 360) as channel:
            channel.set_state(channel.OPEN)
            self.assertIsInstance(channel, Channel)

    def test_channel_with_statement_when_failing(self):
        connection = FakeConnection()
        try:
            with Channel(0, connection, 360) as channel:
                channel.exceptions.append(AMQPChannelError('error'))
                channel.check_for_errors()
        except AMQPChannelError as why:
            self.assertIsInstance(why, AMQPChannelError)

        self.assertEqual(self.get_last_log(),
                         'Closing channel due to an unhandled exception: '
                         'error')

    def test_channel_id(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        self.assertEqual(int(channel), 0)

        channel = Channel(1557, Mock(name='Connection'), 360)

        self.assertEqual(int(channel), 1557)

    def test_channel_open(self):
        def on_open_ok(_, frame_out):
            self.assertIsInstance(frame_out, specification.Channel.Open)
            channel.rpc.on_frame(specification.Channel.OpenOk())

        channel = Channel(0, FakeConnection(on_write=on_open_ok), 360)

        # Close Channel.
        channel.open()

        self.assertEqual(channel._state, channel.OPEN)

    def test_channel_close(self):
        def on_close_ok(_, frame_out):
            if isinstance(frame_out, specification.Basic.Cancel):
                channel.rpc.on_frame(specification.Basic.CancelOk())
                return
            channel.rpc.on_frame(specification.Channel.CloseOk())

        channel = Channel(0, FakeConnection(on_write=on_close_ok), 360)

        # Set up Fake Channel.
        channel._inbound = [1, 2, 3]
        channel.set_state(channel.OPEN)
        channel._consumer_tags = ['4', '5', '6']

        # Close Channel.
        channel.close()

        self.assertEqual(channel._inbound, [])
        self.assertEqual(channel._consumer_tags, [])
        self.assertEqual(channel._state, channel.CLOSED)
        self.assertFalse(channel.exceptions)

    def test_channel_close_gracefully_with_queued_error(self):
        def on_close_ok(_, frame_out):
            if isinstance(frame_out, specification.Basic.Cancel):
                raise AMQPChannelError('travis-ci')
            channel.rpc.on_frame(specification.Channel.CloseOk())

        channel = Channel(0, FakeConnection(on_write=on_close_ok), 360)

        # Set up Fake Channel.
        channel._inbound = [1, 2, 3]
        channel.set_state(channel.OPEN)
        channel._consumer_tags = ['4', '5', '6']
        channel.exceptions.append(AMQPChannelError('travis-ci'))

        # Close Channel.
        channel.close()

        self.assertEqual(channel._inbound, [])
        self.assertEqual(channel._consumer_tags, [])
        self.assertEqual(channel._state, channel.CLOSED)
        self.assertTrue(channel.exceptions)

    def test_channel_close_when_already_closed(self):
        fake_connection = FakeConnection()
        channel = Channel(0, fake_connection, 360)

        # Set up Fake Channel.
        channel._inbound = [1, 2, 3]
        channel.set_state(channel.CLOSED)
        channel._consumer_tags = ['4', '5', '6']

        def state_set(state):
            self.assertEqual(state, channel.CLOSED)

        channel.set_state = state_set

        # Close Channel.
        channel.close()

        self.assertFalse(fake_connection.frames_out)

        self.assertEqual(channel._inbound, [])
        self.assertEqual(channel._consumer_tags, [])
        self.assertEqual(channel._state, channel.CLOSED)
        self.assertFalse(channel.exceptions)

    def test_channel_confirm_deliveries(self):
        def on_select_ok(*_):
            channel.rpc.on_frame(specification.Confirm.SelectOk())

        connection = FakeConnection(on_write=on_select_ok)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)

        self.assertFalse(channel.confirming_deliveries)
        self.assertEqual(channel.confirm_deliveries(), {})
        self.assertTrue(channel.confirming_deliveries)

    def test_channel_close_channel(self):
        channel = Channel(0, FakeConnection(), 360)

        # Set up Fake Channel.
        channel._inbound = [1, 2, 3]
        channel.set_state(channel.OPEN)
        channel._consumer_tags = [4, 5, 6]

        close_frame = specification.Channel.Close(reply_code=200,
                                                  reply_text='travis-ci')
        # Close Channel.
        channel._close_channel(close_frame)

        self.assertEqual(channel._inbound, [])
        self.assertEqual(channel._consumer_tags, [])
        self.assertEqual(channel._state, channel.CLOSED)

    def test_channel_basic_handler_is_defined(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        self.assertIsInstance(channel.basic, Basic)

    def test_channel_exchange_handler_is_defined(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        self.assertIsInstance(channel.exchange, Exchange)

    def test_channel_queue_handler_is_defined(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        self.assertIsInstance(channel.queue, Queue)

    def test_channel_tx_handler_is_defined(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        self.assertIsInstance(channel.tx, Tx)
