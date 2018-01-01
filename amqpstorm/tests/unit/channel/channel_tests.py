from pamqp import ContentHeader
from pamqp import specification
from pamqp.body import ContentBody

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
        with Channel(0, None, 360) as channel:
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
        channel = Channel(0, None, 360)

        self.assertEqual(int(channel), 0)

        channel = Channel(1557, None, 360)

        self.assertEqual(int(channel), 1557)

    def test_channel_build_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        for msg in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)

    def test_channel_build_inbound_messages_without_break_on_empty(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        for _ in range(25):
            channel._inbound.append(deliver)
            channel._inbound.append(header)
            channel._inbound.append(body)

        messages_consumed = 0
        for msg in channel.build_inbound_messages(break_on_empty=False):
            messages_consumed += 1
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)
            if messages_consumed >= 10:
                channel.set_state(channel.CLOSED)
        self.assertEqual(messages_consumed, 10)

    def test_channel_build_inbound_messages_as_tuple(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        for msg in channel.build_inbound_messages(break_on_empty=True,
                                                  to_tuple=True):
            self.assertIsInstance(msg, tuple)
            self.assertEqual(msg[0], message)

    def test_channel_process_data_events(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver(consumer_tag='travis-ci')
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        def callback(msg):
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)

        channel._consumer_callbacks['travis-ci'] = callback
        channel.process_data_events()

    def test_channel_process_data_events_as_tuple(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver(consumer_tag='travis-ci')
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        def callback(body, channel, method, properties):
            self.assertIsInstance(body, bytes)
            self.assertIsInstance(channel, Channel)
            self.assertIsInstance(method, dict)
            self.assertIsInstance(properties, dict)
            self.assertEqual(body, message)

        channel._consumer_callbacks['travis-ci'] = callback
        channel.process_data_events(to_tuple=True)

    def test_channel_start_consuming(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver(consumer_tag='travis-ci')
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        def callback(msg):
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)
            channel.set_state(channel.CLOSED)

        channel._consumer_callbacks['travis-ci'] = callback
        channel.start_consuming()

    def test_channel_start_consuming_multiple_callbacks(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver_one = specification.Basic.Deliver(consumer_tag='travis-ci-1')
        deliver_two = specification.Basic.Deliver(consumer_tag='travis-ci-2')
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [
            deliver_one, header, body,
            deliver_two, header, body
        ]

        def callback_one(msg):
            self.assertEqual(msg.method.get('consumer_tag'), 'travis-ci-1')
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)

        def callback_two(msg):
            self.assertEqual(msg.method.get('consumer_tag'), 'travis-ci-2')
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)
            channel.set_state(channel.CLOSED)

        channel._consumer_callbacks['travis-ci-1'] = callback_one
        channel._consumer_callbacks['travis-ci-2'] = callback_two

        channel.start_consuming()

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
        channel._consumer_tags = ['1', '2', '3']

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
        channel._consumer_tags = ['1', '2', '3']
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
        channel._consumer_tags = ['1', '2', '3']

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
        channel._consumer_tags = [1, 2, 3]

        close_frame = specification.Channel.Close(reply_code=200,
                                                  reply_text='travis-ci')
        # Close Channel.
        channel._close_channel(close_frame)

        self.assertEqual(channel._inbound, [])
        self.assertEqual(channel._consumer_tags, [])
        self.assertEqual(channel._state, channel.CLOSED)

    def test_channel_basic_handler_is_defined(self):
        channel = Channel(0, None, 360)

        self.assertIsInstance(channel.basic, Basic)

    def test_channel_exchange_handler_is_defined(self):
        channel = Channel(0, None, 360)

        self.assertIsInstance(channel.exchange, Exchange)

    def test_channel_queue_handler_is_defined(self):
        channel = Channel(0, None, 360)

        self.assertIsInstance(channel.queue, Queue)

    def test_channel_tx_handler_is_defined(self):
        channel = Channel(0, None, 360)

        self.assertIsInstance(channel.tx, Tx)
