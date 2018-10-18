import threading

from mock import Mock
from pamqp import ContentHeader
from pamqp import specification
from pamqp.body import ContentBody

from amqpstorm import AMQPChannelError
from amqpstorm import Channel
from amqpstorm import Message
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class ChannelBuildMessageTests(TestFramework):
    def test_channel_build_message(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]
        result = channel._build_message(auto_decode=False)

        self.assertIsInstance(result.body, bytes)
        self.assertEqual(result.body, message)

    def test_channel_build_message_auto_decode(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]
        result = channel._build_message(auto_decode=True)

        self.assertIsInstance(result.body, str)
        self.assertEqual(result.body, message.decode('utf-8'))

    def test_channel_build_out_of_order_message_deliver(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        message = self.message
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)

        channel._inbound = [deliver, deliver, header]
        result = channel._build_message(auto_decode=True)

        self.assertEqual(result, None)
        self.assertIn("Received an out-of-order frame:",
                      self.get_last_log())

    def test_channel_build_out_of_order_message_header(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        message = self.message
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [header, deliver, header, body]
        result = channel._build_message(auto_decode=True)

        self.assertEqual(result, None)
        self.assertIn("Received an out-of-order frame:",
                      self.get_last_log())

    def test_channel_build_message_headers(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=10)

        channel._inbound = [deliver, header]
        result = channel._build_message_headers()

        self.assertIsInstance(result[0], specification.Basic.Deliver)
        self.assertIsInstance(result[1], ContentHeader)
        self.assertEqual(result[1].body_size, 10)

    def test_channel_build_message_headers_out_of_order(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=10)

        channel._inbound = [header, deliver]
        result = channel._build_message_headers()

        self.assertEqual(result, None)
        self.assertIn("Received an out-of-order frame:",
                      self.get_last_log())

        channel._inbound = [deliver, deliver]
        result = channel._build_message_headers()

        self.assertEqual(result, None)
        self.assertIn("Received an out-of-order frame:",
                      self.get_last_log())

    def test_channel_build_message_headers_empty(self):
        channel = Channel(0, Mock(name='Connection'), 360)
        channel._inbound = []
        self.assertRaises(IndexError, channel._build_message_headers)

    def test_channel_build_message_empty_and_then_break(self):
        """Start building a message with an empty inbound queue,
            and send an empty ContentBody that should be ignored.

        :return:
        """
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        channel._inbound = []

        def add_inbound():
            channel._inbound.append(ContentBody())

        threading.Timer(function=add_inbound, interval=0.1).start()

        self.assertFalse(channel._build_message_body(128))

    def test_channel_build_message_empty_and_raise(self):
        """Start building a message with an empty inbound queue,
            and raise when the channel is closed.

        :return:
        """
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        channel._inbound = []

        def close_channel():
            channel.set_state(Channel.CLOSED)

        threading.Timer(function=close_channel, interval=0.1).start()

        self.assertRaises(AMQPChannelError,
                          channel._build_message_body, 128)

    def test_channel_build_message_body(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        message = self.message.encode('utf-8')
        message_len = len(message)

        body = ContentBody(value=message)
        channel._inbound = [body]
        result = channel._build_message_body(message_len)

        self.assertEqual(message, result)

    def test_channel_build_message_body_break_on_none_value(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        message = self.message
        message_len = len(message)

        body = ContentBody(value=None)
        channel._inbound = [body]
        result = channel._build_message_body(message_len)

        self.assertEqual(result, b'')

    def test_channel_build_message_body_break_on_empty_value(self):
        channel = Channel(0, Mock(name='Connection'), 360)

        message = self.message
        message_len = len(message)

        body = ContentBody(value=b'')
        channel._inbound = [body]
        result = channel._build_message_body(message_len)

        self.assertEqual(result, b'')

    def test_channel_build_empty_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        generator = channel.build_inbound_messages(break_on_empty=True)

        if hasattr(generator, 'next'):
            self.assertRaises(StopIteration, generator.next)
        else:
            self.assertRaises(StopIteration, generator.__next__)

    def test_channel_build_no_message_but_inbound_not_empty(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        def add_content():
            channel._inbound.append(ContentHeader(body_size=message_len))
            channel._inbound.append(ContentBody(value=message))

        deliver = specification.Basic.Deliver()
        channel._inbound = [deliver]

        self.assertTrue(channel._inbound)

        threading.Timer(function=add_content, interval=0.2).start()

        for msg in channel.build_inbound_messages(break_on_empty=True):
            self.assertEqual(msg.body, message.decode('utf-8'))

        self.assertFalse(channel._inbound)

    def test_channel_build_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        messages_consumed = 0
        for msg in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(msg, Message)
            messages_consumed += 1

        self.assertEqual(messages_consumed, 1)

    def test_channel_build_multiple_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body, deliver, header, body,
                            deliver, header, body, deliver, header, body]

        messages_consumed = 0
        for msg in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(msg, Message)
            messages_consumed += 1

        self.assertEqual(messages_consumed, 4)

    def test_channel_build_large_number_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        for _ in range(10000):
            channel._inbound.append(deliver)
            channel._inbound.append(header)
            channel._inbound.append(body)

        messages_consumed = 0
        for msg in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(msg, Message)
            messages_consumed += 1

        self.assertEqual(messages_consumed, 10000)

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

        messages_consumed = 0
        for msg in channel.build_inbound_messages(break_on_empty=True,
                                                  to_tuple=True):
            self.assertIsInstance(msg, tuple)
            self.assertEqual(msg[0], message)
            messages_consumed += 1

        self.assertEqual(messages_consumed, 1)


class ChannelProcessDataEventTests(TestFramework):
    def test_channel_process_data_events(self):
        self.msg = None

        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver(consumer_tag='travis-ci')
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        def callback(msg):
            self.msg = msg

        channel._consumer_callbacks['travis-ci'] = callback
        channel.process_data_events()

        self.assertIsNotNone(self.msg, 'No message consumed')
        self.assertIsInstance(self.msg.body, str)
        self.assertEqual(self.msg.body.encode('utf-8'), message)

    def test_channel_process_data_events_as_tuple(self):
        self.msg = None

        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver(consumer_tag='travis-ci')
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        def callback(body, channel, method, properties):
            self.msg = (body, channel, method, properties)

        channel._consumer_callbacks['travis-ci'] = callback
        channel.process_data_events(to_tuple=True)

        self.assertIsNotNone(self.msg, 'No message consumed')

        body, channel, method, properties = self.msg

        self.assertIsInstance(body, bytes)
        self.assertIsInstance(channel, Channel)
        self.assertIsInstance(method, dict)
        self.assertIsInstance(properties, dict)
        self.assertEqual(body, message)


class ChannelStartConsumingTests(TestFramework):
    def test_channel_start_consuming(self):
        self.msg = None
        consumer_tag = 'travis-ci'

        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver(consumer_tag='travis-ci')
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        def callback(msg):
            self.msg = msg
            channel.set_state(channel.CLOSED)

        channel.add_consumer_tag(consumer_tag)
        channel._consumer_callbacks['travis-ci'] = callback
        channel.start_consuming()

        self.assertIsNotNone(self.msg, 'No message consumed')
        self.assertIsInstance(self.msg.body, str)
        self.assertEqual(self.msg.body.encode('utf-8'), message)

    def test_channel_start_consuming_idle_wait(self):
        self.msg = None
        consumer_tag = 'travis-ci'

        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        def add_inbound():
            deliver = specification.Basic.Deliver(consumer_tag='travis-ci')
            header = ContentHeader(body_size=message_len)
            body = ContentBody(value=message)

            channel._inbound = [deliver, header, body]

        def callback(msg):
            self.msg = msg
            channel.set_state(channel.CLOSED)

        channel.add_consumer_tag(consumer_tag)
        channel._consumer_callbacks[consumer_tag] = callback

        threading.Timer(function=add_inbound, interval=1).start()
        channel.start_consuming()

        self.assertIsNotNone(self.msg, 'No message consumed')
        self.assertIsInstance(self.msg.body, str)
        self.assertEqual(self.msg.body.encode('utf-8'), message)

    def test_channel_start_consuming_no_consumer_tags(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        channel._consumer_callbacks = ['fake']

        self.assertIsNone(channel.start_consuming())

    def test_channel_start_consuming_multiple_callbacks(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver_one = specification.Basic.Deliver(
            consumer_tag='travis-ci-1')
        deliver_two = specification.Basic.Deliver(
            consumer_tag='travis-ci-2')
        deliver_three = specification.Basic.Deliver(
            consumer_tag='travis-ci-3')
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [
            deliver_one, header, body,
            deliver_two, header, body,
            deliver_three, header, body
        ]

        def callback_one(msg):
            self.assertEqual(msg.method.get('consumer_tag'), 'travis-ci-1')
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)

        def callback_two(msg):
            self.assertEqual(msg.method.get('consumer_tag'), 'travis-ci-2')
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)

        def callback_three(msg):
            self.assertEqual(msg.method.get('consumer_tag'), 'travis-ci-3')
            self.assertIsInstance(msg.body, str)
            self.assertEqual(msg.body.encode('utf-8'), message)
            channel.set_state(channel.CLOSED)

        channel.add_consumer_tag('travis-ci-1')
        channel.add_consumer_tag('travis-ci-2')
        channel.add_consumer_tag('travis-ci-3')
        channel._consumer_callbacks['travis-ci-1'] = callback_one
        channel._consumer_callbacks['travis-ci-2'] = callback_two
        channel._consumer_callbacks['travis-ci-3'] = callback_three

        channel.start_consuming()
