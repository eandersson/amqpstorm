import threading

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
        channel = Channel(0, None, 360)

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
        channel = Channel(0, None, 360)

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
        channel = Channel(0, None, 360)

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
        channel = Channel(0, None, 360)

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
        channel = Channel(0, None, 360)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=10)

        channel._inbound = [deliver, header]
        result = channel._build_message_headers()

        self.assertIsInstance(result[0], specification.Basic.Deliver)
        self.assertIsInstance(result[1], ContentHeader)
        self.assertEqual(result[1].body_size, 10)

    def test_channel_build_message_headers_out_of_order(self):
        channel = Channel(0, None, 360)

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
        channel = Channel(0, None, 360)
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
        channel = Channel(0, None, 360)

        message = self.message.encode('utf-8')
        message_len = len(message)

        body = ContentBody(value=message)
        channel._inbound = [body]
        result = channel._build_message_body(message_len)

        self.assertEqual(message, result)

    def test_channel_build_message_body_break_on_none_value(self):
        channel = Channel(0, None, 360)

        message = self.message
        message_len = len(message)

        body = ContentBody(value=None)
        channel._inbound = [body]
        result = channel._build_message_body(message_len)

        self.assertEqual(result, b'')

    def test_channel_build_message_body_break_on_empty_value(self):
        channel = Channel(0, None, 360)

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

    def test_channel_build_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)

        message = self.message.encode('utf-8')
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        for message in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(message, Message)

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

        index = 0
        for message in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(message, Message)
            index += 1

        self.assertEqual(index, 4)

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

        index = 0
        for message in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(message, Message)
            index += 1

        self.assertEqual(index, 10000)
