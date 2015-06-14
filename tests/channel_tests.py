__author__ = 'eandersson'

import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pamqp import specification
from pamqp.body import ContentBody
from pamqp.header import ContentHeader

from amqpstorm import exception
from amqpstorm import Message
from amqpstorm import Channel

from tests.utility import FakeConnection


logging.basicConfig(level=logging.DEBUG)


class BasicChannelTests(unittest.TestCase):
    def test_build_message(self):
        channel = Channel(0, None, 360)

        message = b'Hello World!'
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]
        result = channel._build_message()

        self.assertEqual(result._body, message)

    def test_build_out_of_order_message(self):
        channel = Channel(0, None, 360)

        message = b'Hello World!'
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, deliver, header, body]
        result = channel._build_message()

        self.assertEqual(result, None)

    def test_build_message_body(self):
        channel = Channel(0, None, 360)

        message = b'Hello World!'
        message_len = len(message)

        body = ContentBody(value=message)
        channel._inbound = [body]
        result = channel._build_message_body(message_len)

        self.assertEqual(message, result)

    def test_build_empty_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        result = None
        for message in channel.build_inbound_messages(break_on_empty=True):
            result = message
        self.assertIsNone(result)

    def test_build_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)

        message = b'Hello World!'
        message_len = len(message)

        deliver = specification.Basic.Deliver()
        header = ContentHeader(body_size=message_len)
        body = ContentBody(value=message)

        channel._inbound = [deliver, header, body]

        for message in channel.build_inbound_messages(break_on_empty=True):
            self.assertIsInstance(message, Message)

    def test_build_multiple_inbound_messages(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)

        message = b'Hello World!'
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

    def test_basic_return(self):
        channel = Channel(0, None, 360)

        basic_return = specification.Basic.Return(reply_code=500,
                                                  reply_text=b'Error')
        channel._basic_return(basic_return)

        self.assertEqual(len(channel.exceptions), 1)
        why = channel.exceptions.pop(0)
        self.assertEqual(str(why), "Message not delivered: Error (500) "
                                   "to queue '' from exchange ''")

    def test_close_channel(self):
        channel = Channel(0, None, 360)

        # Set up Fake Channel.
        channel._inbound = [1, 2, 3]
        channel.set_state(channel.OPEN)
        channel._consumer_tags = [1, 2, 3]

        # Close Channel.
        # FixMe: Work around for reply_text not being a byte string
        # when not sent from RabbitMQ (i.e using the default pamqp message).
        channel._close_channel(specification.Channel.Close(reply_text=b''))

        self.assertEqual(channel._inbound, [])
        self.assertEqual(channel._consumer_tags, [])
        self.assertEqual(channel._state, channel.CLOSED)

    def test_check_error_throw_exception(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.exceptions.append(Exception('Test'))
        self.assertRaises(Exception, channel.check_for_errors)

    def test_check_error_no_exception(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        channel.check_for_errors()

    def test_check_error_channel_closed(self):
        channel = Channel(0, FakeConnection(), 360)
        self.assertRaises(exception.AMQPChannelError, channel.check_for_errors)

    def test_check_error_connection_closed(self):
        channel = Channel(0, FakeConnection(FakeConnection.CLOSED), 360)
        self.assertRaises(exception.AMQPConnectionError,
                          channel.check_for_errors)
