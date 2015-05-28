__author__ = 'eandersson'

import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pamqp.body import ContentBody
from pamqp.header import ContentHeader
from pamqp import specification

from amqpstorm.channel import Channel


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

        self.assertEqual(result.body, message)

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

    def test_basic_return(self):
        channel = Channel(0, None, 360)

        basic_return = specification.Basic.Return(reply_code=500,
                                                  reply_text=b'Error')
        channel._basic_return(basic_return)

        self.assertEqual(len(channel.exceptions), 1)
        exception = channel.exceptions.pop(0)
        self.assertEqual(str(exception), "Message not delivered: Error (500) "
                                         "to queue '' from exchange ''")
