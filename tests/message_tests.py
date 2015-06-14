__author__ = 'eandersson'

import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm import Message


logging.basicConfig(level=logging.DEBUG)


class MessageTests(unittest.TestCase):
    def test_to_dict(self):
        body = b'Hello World'
        message = Message(body=body,
                          properties={'key': 'value'},
                          method={'key': 'value'},
                          channel=None)
        result = message.to_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result['body'], body)

    def test_to_tuple(self):
        body = b'Hello World'
        message = Message(body=body,
                          properties={'key': 'value'},
                          method={'key': 'value'},
                          channel=None)
        body, channel, method, properties = message.to_tuple()
        self.assertEqual(body, body)
        self.assertIsInstance(method, dict)
        self.assertIsInstance(properties, dict)
        self.assertIsNone(channel)