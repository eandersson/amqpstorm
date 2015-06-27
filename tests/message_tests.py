__author__ = 'eandersson'

import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm import Message


logging.basicConfig(level=logging.DEBUG)


class MessageTests(unittest.TestCase):
    def test_create_new_message(self):
        body = b'Hello World'
        message = Message.create(None, body,
                                 properties={'key': 'value',
                                             'headers': {
                                                 b'name': b'eandersson'}
                                             })
        self.assertIsInstance(message, Message)
        self.assertEqual(message._body, body)

        result = message.to_dict()
        self.assertIsNone(result['method'])
        self.assertEqual(result['body'], body)
        self.assertEqual(result['properties']['key'], 'value')

    def test_auto_decode_enabled(self):
        message = Message(body='Hello World',
                          properties={'key': 'value',
                                      'headers': {b'name': b'eandersson'}},
                          method={'key': 'value'},
                          channel=None)
        self.assertIn('name', message.properties['headers'])
        self.assertIn(b'name', message._properties['headers'])
        self.assertIsInstance(message.properties['headers']['name'], str)

    def test_auto_decode_when_method_is_none(self):
        message = Message(body='Hello World',
                          properties={'key': 'value'},
                          method=None,
                          channel=None)
        self.assertIsNone(message.method)

    def test_auto_decode_when_method_contains_list(self):
        method_data = {'key': [b'a', b'b']}
        message = Message(body='Hello World',
                          properties={'key': 'value'},
                          method=method_data,
                          channel=None)
        self.assertEqual(method_data['key'][0].decode('utf-8'),
                         message.method['key'][0])

    def test_auto_decode_when_properties_contains_list(self):
        prop_data = [1, 2, 3, 4, 5]
        message = Message(body='Hello World',
                          properties={'key': [1, 2, 3, 4, 5]},
                          method=prop_data,
                          channel=None)
        self.assertEqual(prop_data, message.properties['key'])
        self.assertEqual(prop_data[0], message.properties['key'][0])
        self.assertEqual(prop_data[4], message.properties['key'][4])

    def test_auto_decode_when_method_is_tuple(self):
        method_data = (1, 2, 3, 4, 5)
        message = Message(body='Hello World',
                          properties={'key': 'value'},
                          method=method_data,
                          channel=None)
        self.assertEqual(method_data, message.method)
        self.assertEqual(method_data[0], message.method[0])
        self.assertEqual(method_data[4], message.method[4])

    def test_auto_decode_disabled(self):
        message = Message(body='Hello World',
                          properties={'key': 'value',
                                      'headers': {b'name': b'eandersson'}},
                          method={'key': 'value'},
                          channel=None,
                          auto_decode=False)
        self.assertIn(b'name', message.properties['headers'])
        self.assertIsInstance(message.properties['headers'][b'name'], bytes)

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
