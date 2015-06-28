__author__ = 'eandersson'

import uuid
import logging

from datetime import datetime

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

    def test_message_default_properties(self):
        body = b'Hello World'

        message = Message.create(None, body)

        self.assertEqual(message.reply_to, '')
        self.assertEqual(message.content_encoding, 'UTF-8')
        self.assertEqual(message.content_type, 'text/plain')
        self.assertIsNone(message.priority)
        self.assertIsNone(message.delivery_mode)
        self.assertIsInstance(message.correlation_id, str)
        self.assertIsInstance(message.timestamp, datetime)

    def test_message_timestamp_custom_value(self):
        dt = datetime.now()

        message = Message.create(None, '', timestamp=dt)

        self.assertEqual(dt, message.timestamp)

    def test_message_content_encoding_custom_value(self):
        content_encoding = 'gzip'

        message = Message.create(None, '', content_encoding=content_encoding)

        self.assertEqual(content_encoding, message.content_encoding)

    def test_message_content_type_custom_value(self):
        content_type = 'application/json'

        message = Message.create(None, '', content_type=content_type)

        self.assertEqual(content_type, message.content_type)

    def test_message_delivery_mode_two(self):
        delivery_mode = 2

        message = Message.create(None, '', delivery_mode=delivery_mode)

        self.assertEqual(delivery_mode, message.delivery_mode)

    def test_message_priority_three(self):
        priority = 3

        message = Message.create(None, '', priority=priority)

        self.assertEqual(priority, message.priority)

    def test_message_correlation_id_custom_value(self):
        correlation_id = str(uuid.uuid4())

        message = Message.create(None, '', correlation_id=correlation_id)

        self.assertEqual(correlation_id, message.correlation_id)

    def test_message_reply_to_custom_value(self):
        reply_to = str(uuid.uuid4())

        message = Message.create(None, '', reply_to=reply_to)

        self.assertEqual(reply_to, message.reply_to)

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

    def test_json(self):
        body = '{"key": "value"}'
        message = Message(body=body, channel=None)

        result = message.json()

        self.assertIsInstance(result, dict)
        self.assertEqual(result['key'], 'value')

    def test_dict(self):
        body = b'Hello World'
        properties = {'key': 'value'}
        method = {b'alternative': 'value'}
        message = Message(body=body,
                          properties=properties,
                          method=method,
                          channel=None)

        result = dict(message)

        self.assertIsInstance(result, dict)
        self.assertEqual(result['body'], body)
        self.assertEqual(result['properties'], properties)
        self.assertEqual(result['method'], method)

    def test_to_dict(self):
        body = b'Hello World'
        properties = {'key': 'value'}
        method = {b'alternative': 'value'}
        message = Message(body=body,
                          properties=properties,
                          method=method,
                          channel=None)

        result = message.to_dict()

        self.assertIsInstance(result, dict)
        self.assertEqual(result['body'], body)
        self.assertEqual(result['properties'], properties)
        self.assertEqual(result['method'], method)

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
