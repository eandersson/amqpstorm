# -*- coding: utf-8 -*-
import random
import string
import sys

from mock import Mock
from pamqp import specification
from pamqp.body import ContentBody
from pamqp.header import ContentHeader

from amqpstorm.channel import Basic
from amqpstorm.channel import Channel
from amqpstorm.compatibility import RANGE
from amqpstorm.exception import AMQPChannelError
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework
from amqpstorm.tests.utility import unittest


class BasicTests(TestFramework):
    def test_basic_qos(self):
        def on_qos_frame(*_):
            channel.rpc.on_frame(specification.Basic.QosOk())

        connection = FakeConnection(on_write=on_qos_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertEqual(basic.qos(), {})

    def test_basic_get(self):
        message = self.message.encode('utf-8')
        message_len = len(message)

        def on_get_frame(*_):
            channel.rpc.on_frame(specification.Basic.GetOk())
            channel.rpc.on_frame(ContentHeader(body_size=message_len))
            channel.rpc.on_frame(ContentBody(value=message))

        connection = FakeConnection(on_write=on_get_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        result = basic.get(queue='travis-ci')

        self.assertFalse(channel.rpc._request)
        self.assertFalse(channel.rpc._response)
        self.assertEqual(result.body, message.decode('utf-8'))

    def test_basic_get_to_dict(self):
        message = self.message.encode('utf-8')
        message_len = len(message)

        def on_get_frame(*_):
            channel.rpc.on_frame(specification.Basic.GetOk())
            channel.rpc.on_frame(ContentHeader(body_size=message_len))
            channel.rpc.on_frame(ContentBody(value=message))

        connection = FakeConnection(on_write=on_get_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        result = basic.get(queue='travis-ci', to_dict=True)

        self.assertFalse(channel.rpc._request)
        self.assertFalse(channel.rpc._response)
        self.assertEqual(result['body'], message)

    def test_basic_get_empty(self):
        def on_get_frame(*_):
            channel.rpc.on_frame(specification.Basic.GetEmpty())

        connection = FakeConnection(on_write=on_get_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        result = basic.get(queue='travis-ci')

        self.assertFalse(channel.rpc._request)
        self.assertFalse(channel.rpc._response)
        self.assertIsNone(result)

    def test_basic_get_fails(self):
        def on_get_frame(*_):
            pass

        connection = FakeConnection(on_write=on_get_frame)
        channel = Channel(9, connection, 0.1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegexp(
            AMQPChannelError,
            'rpc requests .* \(.*\) took too long',
            basic.get, 'travis-ci'
        )

        self.assertFalse(channel.rpc._request)
        self.assertFalse(channel.rpc._response)

    def test_basic_recover(self):
        def on_recover_frame(*_):
            channel.rpc.on_frame(specification.Basic.RecoverOk())

        connection = FakeConnection(on_write=on_recover_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertEqual(basic.recover(), {})

    def test_basic_consume(self):
        tag = 'travis-ci'

        def on_consume_frame(*_):
            channel.rpc.on_frame(specification.Basic.ConsumeOk(tag))

        connection = FakeConnection(on_write=on_consume_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertEqual(basic.consume(), tag)

    def test_basic_ack(self):
        def on_write(channel, frame):
            self.assertEqual(channel, 9)
            self.assertIsInstance(frame, specification.Basic.Ack)

        connection = FakeConnection(on_write=on_write)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertEqual(basic.ack(), None)

    def test_basic_nack(self):
        def on_write(channel, frame):
            self.assertEqual(channel, 9)
            self.assertIsInstance(frame, specification.Basic.Nack)

        connection = FakeConnection(on_write=on_write)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertEqual(basic.nack(), None)

    def test_basic_reject(self):
        def on_write(channel, frame):
            self.assertEqual(channel, 9)
            self.assertIsInstance(frame, specification.Basic.Reject)

        connection = FakeConnection(on_write=on_write)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertEqual(basic.reject(), None)

    def test_basic_publish(self):
        exchange = 'travis-ci'
        routing_key = 'hello'
        properties = {'headers': {
            'key': 'value'
        }}

        connection = FakeConnection()
        channel = Channel(9, connection, 0.01)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)
        basic.publish(body=self.message,
                      routing_key=routing_key,
                      exchange=exchange,
                      properties=properties,
                      mandatory=True,
                      immediate=True)

        channel_id, payload = connection.frames_out.pop()
        basic_publish, content_header, content_body = payload

        # Verify Channel ID
        self.assertEqual(channel_id, 9)

        # Verify Classes
        self.assertIsInstance(basic_publish, specification.Basic.Publish)
        self.assertIsInstance(content_header, ContentHeader)
        self.assertIsInstance(content_body, ContentBody)

        # Verify Content
        self.assertEqual(self.message, content_body.value.decode('utf-8'))
        self.assertEqual(exchange, basic_publish.exchange)
        self.assertEqual(routing_key, basic_publish.routing_key)
        self.assertTrue(basic_publish.immediate)
        self.assertTrue(basic_publish.mandatory)
        self.assertIn('key', dict(content_header.properties)['headers'])

    def test_basic_publish_confirms_ack(self):
        def on_publish_return_ack(*_):
            channel.rpc.on_frame(specification.Basic.Ack())

        connection = FakeConnection(on_write=on_publish_return_ack)
        channel = Channel(9, connection, 1)
        channel._confirming_deliveries = True
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertTrue(basic.publish(body=self.message,
                                      routing_key='travis-ci'))

    def test_basic_publish_confirms_nack(self):
        def on_publish_return_nack(*_):
            channel.rpc.on_frame(specification.Basic.Nack())

        connection = FakeConnection(on_write=on_publish_return_nack)
        channel = Channel(9, connection, 1)
        channel._confirming_deliveries = True
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertFalse(basic.publish(body=self.message,
                                       routing_key='travis-ci'))

    def test_basic_create_content_body(self):
        basic = Basic(None)

        results = []
        for frame in basic._create_content_body(self.message):
            results.append(frame)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].value, self.message)

    def test_basic_create_content_body_growing(self):
        basic = Basic(None)
        long_string = ''.join(random.choice(string.ascii_letters)
                              for _ in RANGE(32768))

        for index in RANGE(32768):
            results = []
            message = long_string[:index + 1]
            for frame in basic._create_content_body(message):
                results.append(frame)

            # Rebuild the string
            result_body = ''
            for frame in results:
                result_body += frame.value

            self.assertEqual(result_body, message)

    def test_basic_create_content_body_long_string(self):
        basic = Basic(None)

        message = self.message.encode('utf-8') * 80960
        results = []
        for frame in basic._create_content_body(message):
            results.append(frame)

        self.assertEqual(len(results), 23)

        # Rebuild the string
        result_body = b''
        for frame in results:
            result_body += frame.value

        # Confirm that it matches the original string.
        self.assertEqual(result_body, message)

    def test_basic_get_message(self):
        message = self.message.encode('utf-8')
        message_len = len(message)

        get_frame = specification.Basic.Get(queue='travis-ci',
                                            no_ack=False)

        def on_get_frame(*_):
            channel.rpc.on_frame(specification.Basic.GetOk())
            channel.rpc.on_frame(ContentHeader(body_size=message_len))
            channel.rpc.on_frame(ContentBody(value=message))

        connection = FakeConnection(on_write=on_get_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        result = basic._get_message(get_frame, auto_decode=False)

        self.assertEqual(result.body, message)

    def test_basic_get_message_auto_decode(self):
        message = self.message.encode('utf-8')
        message_len = len(message)

        get_frame = specification.Basic.Get(queue='travis-ci',
                                            no_ack=False)

        def on_get_frame(*_):
            channel.rpc.on_frame(specification.Basic.GetOk())
            channel.rpc.on_frame(ContentHeader(body_size=message_len))
            channel.rpc.on_frame(ContentBody(value=message))

        connection = FakeConnection(on_write=on_get_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        result = basic._get_message(get_frame, auto_decode=True)

        self.assertEqual(result.body.encode('utf-8'), message)

    def test_basic_get_message_empty_queue(self):
        get_frame = specification.Basic.Get(queue='travis-ci',
                                            no_ack=False)

        def on_get_frame(*_):
            channel.rpc.on_frame(specification.Basic.GetEmpty())

        connection = FakeConnection(on_write=on_get_frame)
        channel = Channel(9, connection, 1)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        result = basic._get_message(get_frame, auto_decode=False)

        self.assertEqual(result, None)

    def test_basic_get_content_body(self):
        body = ContentBody(value=self.message.encode('utf-8'))
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)
        uuid = channel.rpc.register_request([body.name])
        channel.rpc.on_frame(body)

        self.assertEqual(basic._get_content_body(uuid, len(self.message)),
                         self.message.encode('utf-8'))

    def test_basic_get_content_body_break_on_none_value(self):
        body = ContentBody(value=None)
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)
        uuid = channel.rpc.register_request([body.name])
        channel.rpc.on_frame(body)

        self.assertEqual(basic._get_content_body(uuid, 10), b'')

    @unittest.skipIf(sys.version_info[0] == 2, 'No bytes decoding in Python 2')
    def test_basic_py3_utf_8_payload(self):
        message = 'Hellå World!'
        basic = Basic(None)
        payload = basic._handle_utf8_payload(message, {})

        self.assertEqual(payload, b'Hell\xc3\xa5 World!')

    @unittest.skipIf(sys.version_info[0] == 3, 'No unicode obj in Python 3')
    def test_basic_py2_utf_8_payload(self):
        message = u'Hellå World!'
        basic = Basic(None)
        properties = {}
        payload = basic._handle_utf8_payload(message, properties)

        self.assertEqual(payload, 'Hell\xc3\xa5 World!')

    def test_basic_content_in_properties(self):
        basic = Basic(None)
        properties = {
            'content_encoding': 'ascii'
        }
        basic._handle_utf8_payload(self.message, properties)

        self.assertEqual(properties['content_encoding'], 'ascii')

    def test_basic_content_not_in_properties(self):
        basic = Basic(None)
        properties = {}
        basic._handle_utf8_payload(self.message, properties)

        self.assertEqual(properties['content_encoding'], 'utf-8')

    def test_basic_consume_add_tag(self):
        tag = 'travis-ci'
        channel = Channel(0, Mock(name='Connection'), 1)
        basic = Basic(channel)

        self.assertEqual(basic._consume_add_and_get_tag({'consumer_tag': tag}),
                         tag)
        self.assertEqual(channel.consumer_tags[0], tag)

    def test_basic_consume_rpc(self):
        tag = 'travis-ci'

        def on_publish_return_ack(_, frame):
            self.assertIsInstance(frame, specification.Basic.Consume)
            self.assertEqual(frame.arguments, {})
            self.assertEqual(frame.consumer_tag, tag)
            self.assertEqual(frame.exclusive, True)
            self.assertEqual(frame.no_ack, True)
            self.assertEqual(frame.exclusive, True)
            self.assertEqual(frame.queue, '')
            channel.rpc.on_frame(specification.Basic.ConsumeOk(tag))

        connection = FakeConnection(on_write=on_publish_return_ack)
        channel = Channel(9, connection, 1)
        channel.set_state(channel.OPEN)
        basic = Basic(channel)

        self.assertEqual(
            basic._consume_rpc_request({}, tag, True, True, True, ''),
            {'consumer_tag': 'travis-ci'})
