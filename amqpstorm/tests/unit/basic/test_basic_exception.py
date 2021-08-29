from pamqp import specification
from pamqp.body import ContentBody

from amqpstorm import Channel
from amqpstorm import exception
from amqpstorm.basic import Basic
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class BasicExceptionTests(TestFramework):
    def test_basic_qos_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'prefetch_count should be an integer',
            basic.qos, 'travis-ci'
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'prefetch_size should be an integer',
            basic.qos, 1, 'travis-ci'
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'global_ should be a boolean',
            basic.qos, 1, 1, 'travis-ci'
        )

    def test_basic_get_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'queue should be a string',
            basic.get, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'no_ack should be a boolean',
            basic.get, '', 'travis-ci'
        )

        channel.consumer_tags.append('travis-ci')

        self.assertRaisesRegex(
            exception.AMQPChannelError,
            "Cannot call 'get' when channel "
            "is set to consume",
            basic.get, '', True, 'travis-ci'
        )

    def test_basic_get_invalid_message_impl(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'message_impl should be derived from BaseMessage',
            basic.get, message_impl=int
        )

    def test_basic_recover_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'requeue should be a boolean',
            basic.recover, None
        )

    def test_basic_consume_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'queue should be a string',
            basic.consume, None, 1
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'consumer_tag should be a string',
            basic.consume, None, '', 1
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'exclusive should be a boolean',
            basic.consume, None, '', '', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'no_ack should be a boolean',
            basic.consume, None, '', '', True, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'no_local should be a boolean',
            basic.consume, None, '', '', True, True, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'arguments should be a dict or None',
            basic.consume, None, '', '', True, True, True, []
        )

    def test_basic_cancel_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'consumer_tag should be a string',
            basic.cancel, None
        )

    def test_basic_publish_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'body should be a string',
            basic.publish, None, ''
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'routing_key should be a string',
            basic.publish, '', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'exchange should be a string',
            basic.publish, '', '', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'properties should be a dict or None',
            basic.publish, '', '', '', []
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'properties should be a dict or None',
            basic.publish, '', '', '', 1
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'mandatory should be a boolean',
            basic.publish, '', '', '', {}, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'immediate should be a boolean',
            basic.publish, '', '', '', {}, True, None
        )

    def test_basic_ack_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'delivery_tag should be an integer',
            basic.ack, 'travis-ci'
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'delivery_tag should be an integer',
            basic.ack, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'multiple should be a boolean',
            basic.ack, 1, None
        )

    def test_basic_nack_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'delivery_tag should be an integer',
            basic.nack, 'travis-ci'
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'multiple should be a boolean',
            basic.nack, 1, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'requeue should be a boolean',
            basic.nack, 1, True, None
        )

    def test_basic_reject_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'delivery_tag should be an integer',
            basic.reject, 'travis-ci'
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'delivery_tag should be an integer',
            basic.reject, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'requeue should be a boolean',
            basic.reject, 1, None
        )

    def test_basic_get_content_body_timeout_error(self):
        body = ContentBody(value=self.message)
        channel = Channel(0, FakeConnection(), 0.01)
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)
        uuid = channel.rpc.register_request([body.name])

        self.assertRaisesRegex(
            exception.AMQPChannelError,
            r'rpc requests .* \(.*\) took too long',
            basic._get_content_body, uuid, len(self.message)
        )

    def test_basic_publish_confirms_raises_on_timeout(self):
        connection = FakeConnection()
        channel = Channel(9, connection, 0.01)
        channel._confirming_deliveries = True
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPChannelError,
            r'rpc requests .* \(.*\) took too long',
            basic.publish, body=self.message,
            routing_key='travis-ci'
        )

    def test_basic_publish_confirms_raises_on_invalid_frame(self):
        def on_publish_return_invalid_frame(*_):
            channel.rpc.on_frame(specification.Basic.Cancel())

        connection = FakeConnection(on_write=on_publish_return_invalid_frame)
        channel = Channel(9, connection, 0.01)
        channel._confirming_deliveries = True
        channel.set_state(Channel.OPEN)
        basic = Basic(channel)

        self.assertRaisesRegex(
            exception.AMQPChannelError,
            r'rpc requests .* \(.*\) took too long',
            basic.publish, body=self.message,
            routing_key='travis-ci'
        )
