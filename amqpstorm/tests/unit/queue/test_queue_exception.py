from amqpstorm import Channel
from amqpstorm import exception
from amqpstorm.queue import Queue
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class QueueExceptionTests(TestFramework):
    def test_queue_declare_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        queue = Queue(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'queue should be a string',
            queue.declare, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'passive should be a boolean',
            queue.declare, 'travis-ci', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'durable should be a boolean',
            queue.declare, 'travis-ci', True, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'exclusive should be a boolean',
            queue.declare, 'travis-ci', True, True, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'auto_delete should be a boolean',
            queue.declare, 'travis-ci', True, True, True, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'arguments should be a dict or None',
            queue.declare, 'travis-ci', True, True, True, True, []
        )

    def test_queue_delete_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        queue = Queue(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'queue should be a string',
            queue.delete, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'if_unused should be a boolean',
            queue.delete, '', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'if_empty should be a boolean',
            queue.delete, '', True, None
        )

    def test_queue_purge_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        queue = Queue(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'queue should be a string',
            queue.purge, None
        )

    def test_queue_bind_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        queue = Queue(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'queue should be a string',
            queue.bind, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'exchange should be a string',
            queue.bind, '', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'routing_key should be a string',
            queue.bind, '', '', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'arguments should be a dict or None',
            queue.bind, '', '', '', []
        )

    def test_queue_unbind_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        queue = Queue(channel)

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'queue should be a string',
            queue.unbind, None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'exchange should be a string',
            queue.unbind, '', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'routing_key should be a string',
            queue.unbind, '', '', None
        )

        self.assertRaisesRegex(
            exception.AMQPInvalidArgument,
            'arguments should be a dict or None',
            queue.unbind, '', '', '', []
        )
