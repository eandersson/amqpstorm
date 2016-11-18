from amqpstorm import Channel
from amqpstorm import exception
from amqpstorm.exchange import Exchange
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class ExchangeExceptionTests(TestFramework):
    def test_exchange_declare_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        exchange = Exchange(channel)

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'exchange should be a string',
            exchange.declare, None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'exchange_type should be a string',
            exchange.declare, 'travis-ci', None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'passive should be a boolean',
            exchange.declare, 'travis-ci', 'travis-ci', None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'durable should be a boolean',
            exchange.declare, 'travis-ci', 'travis-ci', True, None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'auto_delete should be a boolean',
            exchange.declare, 'travis-ci', 'travis-ci', True, True, None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'arguments should be a dict or None',
            exchange.declare, 'travis-ci', 'travis-ci', True, True, True, []
        )

    def test_exchange_delete_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        exchange = Exchange(channel)

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'exchange should be a string',
            exchange.delete, None
        )

    def test_exchange_bind_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        exchange = Exchange(channel)

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'destination should be a string',
            exchange.bind, None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'source should be a string',
            exchange.bind, '', None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'routing_key should be a string',
            exchange.bind, '', '', None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'arguments should be a dict or None',
            exchange.bind, '', '', '', []
        )

    def test_exchange_unbind_invalid_parameter(self):
        channel = Channel(0, FakeConnection(), 360)
        channel.set_state(Channel.OPEN)
        exchange = Exchange(channel)

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'destination should be a string',
            exchange.unbind, None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'source should be a string',
            exchange.unbind, '', None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'routing_key should be a string',
            exchange.unbind, '', '', None
        )

        self.assertRaisesRegexp(
            exception.AMQPInvalidArgument,
            'arguments should be a dict or None',
            exchange.unbind, '', '', '', []
        )
