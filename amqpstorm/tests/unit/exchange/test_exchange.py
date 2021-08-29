from pamqp.commands import Exchange as pamqp_exchange

from amqpstorm.channel import Channel
from amqpstorm.channel import Exchange
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class ExchangeTests(TestFramework):
    def test_exchange_declare(self):
        def on_declare(*_):
            channel.rpc.on_frame(pamqp_exchange.DeclareOk())

        connection = FakeConnection(on_write=on_declare)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        exchange = Exchange(channel)
        self.assertFalse(exchange.declare())

    def test_exchange_delete(self):
        def on_delete(*_):
            channel.rpc.on_frame(pamqp_exchange.DeleteOk())

        connection = FakeConnection(on_write=on_delete)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        exchange = Exchange(channel)
        self.assertFalse(exchange.delete())

    def test_exchange_bind(self):
        def on_bind(*_):
            channel.rpc.on_frame(pamqp_exchange.BindOk())

        connection = FakeConnection(on_write=on_bind)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        exchange = Exchange(channel)
        self.assertFalse(exchange.bind())

    def test_exchange_unbind(self):
        def on_unbind(*_):
            channel.rpc.on_frame(pamqp_exchange.UnbindOk())

        connection = FakeConnection(on_write=on_unbind)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        exchange = Exchange(channel)
        self.assertFalse(exchange.unbind())
