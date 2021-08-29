from pamqp.specification import Queue as pamqp_queue

from amqpstorm.channel import Channel
from amqpstorm.channel import Queue
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework


class QueueTests(TestFramework):
    def test_queue_declare(self):
        def on_declare(*_):
            channel.rpc.on_frame(pamqp_queue.DeclareOk())

        connection = FakeConnection(on_write=on_declare)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        queue = Queue(channel)

        expected_result = {
            'queue': '',
            'message_count': 0,
            'consumer_count': 0
        }
        self.assertEqual(queue.declare(), expected_result)

    def test_queue_delete(self):
        def on_delete(*_):
            channel.rpc.on_frame(pamqp_queue.DeleteOk())

        connection = FakeConnection(on_write=on_delete)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        exchange = Queue(channel)

        self.assertEqual(exchange.delete(), {'message_count': 0})

    def test_queue_purge(self):
        def on_purge(*_):
            channel.rpc.on_frame(pamqp_queue.PurgeOk())

        connection = FakeConnection(on_write=on_purge)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        exchange = Queue(channel)

        self.assertEqual(exchange.purge('travis_ci'), {'message_count': 0})

    def test_queue_bind(self):
        def on_bind(*_):
            channel.rpc.on_frame(pamqp_queue.BindOk())

        connection = FakeConnection(on_write=on_bind)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        exchange = Queue(channel)

        self.assertFalse(exchange.bind())

    def test_queue_unbind(self):
        def on_unbind(*_):
            channel.rpc.on_frame(pamqp_queue.UnbindOk())

        connection = FakeConnection(on_write=on_unbind)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        exchange = Queue(channel)

        self.assertFalse(exchange.unbind())
