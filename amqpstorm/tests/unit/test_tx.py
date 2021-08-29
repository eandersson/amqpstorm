from pamqp import commands

from amqpstorm.channel import Channel
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import TestFramework
from amqpstorm.tx import Tx


class TxTests(TestFramework):
    def test_tx_select(self):
        def on_tx_select(*_):
            channel.rpc.on_frame(commands.Tx.SelectOk())

        connection = FakeConnection(on_write=on_tx_select)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        tx = Tx(channel)

        self.assertIsInstance(tx.select(), dict)
        self.assertTrue(tx._tx_active)

    def test_tx_commit(self):
        def on_tx_commit(*_):
            channel.rpc.on_frame(commands.Tx.CommitOk())

        connection = FakeConnection(on_write=on_tx_commit)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        tx = Tx(channel)

        self.assertIsInstance(tx.commit(), dict)
        self.assertFalse(tx._tx_active)

    def test_tx_rollback(self):
        def on_tx_rollback(*_):
            channel.rpc.on_frame(commands.Tx.RollbackOk())

        connection = FakeConnection(on_write=on_tx_rollback)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        tx = Tx(channel)

        self.assertIsInstance(tx.rollback(), dict)
        self.assertFalse(tx._tx_active)

    def test_tx_with_statement(self):
        self._active_transaction = False

        def on_tx(*_):
            if not self._active_transaction:
                channel.rpc.on_frame(commands.Tx.SelectOk())
                self._active_transaction = True
                return
            self._active_transaction = False
            channel.rpc.on_frame(commands.Tx.CommitOk())

        connection = FakeConnection(on_write=on_tx)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        tx = Tx(channel)

        with tx:
            self.assertTrue(tx._tx_active)
        self.assertFalse(tx._tx_active)

    def test_tx_with_statement_already_commited(self):
        self._active_transaction = False

        def on_tx(*_):
            if not self._active_transaction:
                channel.rpc.on_frame(commands.Tx.SelectOk())
                self._active_transaction = True
                return
            self._active_transaction = False
            channel.rpc.on_frame(commands.Tx.CommitOk())

        connection = FakeConnection(on_write=on_tx)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        tx = Tx(channel)

        with tx:
            tx.commit()
            self.assertFalse(tx._tx_active)
        self.assertFalse(tx._tx_active)

    def test_tx_with_statement_when_raises(self):
        def on_tx(_, frame):
            if isinstance(frame, commands.Tx.Select):
                channel.rpc.on_frame(commands.Tx.SelectOk())
                return
            channel.rpc.on_frame(commands.Tx.CommitOk())

        connection = FakeConnection(on_write=on_tx)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        tx = Tx(channel)

        try:
            with tx:
                tx.commit()
                raise Exception('travis-ci')
        except Exception:
            self.assertEqual(self.get_last_log(),
                             'Leaving Transaction on exception: travis-ci')

        self.assertFalse(tx._tx_active)

    def test_tx_with_statement_when_failing(self):
        self._active_transaction = False

        def on_tx(*_):
            if not self._active_transaction:
                channel.rpc.on_frame(commands.Tx.SelectOk())
                self._active_transaction = True
                return
            self._active_transaction = False
            channel.rpc.on_frame(commands.Tx.RollbackOk())

        connection = FakeConnection(on_write=on_tx)
        channel = Channel(0, connection, 0.01)
        channel.set_state(Channel.OPEN)
        tx = Tx(channel)

        try:
            with tx:
                self.assertTrue(tx._tx_active)
                raise Exception('error')
        except Exception as why:
            self.assertEqual('error', str(why))

        self.assertFalse(tx._tx_active)
        self.assertEqual(self.get_last_log(),
                         'Leaving Transaction on exception: error')
