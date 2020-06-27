import time

from amqpstorm.exception import AMQPChannelError
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class TxFunctionalTests(TestFunctionalFramework):
    @setup()
    def test_functional_tx_select(self):
        self.channel.tx.select()

    @setup()
    def test_functional_tx_select_multiple(self):
        for _ in range(10):
            self.channel.tx.select()

    @setup(queue=True)
    def test_functional_tx_commit(self):
        self.channel.tx.select()

        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        # Sleep for 0.1s to make sure RabbitMQ has time to catch up.
        time.sleep(0.1)

        queue_status = self.channel.queue.declare(self.queue_name,
                                                  passive=True)
        self.assertEqual(queue_status['message_count'], 0)

        self.channel.tx.commit()

        queue_status = self.channel.queue.declare(self.queue_name,
                                                  passive=True)
        self.assertEqual(queue_status['message_count'], 1)

    @setup(queue=True)
    def test_functional_tx_commit_multiple(self):
        self.channel.tx.select()

        self.channel.queue.declare(self.queue_name)
        for _ in range(10):
            self.channel.basic.publish(self.message, self.queue_name)

        # Sleep for 0.1s to make sure RabbitMQ has time to catch up.
        time.sleep(0.1)

        queue_status = self.channel.queue.declare(self.queue_name,
                                                  passive=True)
        self.assertEqual(queue_status['message_count'], 0)

        self.channel.tx.commit()

        queue_status = self.channel.queue.declare(self.queue_name,
                                                  passive=True)
        self.assertEqual(queue_status['message_count'], 10)

    @setup()
    def test_functional_tx_commit_without_select(self):
        self.assertRaisesRegexp(
            AMQPChannelError,
            'Channel 1 was closed by remote server: '
            'PRECONDITION_FAILED - channel is not transactional',
            self.channel.tx.commit
        )

    @setup(queue=True)
    def test_functional_tx_rollback(self):
        self.channel.tx.select()

        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        self.channel.tx.rollback()

        queue_status = self.channel.queue.declare(self.queue_name,
                                                  passive=True)
        self.assertEqual(queue_status['message_count'], 0)

    @setup(queue=True)
    def test_functional_tx_rollback_multiple(self):
        self.channel.tx.select()

        self.channel.queue.declare(self.queue_name)
        for _ in range(10):
            self.channel.basic.publish(self.message, self.queue_name)

        self.channel.tx.rollback()

        queue_status = self.channel.queue.declare(self.queue_name,
                                                  passive=True)
        self.assertEqual(queue_status['message_count'], 0)

    @setup()
    def test_functional_tx_rollback_without_select(self):
        self.assertRaisesRegexp(
            AMQPChannelError,
            'Channel 1 was closed by remote server: '
            'PRECONDITION_FAILED - channel is not transactional',
            self.channel.tx.rollback
        )
