from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class BasicFunctionalTests(TestFunctionalFramework):
    @setup()
    def test_functional_basic_qos(self):
        result = self.channel.basic.qos(prefetch_count=100)
        self.assertFalse(result)

    @setup(queue=True)
    def test_functional_basic_get(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        message = self.channel.basic.get(self.queue_name)
        self.assertEqual(message.body, self.message)
        message.ack()

    @setup(queue=True)
    def test_functional_basic_get_empty(self):
        self.channel.queue.declare(self.queue_name)

        message = self.channel.basic.get(self.queue_name)
        self.assertIsNone(message)

    @setup(queue=True)
    def test_functional_basic_cancel(self):
        self.channel.queue.declare(self.queue_name)
        consumer_tag = self.channel.basic.consume(None, self.queue_name)

        result = self.channel.basic.cancel(consumer_tag)
        self.assertEqual(result['consumer_tag'], consumer_tag)

    @setup(queue=True)
    def test_functional_basic_recover(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        self.assertFalse(self.channel.basic.recover(requeue=True))

    @setup(queue=True)
    def test_functional_basic_ack(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        message = self.channel.basic.get(self.queue_name)
        result = self.channel.basic.ack(delivery_tag=message.delivery_tag)

        self.assertIsNone(result)

        # Make sure the message wasn't requeued.
        self.assertFalse(self.channel.basic.get(self.queue_name))

    @setup(queue=True)
    def test_functional_basic_ack_multiple(self):
        message = None
        self.channel.queue.declare(self.queue_name)

        for _ in range(5):
            self.channel.basic.publish(self.message, self.queue_name)

        for _ in range(5):
            message = self.channel.basic.get(self.queue_name)

        self.assertIsNotNone(message)

        self.channel.basic.ack(
            delivery_tag=message.delivery_tag,
            multiple=True
        )

        # Make sure the message wasn't requeued.
        self.assertFalse(self.channel.basic.get(self.queue_name))

    @setup(queue=True)
    def test_functional_basic_nack(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        message = self.channel.basic.get(self.queue_name)
        result = self.channel.basic.nack(
            requeue=False,
            delivery_tag=message.delivery_tag
        )

        self.assertIsNone(result)

        # Make sure the message wasn't requeued.
        self.assertFalse(self.channel.basic.get(self.queue_name))

    @setup(queue=True)
    def test_functional_basic_nack_multiple(self):
        message = None
        self.channel.queue.declare(self.queue_name)

        for _ in range(5):
            self.channel.basic.publish(self.message, self.queue_name)

        for _ in range(5):
            message = self.channel.basic.get(self.queue_name)

        self.assertIsNotNone(message)
        self.channel.basic.nack(
            delivery_tag=message.delivery_tag,
            requeue=False,
            multiple=True
        )

        # Make sure the message wasn't requeued.
        self.assertFalse(self.channel.basic.get(self.queue_name))

    @setup(queue=True)
    def test_functional_basic_nack_requeue(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        message = self.channel.basic.get(self.queue_name)
        result = self.channel.basic.nack(
            requeue=True,
            delivery_tag=message.delivery_tag
        )

        self.assertIsNone(result)

        # Make sure the message was requeued.
        self.assertIsNotNone(self.channel.basic.get(self.queue_name))

    @setup(queue=True)
    def test_functional_basic_reject(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        message = self.channel.basic.get(self.queue_name)
        result = self.channel.basic.reject(
            requeue=False,
            delivery_tag=message.delivery_tag
        )

        self.assertIsNone(result)

        # Make sure the message wasn't requeued.
        self.assertFalse(self.channel.basic.get(self.queue_name))

    @setup(queue=True)
    def test_functional_basic_reject_requeue(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.basic.publish(self.message, self.queue_name)

        message = self.channel.basic.get(self.queue_name)
        result = self.channel.basic.reject(
            requeue=True,
            delivery_tag=message.delivery_tag
        )

        self.assertIsNone(result)

        # Make sure the message was requeued.
        self.assertIsNotNone(self.channel.basic.get(self.queue_name))
