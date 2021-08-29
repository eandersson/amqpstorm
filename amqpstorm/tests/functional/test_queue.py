import amqpstorm
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class QueueFunctionalTests(TestFunctionalFramework):
    @setup(queue=True)
    def test_functional_queue_declare(self):
        self.channel.queue.declare(self.queue_name,
                                   passive=False,
                                   durable=True, auto_delete=True)

        expected_result = {
            'consumer_count': 0,
            'message_count': 0,
            'queue': self.queue_name
        }

        self.assertEqual(
            expected_result,
            self.channel.queue.declare(self.queue_name,
                                       passive=True)
        )

    @setup(queue=True)
    def test_functional_queue_delete(self):
        self.channel.queue.declare(self.queue_name)
        self.channel.queue.delete(self.queue_name,
                                  if_unused=True)
        self.assertRaises(amqpstorm.AMQPChannelError,
                          self.channel.queue.declare,
                          self.queue_name, passive=True)

    @setup(queue=True)
    def test_functional_queue_purge(self):
        messages_to_send = 10
        self.channel.queue.declare(self.queue_name, auto_delete=True)
        for _ in range(messages_to_send):
            self.channel.basic.publish(self.message, self.queue_name)
        result = self.channel.queue.purge(self.queue_name)
        self.assertEqual(result['message_count'], messages_to_send)

    @setup(queue=True)
    def test_functional_queue_bind(self):
        self.channel.queue.declare(self.queue_name,
                                   passive=False,
                                   durable=True, auto_delete=True)
        self.assertEqual(self.channel.queue.bind(queue=self.queue_name,
                                                 exchange='amq.direct'), {})

    @setup(queue=True)
    def test_functional_queue_bind_no_queue(self):
        self.channel.queue.declare(self.queue_name,
                                   passive=False,
                                   durable=True, auto_delete=True)
        self.assertEqual(self.channel.queue.bind(queue=self.queue_name,
                                                 exchange='amq.direct'), {})

    @setup(queue=True)
    def test_functional_queue_unbind(self):
        self.channel.queue.declare(self.queue_name,
                                   passive=False,
                                   durable=True, auto_delete=True)
        self.channel.queue.bind(queue=self.queue_name, exchange='amq.direct')
        self.assertEqual(self.channel.queue.unbind(queue=self.queue_name,
                                                   exchange='amq.direct'), {})
