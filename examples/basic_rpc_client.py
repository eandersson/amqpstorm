__author__ = 'eandersson'
"""
RPC Client example based on code from the official RabbitMQ Tutorial.
http://www.rabbitmq.com/tutorials/tutorial-six-python.html
"""
import uuid

import amqpstorm


CONNECTION = amqpstorm.Connection('localhost', 'guest', 'guest')
CHANNEL = CONNECTION.channel()
CHANNEL.queue.declare(queue='rpc_queue')


class FibonacciRpcClient(object):
    def __init__(self):
        self.connection = amqpstorm.Connection('localhost', 'guest', 'guest')

        self.channel = self.connection.channel()

        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']

        self.channel.basic.consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, body, channel, header, properties):
        if self.correlation_id == properties['correlation_id'].decode('utf-8'):
            self.response = body

    def call(self, number):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic.publish(exchange='',
                                   routing_key='rpc_queue',
                                   properties={
                                       'reply_to': self.callback_queue,
                                       'correlation_id': self.correlation_id,
                                   },
                                   body=str(number))
        while not self.response:
            self.channel.process_data_events()
        return int(self.response)


fibonacci_rpc = FibonacciRpcClient()

print(" [x] Requesting fib(30)")
response = fibonacci_rpc.call(30)
print(" [.] Got %r" % (response,))
