__author__ = 'eandersson'
"""
RPC Client example based on code from the official RabbitMQ Tutorial.
http://www.rabbitmq.com/tutorials/tutorial-six-python.html
"""
import uuid

import amqpstorm


class FibonacciRpcClient(object):
    def __init__(self, host, username, password):
        """
        :param host: RabbitMQ Server e.g. 127.0.0.1
        :param username: RabbitMQ Username e.g. guest
        :param password: RabbitMQ Password e.g. guest
        :return:
        """
        self.host = host
        self.username = username
        self.password = password
        self.channel = None
        self.response = None
        self.connection = None
        self.callback_queue = None
        self.correlation_id = None
        self.open()

    def open(self):
        self.connection = amqpstorm.Connection(self.host,
                                               self.username,
                                               self.password)

        self.channel = self.connection.channel()

        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']

        self.channel.basic.consume(self._on_response, no_ack=True,
                                   queue=self.callback_queue)

    def close(self):
        self.channel.stop_consuming()
        self.channel.close()
        self.connection.close()

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

    def _on_response(self, body, channel, header, properties):
        if self.correlation_id != properties['correlation_id'].decode('utf-8'):
            return
        self.response = body


fibonacci_rpc = FibonacciRpcClient('127.0.0.1', 'guest', 'guest')

print(" [x] Requesting fib(30)")
response = fibonacci_rpc.call(30)
print(" [.] Got %r" % (response,))
fibonacci_rpc.close()