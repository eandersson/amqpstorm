"""
A simple RPC Client.
"""
import amqpstorm

from amqpstorm import Message


class FibonacciRpcClient(object):
    def __init__(self, host, username, password):
        """
        :param host: RabbitMQ Server e.g. localhost
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
        message = Message.create(self.channel, body=str(number))
        message.reply_to = self.callback_queue
        self.correlation_id = message.correlation_id
        message.publish(routing_key='rpc_queue')

        while not self.response:
            self.channel.process_data_events()
        return int(self.response)

    def _on_response(self, message):
        if self.correlation_id != message.correlation_id:
            return
        self.response = message.body


if __name__ == '__main__':
    FIBONACCI_RPC = FibonacciRpcClient('localhost', 'guest', 'guest')

    print(" [x] Requesting fib(30)")
    RESPONSE = FIBONACCI_RPC.call(30)
    print(" [.] Got %r" % (RESPONSE,))
    FIBONACCI_RPC.close()
