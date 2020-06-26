"""
Example of a Flask web application using RabbitMQ for RPC calls.
"""
import threading
from time import sleep

import amqpstorm
from amqpstorm import Message
from flask import Flask

APP = Flask(__name__)


class RpcClient(object):
    """Asynchronous Rpc client."""

    def __init__(self, host, username, password, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self.open()

    def open(self):
        """Open Connection."""
        self.connection = amqpstorm.Connection(self.host, self.username,
                                               self.password)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, no_ack=True,
                                   queue=self.callback_queue)
        self._create_process_thread()

    def _create_process_thread(self):
        """Create a thread responsible for consuming messages in response
        RPC requests.
        """
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        """Process Data Events using the Process Thread."""
        self.channel.start_consuming()

    def _on_response(self, message):
        """On Response store the message with the correlation id in a local
        dictionary.
        """
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        # Create the Message object.
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue

        # Create an entry in our local dictionary, using the automatically
        # generated correlation_id as our key.
        self.queue[message.correlation_id] = None

        # Publish the RPC request.
        message.publish(routing_key=self.rpc_queue)

        # Return the Unique ID used to identify the request.
        return message.correlation_id


@APP.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Simple Flask implementation for making asynchronous Rpc calls. """

    # Send the request and store the requests Unique ID.
    corr_id = RPC_CLIENT.send_request(payload)

    # Wait until we have received a response.
    # TODO: Add a timeout here and clean up if it fails!
    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)

    # Return the response to the user.
    return RPC_CLIENT.queue.pop(corr_id)


if __name__ == '__main__':
    RPC_CLIENT = RpcClient('localhost', 'guest', 'guest', 'rpc_queue')
    APP.run()
