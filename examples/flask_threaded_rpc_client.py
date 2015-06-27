__author__ = 'eandersson'

import uuid
import threading
from time import sleep

import amqpstorm
from flask import Flask

from examples import HOST
from examples import USERNAME
from examples import PASSWORD


app = Flask(__name__)


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
         to RPC requests.
        """
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        """Process Data Events using the Process Thread."""
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        """On Response store the message with the correlation id in a local
         dictionary.
        """
        self.queue[message.properties['correlation_id']] = message.body

    def send_request(self, payload):
        # Generate a Unique ID used to identify the request.
        corr_id = str(uuid.uuid4())

        # Create an entry in our local dictionary.
        self.queue[corr_id] = None

        # Publish the RPC request.
        self.channel.basic.publish(exchange='',
                                   routing_key=self.rpc_queue,
                                   properties={
                                       'reply_to': self.callback_queue,
                                       'correlation_id': corr_id,
                                   },
                                   body=payload)

        # Return the Unique ID used to identify the request.
        return corr_id


@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Simple Flask implementation for making asynchronous Rpc calls. """

    # Send the request and store the requests Unique ID.
    corr_id = rpc_client.send_request(payload)

    # Wait until we have received a response.
    while rpc_client.queue[corr_id] is None:
        sleep(0.1)

    # Return the response to the user.
    return rpc_client.queue[corr_id]


if __name__ == '__main__':
    rpc_client = RpcClient(HOST, USERNAME, PASSWORD, 'rpc_queue')
    app.run()
