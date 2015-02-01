__author__ = 'eandersson'

import uuid
import threading
from time import sleep

from flask import Flask

import amqpstorm


app = Flask(__name__)


class RpcClient(object):
    """Asynchronous Rpc client."""

    def __init__(self, rpc_queue):
        self.queue = {}
        self.rpc_queue = rpc_queue
        self.connection = amqpstorm.Connection('localhost', 'guest', 'guest')
        self.channel = self.connection.channel()
        self.channel.queue.declare(rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        self.channel.basic.consume(self._on_response, no_ack=True,
                                   queue=self.callback_queue)
        self.channel.start_consuming()

    def _on_response(self, body, channel, header, properties):
        self.queue[properties['correlation_id'].decode('utf-8')] = body

    def send_request(self, payload):
        corr_id = str(uuid.uuid4())
        self.queue[corr_id] = None

        self.channel.basic.publish(exchange='',
                                   routing_key=self.rpc_queue,
                                   properties={
                                       'reply_to': self.callback_queue,
                                       'correlation_id': corr_id,
                                   },
                                   body=payload)
        return corr_id


@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Simple Flask implementation for making asynchronous Rpc calls. """
    corr_id = rpc_client.send_request(payload)

    while rpc_client.queue[corr_id] is None:
        sleep(0.1)

    return rpc_client.queue[corr_id]


if __name__ == '__main__':
    rpc_client = RpcClient('rpc_queue')
    app.run()
