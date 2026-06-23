"""
Consumer that connects to a RabbitMQ cluster of several nodes and fails over to
another node whenever the current one becomes unavailable.

Pass an explicit list of nodes, or a single round-robin DNS name that resolves
to multiple A records. The DNS name is re-resolved on every connection attempt,
so it picks up the resolver's rotation and any change to the node set.
"""
import logging
import random
import socket
import time

import amqpstorm
from amqpstorm import Connection

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()

CLUSTER_NODE_HOSTNAME = 'rmq.eandersson.net'
CLUSTER_NODES = [
    ('192.168.0.1', 5672),
    ('192.168.0.2', 5672),
    ('192.168.0.3', 5672),
]


class ClusteredConsumer:
    def __init__(self, nodes=None, hostname=None, port=5672, username='guest',
                 password='guest', queue='example_queue', timeout=5,
                 max_retries=None, **connection_kwargs):
        """
        :param list nodes: List of (hostname, port) cluster nodes.
        :param str hostname: Round-robin DNS name, re-resolved to its A records
                             on every connection attempt; use instead of nodes.
        :param int port: Port used together with hostname.
        :param str username: RabbitMQ username.
        :param str password: RabbitMQ password.
        :param str queue: Queue to consume from.
        :param int,float timeout: Per-node socket timeout. A lower value fails
                                  over to the next node faster when one is
                                  unreachable.
        :param int max_retries: Give up after this many full passes over the
                                node list (None retries forever).
        :param connection_kwargs: Extra keyword arguments forwarded to
                                  amqpstorm.Connection, e.g. ssl=True with
                                  ssl_options={...}, virtual_host or heartbeat.
        """
        self.nodes = list(nodes) if nodes else []
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.queue = queue
        self.timeout = timeout
        self.max_retries = max_retries
        self.connection_kwargs = connection_kwargs
        self.connection = None

    def resolve_nodes(self):
        """Return the current cluster nodes as (hostname, port) pairs.

            A configured round-robin DNS name is resolved fresh on every call,
            so each reconnect sees the latest set of A records.

        :rtype: list
        """
        if not self.hostname:
            return self.nodes
        addresses = []
        for info in socket.getaddrinfo(self.hostname, self.port,
                                       type=socket.SOCK_STREAM):
            address = info[4][0]
            if address not in addresses:
                addresses.append(address)
        return [(address, self.port) for address in addresses]

    def create_connection(self):
        """Connect to the first reachable node, failing over between nodes.

            Each pass tries every node once, in a random order to spread load
            across the cluster. If no node is reachable we back off (capped at
            30 seconds) and try the whole list again.

        :raises amqpstorm.AMQPConnectionError: If max_retries is set and no
                                               node could be reached.
        :return:
        """
        attempts = 0
        while True:
            attempts += 1
            try:
                nodes = self.resolve_nodes()
            except socket.gaierror as why:
                LOGGER.warning('Could not resolve %s - %s', self.hostname, why)
                nodes = []
            for hostname, port in random.sample(nodes, len(nodes)):
                try:
                    self.connection = Connection(
                        hostname, self.username, self.password,
                        port=port, timeout=self.timeout,
                        **self.connection_kwargs
                    )
                    LOGGER.info('Connected to cluster node %s:%d',
                                hostname, port)
                    return
                except amqpstorm.AMQPError as why:
                    LOGGER.warning('Node %s:%d unavailable - %s',
                                   hostname, port, why)
            if self.max_retries and attempts >= self.max_retries:
                raise amqpstorm.AMQPConnectionError(
                    f'no cluster node reachable after {attempts} attempts'
                )
            time.sleep(min(attempts * 2, 30))

    def start(self):
        """Consume from the cluster, failing over to another node on errors.

        :return:
        """
        if not self.connection:
            self.create_connection()
        while True:
            try:
                channel = self.connection.channel()
                channel.queue.declare(self.queue)
                channel.basic.consume(self, self.queue, no_ack=False)
                channel.start_consuming()
            except amqpstorm.AMQPError as why:
                LOGGER.warning(why)
                self.create_connection()
            except KeyboardInterrupt:
                self.connection.close()
                break

    def __call__(self, message):
        """Handle a delivered message.

        :param amqpstorm.Message message:
        :return:
        """
        print('Message:', message.body)
        message.ack()


if __name__ == '__main__':
    CONSUMER = ClusteredConsumer(nodes=CLUSTER_NODES)
    # CONSUMER = ClusteredConsumer(hostname=CLUSTER_NODE_HOSTNAME)
    CONSUMER.start()
