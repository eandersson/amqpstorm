from amqpstorm.compatibility import quote
from amqpstorm.management.basic import Basic
from amqpstorm.management.channel import Channel
from amqpstorm.management.connection import Connection
from amqpstorm.management.exchange import Exchange
from amqpstorm.management.healthchecks import HealthChecks
from amqpstorm.management.http_client import HTTPClient
from amqpstorm.management.queue import Queue
from amqpstorm.management.user import User
from amqpstorm.management.virtual_host import VirtualHost

API_ALIVENESS_TEST = 'aliveness-test/%s'
API_NODES = 'nodes'
API_OVERVIEW = 'overview'
API_WHOAMI = 'whoami'
API_TOP = 'top/%s'


class ManagementApi(object):
    """RabbitMQ Management Api

    e.g.
    ::

        from amqpstorm.management import ManagementApi
        client = ManagementApi('http://localhost:15672', 'guest', 'guest')
        client.user.create('my_user', 'password')
        client.user.set_permission(
            'my_user',
            virtual_host='/',
            configure_regex='.*',
            write_regex='.*',
            read_regex='.*'
        )
    """

    def __init__(self, api_url, username, password, timeout=10,
                 verify=None, cert=None):
        self.http_client = HTTPClient(
            api_url, username, password,
            timeout=timeout, verify=verify, cert=cert
        )
        self._basic = Basic(self.http_client)
        self._channel = Channel(self.http_client)
        self._connection = Connection(self.http_client)
        self._exchange = Exchange(self.http_client)
        self._healthchecks = HealthChecks(self.http_client)
        self._queue = Queue(self.http_client)
        self._user = User(self.http_client)
        self._virtual_host = VirtualHost(self.http_client)

    @property
    def basic(self):
        """RabbitMQ Basic Operations.

            e.g.
            ::

                client.basic.publish('Hello RabbitMQ', routing_key='my_queue')

        :rtype: amqpstorm.management.basic.Basic
        """
        return self._basic

    @property
    def channel(self):
        """RabbitMQ Channel Operations.

            e.g.
            ::

                client.channel.list()

        :rtype: amqpstorm.management.channel.Channel
        """
        return self._channel

    @property
    def connection(self):
        """RabbitMQ Connection Operations.

            e.g.
            ::

                client.connection.list()

        :rtype: amqpstorm.management.connection.Connection
        """
        return self._connection

    @property
    def exchange(self):
        """RabbitMQ Exchange Operations.

            e.g.
            ::

                client.exchange.declare('my_exchange')

        :rtype: amqpstorm.management.exchange.Exchange
        """
        return self._exchange

    @property
    def healthchecks(self):
        """RabbitMQ Healthchecks.

            e.g.
            ::

                client.healthchecks.get()

        :rtype: amqpstorm.management.healthchecks.Healthchecks
        """
        return self._healthchecks

    @property
    def queue(self):
        """RabbitMQ Queue Operations.

            e.g.
            ::

                client.queue.declare('my_queue', virtual_host='/')

        :rtype: amqpstorm.management.queue.Queue
        """
        return self._queue

    @property
    def user(self):
        """RabbitMQ User Operations.

            e.g.
            ::

                client.user.create('my_user', 'password')

        :rtype: amqpstorm.management.user.User
        """
        return self._user

    @property
    def virtual_host(self):
        """RabbitMQ VirtualHost Operations.

        :rtype: amqpstorm.management.virtual_host.VirtualHost
        """
        return self._virtual_host

    def aliveness_test(self, virtual_host='/'):
        """Aliveness Test.

        e.g.
        ::

            from amqpstorm.management import ManagementApi
            client = ManagementApi('http://localhost:15672', 'guest', 'guest')
            result = client.aliveness_test('/')
            if result['status'] == 'ok':
                print("RabbitMQ is alive!")
            else:
                print("RabbitMQ is not alive! :(")

        :param str virtual_host: Virtual host name

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        virtual_host = quote(virtual_host, '')
        return self.http_client.get(API_ALIVENESS_TEST %
                                    virtual_host)

    def overview(self):
        """Get Overview.

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        return self.http_client.get(API_OVERVIEW)

    def nodes(self):
        """Get Nodes.

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        return self.http_client.get(API_NODES)

    def top(self):
        """Top Processes.

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: list
        """
        nodes = []
        for node in self.nodes():
            nodes.append(self.http_client.get(API_TOP % node['name']))
        return nodes

    def whoami(self):
        """Who am I?

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        return self.http_client.get(API_WHOAMI)
