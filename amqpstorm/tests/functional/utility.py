import functools
import time

from amqpstorm import Connection
from amqpstorm.management import exception
from amqpstorm.management import ManagementApi
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import USERNAME
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import HOST
from amqpstorm.tests.utility import TestFramework


class TestFunctionalFramework(TestFramework):
    """Extended Test Base for functional unit-tests."""

    def __init__(self, *args, **kwargs):
        self.api = ManagementApi(HTTP_URL, USERNAME, PASSWORD, timeout=1)
        self.queue_name = None
        self.exchange_name = None
        self.virtual_host_name = None
        super(TestFunctionalFramework, self).__init__(*args, **kwargs)


def retry_function_wrapper(callable_function, retry_limit=10,
                           sleep_interval=1):
    """Retry wrapper used to retry functions before failing.

    :param callable_function: Function to call.
    :param retry_limit: Re-try limit.
    :param sleep_interval: Sleep interval between retries.

    :return:
    """
    retries = retry_limit
    while retries > 0:
        # noinspection PyCallingNonCallable
        result = callable_function()
        if result:
            return result
        retries -= 1
        time.sleep(sleep_interval)


def setup(new_connection=True, new_channel=True, queue=False, exchange=False,
          override_names=None):
    """Set up default testing scenario.

    :param new_connection: Create a new connection.
    :param new_channel: Create a new channel.
    :param queue: Remove any queues created by the test.
    :param exchange: Remove any exchanges created by the test.
    :param override_names: Override default queue/exchange naming.

    :return:
    """

    def outer(f):
        @functools.wraps(f)
        def setup_wrapper(self, *args, **kwargs):
            name = f.__name__
            self.queue_name = name
            self.exchange_name = name
            self.virtual_host_name = name
            if new_connection:
                self.connection = Connection(HOST, USERNAME, PASSWORD,
                                             timeout=1)
                if new_channel:
                    self.channel = self.connection.channel()
            try:
                result = f(self, *args, **kwargs)
            finally:
                names = [name]
                if override_names:
                    names = override_names
                clean(names, queue, exchange)
                if self.channel:
                    self.channel.close()
                if self.connection:
                    self.connection.close()
            return result

        setup_wrapper.__wrapped_function = f
        setup_wrapper.__wrapper_name = 'setup_wrapper'
        return setup_wrapper

    return outer


def clean(names, queue=False, exchange=False):
    """Clean any queues or exchanges created by the test.

    :param names: Queue/Exchange names.
    :param queue: Remove queues.
    :param exchange: Remove exchanges.

    :return:
    """
    if not queue and not exchange:
        return
    api = ManagementApi(HTTP_URL, USERNAME, PASSWORD, timeout=10)
    if queue:
        _delete_queues(api, names)
    if exchange:
        _delete_exchanges(api, names)


def _delete_exchanges(management_api, exchanges):
    """Delete exchanges.

    :param ManagementApi management_api:
    :param exchanges:
    """
    for name in exchanges:
        try:
            management_api.exchange.delete(name)
        except exception.ApiError:
            pass


def _delete_queues(api, queues):
    """Delete queues.

    :param ManagementApi api:
    :param list exchanges:
    """
    for name in queues:
        try:
            api.queue.delete(name)
        except exception.ApiError:
            pass
