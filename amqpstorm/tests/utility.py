import functools
import logging
import time
import uuid

from amqpstorm.connection import Channel
from amqpstorm.connection import Connection
from amqpstorm.management import ManagementApi
from amqpstorm.tests import HOST
from amqpstorm.tests import HTTP_URL
from amqpstorm.tests import PASSWORD
from amqpstorm.tests import USERNAME

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class SslTLSv1_2(object):
    """Fake SSL Class with TLS v1_2 support used for Unit-Testing."""
    PROTOCOL_TLSv1_2 = 5


class SslTLSv1_1(object):
    """Fake SSL Class with TLS v1_1 support used for Unit-Testing."""
    PROTOCOL_TLSv1_1 = 4


class SslTLSv1(object):
    """Fake SSL Class with TLS v1 support used for Unit-Testing."""
    PROTOCOL_TLSv1 = 3


class SslTLSNone(object):
    """Fake SSL Class with no TLS support used for Unit-Testing."""
    pass


class FakeConnection(Connection):
    """Fake Connection for Unit-Testing."""

    def __init__(self, state=Connection.OPEN, on_write=None):
        super(FakeConnection, self).__init__('localhost', 'guest', 'guest',
                                             lazy=True)
        self.frames_out = []
        self.parameters = {
            'hostname': 'localhost',
            'port': 1234,
            'heartbeat': 60,
            'timeout': 30,
            'ssl': False,
            'ssl_options': {}
        }
        self.set_state(state)
        self.on_write = on_write

    def get_last_frame(self):
        """Get and pop the last frame written."""
        if self.frames_out:
            return self.frames_out.pop(0)[1]

    def write_frame(self, channel_id, frame_out):
        if self.on_write:
            self.on_write(channel_id, frame_out)
        self.frames_out.append((channel_id, frame_out))

    def write_frames(self, channel_id, frames_out):
        if self.on_write:
            self.on_write(channel_id, frames_out)
        self.frames_out.append((channel_id, frames_out))


class FakeChannel(Channel):
    """Fake Channel for Unit-Testing."""
    result = list()

    def __init__(self, state=Channel.OPEN):
        super(FakeChannel, self).__init__(0, None, 0.01)
        self.set_state(state)
        self._basic = FakeBasic(self)

    def close(self, **_):
        self.set_state(self.CLOSED)


class FakeBasic(object):
    """Fake Basic for Unit-Testing."""

    def __init__(self, channel):
        self.channel = channel

    def ack(self, delivery_tag=None, multiple=False):
        self.channel.result.append((delivery_tag, multiple))

    def nack(self, delivery_tag=None, multiple=False, requeue=True):
        self.channel.result.append((delivery_tag, multiple, requeue))

    def reject(self, delivery_tag=None, requeue=True):
        self.channel.result.append((delivery_tag, requeue))


class FakePayload(object):
    """Fake Payload for Unit-Testing."""
    __slots__ = ['name', 'value']

    def __init__(self, name, value=''):
        self.name = name
        self.value = value


class FakeFrame(object):
    """Fake Frame for Unit-Testing."""
    __slots__ = ['name', '_data_1']

    def __init__(self, name='FakeFrame'):
        self.name = name
        self._data_1 = 'hello world'

    def __iter__(self):
        for attribute in ['_data_1']:
            yield (attribute[1::], getattr(self, attribute))


class FakeHTTPClient(object):
    """Fake HTTP client for Unit-Testing."""

    def __init__(self, on_get=None, on_post=None):
        self.on_get = on_get
        self.on_post = on_post

    def get(self, path):
        return self.on_get(path)

    def post(self, path, payload):
        assert payload
        return self.on_post(path)


class MockLoggingHandler(logging.Handler):
    """Mock Logging Handler for Unit-Testing."""

    def __init__(self, *args, **kwargs):
        self.messages = {
            'warning': [],
            'error': [],
            'critical': [],
        }
        logging.Handler.__init__(self, *args, **kwargs)

    def clear(self):
        """Clear logging."""
        for name in self.messages:
            self.messages[name] = []

    def emit(self, record):
        levelname = record.levelname.lower()
        if levelname not in self.messages:
            return
        self.messages[record.levelname.lower()].append(record.getMessage())


class TestFramework(unittest.TestCase):
    """Test Base for all unit-tests."""
    message = str(uuid.uuid4())

    def __init__(self, *args, **kwargs):
        self.channel = None
        self.connection = None
        self.validate_logging = True
        self.logging_handler = None
        self._setup_logging_handler()
        super(TestFramework, self).__init__(*args, **kwargs)

    def setUp(self):
        self.validate_logging = True
        self.logging_handler.clear()
        if hasattr(self, 'configure'):
            getattr(self, 'configure')()

    def disable_logging_validation(self):
        """Disable logging validation."""
        self.validate_logging = False

    def get_last_log(self):
        for name in self.logging_handler.messages:
            if self.logging_handler.messages[name]:
                return self.logging_handler.messages[name].pop(0)

    def tearDown(self):
        if self.validate_logging:
            try:
                self.assertFalse(self.logging_handler.messages['warning'])
                self.assertFalse(self.logging_handler.messages['error'])
                self.assertFalse(self.logging_handler.messages['critical'])
            finally:
                self.logging_handler.clear()

    def _setup_logging_handler(self):
        """Setup the internal logging handler."""
        for handler in logging.root.handlers:
            if isinstance(handler, MockLoggingHandler):
                self.logging_handler = handler
        if not self.logging_handler:
            self.logging_handler = MockLoggingHandler()
            logging.root.addHandler(self.logging_handler)
        self.logging_handler.clear()


class TestFunctionalFramework(TestFramework):
    """Extended Test Base for functional unit-tests."""

    def __init__(self, *args, **kwargs):
        self.api = ManagementApi(HTTP_URL, USERNAME, PASSWORD, timeout=1)
        self.queue_name = None
        self.exchange_name = None
        self.virtual_host_name = None
        super(TestFunctionalFramework, self).__init__(*args, **kwargs)


def fake_function():
    """Fake Function used for Unit-Testing."""
    pass


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
    with Connection(HOST, USERNAME, PASSWORD, timeout=1) as connection:
        with connection.channel() as channel:
            if queue:
                for name in names:
                    channel.queue.delete(name)
            if exchange:
                for name in names:
                    channel.exchange.delete(name)
