import logging
import uuid

from amqpstorm.connection import Channel
from amqpstorm.connection import Connection

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

    def __init__(self, connection=None, state=Channel.OPEN):
        if not connection:
            connection = FakeConnection()
        super(FakeChannel, self).__init__(1, connection, 0.01)
        self.set_state(state)
        self._basic = FakeBasic(self)

    def close(self, **_):
        self.set_state(self.CLOSED)


class FakeBasic(object):
    """Fake Basic for Unit-Testing."""

    def __init__(self, channel):
        self.channel = channel

    def ack(self, delivery_tag=0, multiple=False):
        self.channel.result.append((delivery_tag, multiple))

    def nack(self, delivery_tag=0, multiple=False, requeue=True):
        self.channel.result.append((delivery_tag, multiple, requeue))

    def reject(self, delivery_tag=0, requeue=True):
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


def fake_function():
    """Fake Function used for Unit-Testing."""
    pass
