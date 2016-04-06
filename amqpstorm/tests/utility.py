from amqpstorm import compatibility
from amqpstorm.base import Stateful
from amqpstorm.exception import AMQPInvalidArgument


class FakeConnection(Stateful):
    """Fake Connection for Unit-Testing."""
    frames_out = []
    parameters = {
        'hostname': 'localhost',
        'port': 1234,
        'heartbeat': 60,
        'timeout': 30,
        'ssl': False
    }

    def __init__(self, state=3):
        super(FakeConnection, self).__init__()
        self.set_state(state)

    def write_frame(self, channel_id, frame_out):
        self.frames_out.append((channel_id, frame_out))

    def write_frames(self, channel_id, frames_out):
        self.frames_out.append((channel_id, frames_out))


class FakeChannel(Stateful):
    """Fake Channel for Unit-Testing."""
    result = list()

    def __init__(self, state=Stateful.OPEN):
        super(FakeChannel, self).__init__()
        self.set_state(state)
        self.basic = FakeBasic(self)

    def close(self):
        self.set_state(self.CLOSED)


class FakeBasic(object):
    """Fake Basic for Unit-Testing."""

    def __init__(self, channel):
        self.channel = channel

    def ack(self, delivery_tag=None, multiple=False):
        if delivery_tag is not None \
                and not compatibility.is_integer(delivery_tag):
            raise AMQPInvalidArgument('delivery_tag should be an integer '
                                      'or None')
        elif not isinstance(multiple, bool):
            raise AMQPInvalidArgument('multiple should be a boolean')
        self.channel.result.append((delivery_tag, multiple))

    def nack(self, delivery_tag=None, multiple=False, requeue=True):
        if delivery_tag is not None \
                and not compatibility.is_integer(delivery_tag):
            raise AMQPInvalidArgument('delivery_tag should be an integer '
                                      'or None')
        elif not isinstance(multiple, bool):
            raise AMQPInvalidArgument('multiple should be a boolean')
        elif not isinstance(requeue, bool):
            raise AMQPInvalidArgument('requeue should be a boolean')
        self.channel.result.append((delivery_tag, multiple, requeue))

    def reject(self, delivery_tag=None, requeue=True):
        if delivery_tag is not None \
                and not compatibility.is_integer(delivery_tag):
            raise AMQPInvalidArgument('delivery_tag should be an integer '
                                      'or None')
        elif not isinstance(requeue, bool):
            raise AMQPInvalidArgument('requeue should be a boolean')
        self.channel.result.append((delivery_tag, requeue))


class FakePayload(object):
    """Fake Payload for Unit-Testing."""
    __slots__ = ['name']

    def __iter__(self):
        for attribute in self.__slots__:
            yield (attribute, getattr(self, attribute))

    def __init__(self, name):
        self.name = name


class FakeFrame(object):
    """Fake Frame for Unit-Testing."""
    _data_1 = 'hello'
    _data_2 = 'world'

    def __init__(self, name='FakeFrame'):
        self.name = name

    def __iter__(self):
        for attribute in ['_data_1', '_data_2']:
            yield (attribute[1::], getattr(self, attribute))
