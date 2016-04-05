__author__ = 'eandersson'

from amqpstorm.base import Stateful


class FakeConnection(Stateful):
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

    def check_for_errors(self):
        super(FakeConnection, self).check_for_errors()

    def write_frame(self, channel_id, frame_out):
        self.frames_out.append((channel_id, frame_out))

    def write_frames(self, channel_id, frames_out):
        self.frames_out.append((channel_id, frames_out))


class TestPayload(object):
    __slots__ = ['name']

    def __iter__(self):
        for attribute in self.__slots__:
            yield (attribute, getattr(self, attribute))

    def __init__(self, name):
        self.name = name


class FakeFrame(object):
    fake = 'fake'
    data = 'data'

    def __init__(self):
        self.name = 'FakeFrame'

    def __iter__(self):
        for attribute in ['fake', 'data']:
            yield (attribute[1::], getattr(self, attribute))
