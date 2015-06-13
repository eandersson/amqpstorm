__author__ = 'eandersson'

from amqpstorm.base import Stateful


class FakeConnection(Stateful):
    frames_out = []
    parameters = {
        'heartbeat': 60
    }

    def __init__(self, state=3):
        super(FakeConnection, self).__init__()
        self.set_state(state)

    def check_for_errors(self):
        super(FakeConnection, self).check_for_errors()

    def write_frame(self, channel_id, frame_out):
        self.frames_out.append((channel_id, frame_out))


class TestPayload(object):
    __slots__ = ['name']

    def __iter__(self):
        for attribute in self.__slots__:
            yield (attribute, getattr(self, attribute))

    def __init__(self, name):
        self.name = name
