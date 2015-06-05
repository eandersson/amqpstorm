__author__ = 'eandersson'

import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.base import Rpc
from amqpstorm.base import Stateful

logging.basicConfig(level=logging.DEBUG)


class FakeConnection(Stateful):
    def __init__(self, state=3):
        super(FakeConnection, self).__init__()
        self.set_state(state)

    def check_for_errors(self):
        pass


class TestPayload(object):
    __slots__ = ['name']

    def __iter__(self):
        for attribute in self.__slots__:
            yield (attribute, getattr(self, attribute))

    def __init__(self, name):
        self.name = name


class BasicChannelTests(unittest.TestCase):
    def test_register_request(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['Test'])
        self.assertEqual(len(rpc.request), 1)
        for key in rpc.request:
            self.assertEqual(uuid, rpc.request[key])

    def test_get_request(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['Test'])
        self.assertTrue(rpc.on_frame(TestPayload(name='Test')))
        self.assertIsInstance(rpc.get_request(uuid=uuid, raw=True),
                              TestPayload)

    def test_remove(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['Test'])
        self.assertEqual(len(rpc.request), 1)
        self.assertEqual(len(rpc.response), 1)
        rpc.on_frame(TestPayload(name='Test'))
        rpc.remove(uuid)
        self.assertEqual(len(rpc.request), 0)
        self.assertEqual(len(rpc.response), 0)

    def test_remove_request(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['Test'])
        self.assertEqual(len(rpc.request), 1)
        rpc.remove_request(uuid)
        self.assertEqual(len(rpc.request), 0)

    def test_remove_response(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['Test'])
        self.assertEqual(len(rpc.response), 1)
        rpc.remove_response(uuid)
        self.assertEqual(len(rpc.response), 0)

    def test_on_frame(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['Test'])
        self.assertEqual(rpc.response[uuid], None)
        rpc.on_frame(TestPayload(name='Test'))
        self.assertIsInstance(rpc.response[uuid], TestPayload)
