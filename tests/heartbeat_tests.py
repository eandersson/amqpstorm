__author__ = 'eandersson'

import time
import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.heartbeat import Heartbeat

logging.basicConfig(level=logging.DEBUG)


class HeartbeatTests(unittest.TestCase):

    def test_heartbeat_start(self):
        heartbeat = Heartbeat([], 1)
        heartbeat.start()
        self.assertIsNotNone(heartbeat._timer)
        heartbeat.stop()

    def test_heartbeat_stop(self):
        heartbeat = Heartbeat([], 1)
        heartbeat.start()
        heartbeat.stop()
        self.assertIsNone(heartbeat._timer)

    def test_register_beat(self):
        heartbeat = Heartbeat([], 1)
        self.assertEqual(heartbeat._beats_since_check, 0)
        heartbeat.register_beat()
        self.assertEqual(heartbeat._beats_since_check, 1)

    def test_register_heartbeat(self):
        heartbeat = Heartbeat([], 1)
        last_heartbeat = heartbeat._last_heartbeat
        time.sleep(0.01)
        heartbeat.register_heartbeat()
        self.assertNotEqual(heartbeat._last_heartbeat, last_heartbeat)

    def test_basic_raise_on_missed_heartbeats(self):
        exceptions = []
        heartbeat = Heartbeat(exceptions, 1)
        heartbeat.start()
        time.sleep(3)
        self.assertGreater(len(exceptions), 0)
