import time

from amqpstorm.exception import AMQPConnectionError
from amqpstorm.heartbeat import Heartbeat
from amqpstorm.tests.utility import TestFramework
from amqpstorm.tests.utility import fake_function


class HeartbeatTests(TestFramework):
    def test_heartbeat_start(self):
        heartbeat = Heartbeat(60, fake_function)

        self.assertFalse(heartbeat._running.is_set())

        heartbeat.start([])

        self.assertTrue(heartbeat._running.is_set())

        self.assertIsNotNone(heartbeat._timer)
        heartbeat.stop()

    def test_heartbeat_disabled(self):
        heartbeat = Heartbeat(0, fake_function)

        self.assertFalse(heartbeat._running.is_set())

        heartbeat.start([])

        self.assertFalse(heartbeat._running.is_set())
        self.assertIsNone(heartbeat._timer)

    def test_heartbeat_stop(self):
        heartbeat = Heartbeat(60, fake_function)

        self.assertFalse(heartbeat._running.is_set())

        heartbeat.start([])

        self.assertTrue(heartbeat._running.is_set())

        heartbeat.stop()

        self.assertFalse(heartbeat._running.is_set())
        self.assertIsNone(heartbeat._timer)

    def test_heartbeat_interval(self):
        heartbeat = Heartbeat(60, fake_function)

        self.assertEqual(heartbeat._interval, 60)
        self.assertEqual(heartbeat._threshold, 0)

        heartbeat = Heartbeat(360, fake_function)

        self.assertEqual(heartbeat._interval, 360)
        self.assertEqual(heartbeat._threshold, 0)

    def test_heartbeat_no_interval(self):
        heartbeat = Heartbeat(0, fake_function)

        self.assertEqual(heartbeat._interval, 0)
        self.assertEqual(heartbeat._threshold, 0)
        self.assertFalse(heartbeat.start([]))

        heartbeat = Heartbeat(None, fake_function)

        self.assertEqual(heartbeat._interval, None)
        self.assertEqual(heartbeat._threshold, 0)
        self.assertFalse(heartbeat.start([]))

    def test_heartbeat_register_reads(self):
        heartbeat = Heartbeat(60, fake_function)
        time.sleep(0.01)
        for _ in range(32):
            heartbeat.register_read()

        self.assertEqual(heartbeat._reads_since_check, 32)

    def test_heartbeat_register_writes(self):
        heartbeat = Heartbeat(60, fake_function)
        time.sleep(0.01)
        for _ in range(32):
            heartbeat.register_write()

        self.assertEqual(heartbeat._writes_since_check, 32)

    def test_heartbeat_do_not_execute_life_signs_when_stopped(self):
        heartbeat = Heartbeat(60, fake_function)

        self.assertFalse(heartbeat._check_for_life_signs())

    def test_heartbeat_do_not_start_new_timer_when_stopped(self):
        heartbeat = Heartbeat(60, fake_function)

        self.assertIsNone(heartbeat._timer)

        heartbeat._start_new_timer()

        self.assertIsNone(heartbeat._timer)

    def test_heartbeat_basic_raise_on_missed_heartbeats(self):
        heartbeat = Heartbeat(0.01, fake_function)
        exceptions = []

        self.assertTrue(heartbeat.start(exceptions))

        time.sleep(0.1)

        self.assertGreater(len(heartbeat._exceptions), 0)

        heartbeat.stop()

    def test_heartbeat_send_heartbeat(self):
        self.beats = 0

        def send_heartbeat():
            self.beats += 1

        heartbeat = Heartbeat(60, send_heartbeat_impl=send_heartbeat)
        heartbeat._running.set()

        for _ in range(8):
            heartbeat.register_read()
            self.assertTrue(heartbeat._check_for_life_signs())

        self.assertEqual(self.beats, 8)

        heartbeat.stop()

    def test_heartbeat_threshold_reset(self):
        heartbeat = Heartbeat(60, fake_function)
        heartbeat._running.set()
        heartbeat.register_write()

        self.assertEqual(heartbeat._threshold, 0)
        self.assertTrue(heartbeat._check_for_life_signs())
        self.assertEqual(heartbeat._threshold, 1)

        heartbeat.register_write()
        heartbeat.register_read()

        self.assertTrue(heartbeat._check_for_life_signs())
        self.assertEqual(heartbeat._threshold, 0)

    def test_heartbeat_raise_after_threshold(self):
        heartbeat = Heartbeat(60, fake_function)
        exceptions = []

        self.assertTrue(heartbeat.start(exceptions))

        heartbeat.register_write()

        self.assertTrue(heartbeat._check_for_life_signs())

        heartbeat.register_write()

        self.assertFalse(heartbeat._check_for_life_signs())
        self.assertTrue(exceptions)

        heartbeat.stop()

    def test_heartbeat_running_cleared_after_raise(self):
        heartbeat = Heartbeat(60, fake_function)
        exceptions = []

        self.assertTrue(heartbeat.start(exceptions))
        self.assertTrue(heartbeat._running.is_set())

        heartbeat.register_write()
        heartbeat._check_for_life_signs()
        heartbeat.register_write()
        heartbeat._check_for_life_signs()

        self.assertFalse(heartbeat._running.is_set())

        heartbeat.stop()

    def test_heartbeat_raise_when_check_for_life_when_exceptions_not_set(self):
        heartbeat = Heartbeat(60, fake_function)
        heartbeat._running.set()
        heartbeat._reads_since_check = 0
        heartbeat._threshold = 3
        heartbeat.register_write()

        # Normally the exception should be passed down to the list of
        # exceptions in the connection, but if that list for some obscure
        # reason is None, we should raise directly in _check_for_life_signs.
        self.assertRaises(AMQPConnectionError, heartbeat._check_for_life_signs)

    def test_heartbeat_extended_loop(self):
        self.beats = 0

        def send_heartbeat():
            self.beats += 1

        heartbeat = Heartbeat(60, send_heartbeat_impl=send_heartbeat)
        heartbeat._running.set()
        heartbeat.register_read()

        # Miss one write/
        self.assertTrue(heartbeat._check_for_life_signs())

        for _ in range(32):
            heartbeat.register_read()
            heartbeat.register_write()

            self.assertFalse(heartbeat._threshold)
            self.assertTrue(heartbeat._check_for_life_signs())

            time.sleep(0.01)

        heartbeat.register_write()

        # Miss one read.
        self.assertTrue(heartbeat._check_for_life_signs())

        self.assertEqual(heartbeat._threshold, 1)
        self.assertEqual(self.beats, 1)

        heartbeat.register_read()
        heartbeat.register_write()
        self.assertTrue(heartbeat._check_for_life_signs())

        self.assertEqual(heartbeat._threshold, 0)

        heartbeat.stop()

    def test_heartbeat_raise_exception(self):
        heartbeat = Heartbeat(60, None)

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection dead, no heartbeat or data received in >= 120s',
            heartbeat._raise_or_append_exception
        )

        heartbeat = Heartbeat(120, None)

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection dead, no heartbeat or data received in >= 240',
            heartbeat._raise_or_append_exception
        )

    def test_heartbeat_append_exception(self):
        heartbeat = Heartbeat(60, None)
        heartbeat._exceptions = []

        def check(exception):
            heartbeat._raise_or_append_exception()
            if exception:
                raise exception.pop()

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection dead, no heartbeat or data received in >= 120s',
            check, heartbeat._exceptions
        )

        heartbeat._interval = 120

        self.assertRaisesRegexp(
            AMQPConnectionError,
            'Connection dead, no heartbeat or data received in >= 240',
            check, heartbeat._exceptions
        )
