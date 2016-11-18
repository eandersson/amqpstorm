from amqpstorm.base import Stateful

from amqpstorm.tests.utility import TestFramework


class StatefulTests(TestFramework):
    def test_stateful_default_is_closed(self):
        stateful = Stateful()

        self.assertTrue(stateful.is_closed)

    def test_stateful_set_open(self):
        stateful = Stateful()
        stateful.set_state(Stateful.OPEN)

        self.assertTrue(stateful.is_open)

    def test_stateful_set_opening(self):
        stateful = Stateful()
        stateful.set_state(Stateful.OPENING)

        self.assertTrue(stateful.is_opening)

    def test_stateful_set_closed(self):
        stateful = Stateful()
        stateful.set_state(Stateful.CLOSED)

        self.assertTrue(stateful.is_closed)

    def test_stateful_set_closing(self):
        stateful = Stateful()
        stateful.set_state(Stateful.CLOSING)

        self.assertTrue(stateful.is_closing)

    def test_stateful_get_current_state(self):
        stateful = Stateful()

        stateful.set_state(Stateful.CLOSED)
        self.assertEqual(stateful.current_state, Stateful.CLOSED)

        stateful.set_state(Stateful.CLOSING)
        self.assertEqual(stateful.current_state, Stateful.CLOSING)

        stateful.set_state(Stateful.OPEN)
        self.assertEqual(stateful.current_state, Stateful.OPEN)

        stateful.set_state(Stateful.OPENING)
        self.assertEqual(stateful.current_state, Stateful.OPENING)
