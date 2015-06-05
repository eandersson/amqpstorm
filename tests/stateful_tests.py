__author__ = 'eandersson'

import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.base import Stateful


logging.basicConfig(level=logging.DEBUG)


class MessageTests(unittest.TestCase):
    def test_default_is_closed(self):
        stateful = Stateful()
        self.assertTrue(stateful.is_closed)

    def test_set_open(self):
        stateful = Stateful()
        stateful.set_state(Stateful.OPEN)
        self.assertTrue(stateful.is_open)

    def test_set_opening(self):
        stateful = Stateful()
        stateful.set_state(Stateful.OPENING)
        self.assertTrue(stateful.is_opening)

    def test_set_closed(self):
        stateful = Stateful()
        stateful.set_state(Stateful.CLOSED)
        self.assertTrue(stateful.is_closed)

    def test_set_closing(self):
        stateful = Stateful()
        stateful.set_state(Stateful.CLOSING)
        self.assertTrue(stateful.is_closing)

    def test_exception_handling(self):
        stateful = Stateful()
        stateful.set_state(Stateful.OPEN)
        stateful.exceptions.append(Exception('Test'))
        self.assertTrue(stateful.is_open)
        self.assertRaises(Exception, stateful.check_for_errors)
        self.assertTrue(stateful.is_closed)

    def test_multiple_exceptions(self):
        stateful = Stateful()
        stateful.set_state(Stateful.OPEN)
        stateful.exceptions.append(IOError('Test'))
        stateful.exceptions.append(Exception('Test'))
        self.assertTrue(stateful.is_open)
        self.assertRaises(IOError, stateful.check_for_errors)
        self.assertRaises(IOError, stateful.check_for_errors)
        self.assertTrue(stateful.is_closed)
