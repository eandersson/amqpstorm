__author__ = 'eandersson'

import sys
import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm import compatibility


logging.basicConfig(level=logging.DEBUG)


class CompatibilityTests(unittest.TestCase):

    def test_basic_integer(self):
        x = 0
        self.assertTrue(compatibility.is_integer(x))

    @unittest.skipIf(sys.version_info[0] == 3, 'No long obj in Python 3')
    def test_long_integer(self):
        x = long(1)
        self.assertTrue(compatibility.is_integer(x))

    def test_normal_string(self):
        x = ''
        self.assertTrue(compatibility.is_string(x))

    def test_byte_string(self):
        x = b''
        self.assertTrue(compatibility.is_string(x))

    @unittest.skipIf(sys.version_info[0] == 3, 'No unicode obj in Python 3')
    def test_unicode_string(self):
        x = unicode('')
        self.assertTrue(compatibility.is_string(x))

    def test_is_not_string(self):
        x = 0
        self.assertFalse(compatibility.is_string(x))

    @unittest.skipIf(sys.version_info[0] == 3, 'No unicode obj in Python 3')
    def test_is_unicode(self):
        x = unicode('')
        self.assertTrue(compatibility.is_unicode(x))

    def test_is_not_unicode(self):
        x = ''
        self.assertFalse(compatibility.is_unicode(x))