__author__ = 'eandersson'

import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.channel import Basic


logging.basicConfig(level=logging.DEBUG)


class BasicBasicTests(unittest.TestCase):
    def test_basic_return(self):
        basic = Basic(None)

        message = b'Hello World!'
        results = []
        for frame in basic._create_content_body(message):
            results.append(frame)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].value, message)

    def test_basic_return_long_string(self):
        basic = Basic(None)

        message = b'Hello World!' * 80960
        results = []
        for frame in basic._create_content_body(message):
            results.append(frame)

        self.assertEqual(len(results), 8)

        # Rebuild the string
        result_body = b''
        for frame in results:
            result_body += frame.value

        # Confirm that it matches the original string.
        self.assertEqual(result_body, message)
