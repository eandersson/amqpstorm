__author__ = 'eandersson'

import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.base import BaseChannel


logging.basicConfig(level=logging.DEBUG)


class BasicChannelTests(unittest.TestCase):
    def test_channel_id(self):
        channel = BaseChannel(1337)
        self.assertEqual(channel.channel_id, 1337)

    def test_add_consumer_tag(self):
        channel = BaseChannel(0)
        channel.add_consumer_tag('my_tag')
        self.assertEqual(channel.consumer_tags[0], 'my_tag')

    def test_remove_single_consumer_tag(self):
        channel = BaseChannel(0)
        channel.add_consumer_tag('1')
        channel.add_consumer_tag('2')
        channel.remove_consumer_tag('1')
        self.assertEqual(len(channel.consumer_tags), 1)
        self.assertEqual(channel.consumer_tags[0], '2')

    def test_remove_all_consumer_tags(self):
        channel = BaseChannel(0)
        channel.add_consumer_tag('my_tag')
        channel.add_consumer_tag('my_tag')
        channel.add_consumer_tag('my_tag')
        channel.remove_consumer_tag()
        self.assertEqual(len(channel.consumer_tags), 0)
