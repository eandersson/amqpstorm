import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.base import Stateful
from amqpstorm.base import BaseChannel
from amqpstorm.exception import AMQPChannelError

logging.basicConfig(level=logging.DEBUG)


class BasicChannelTests(unittest.TestCase):
    def test_base_channel_id(self):
        channel = BaseChannel(100)

        self.assertEqual(channel.channel_id, 100)

    def test_base_channel_add_consumer_tag(self):
        channel = BaseChannel(0)
        channel.add_consumer_tag('travis-ci')

        self.assertEqual(channel.consumer_tags[0], 'travis-ci')

    def test_base_channel_add_consumer_tag_none_raises(self):
        channel = BaseChannel(0)
        self.assertRaisesRegexp(AMQPChannelError,
                                'consumer tag needs to be a string',
                                channel.add_consumer_tag, None)
        self.assertFalse(channel.consumer_tags)

    def test_base_channel_remove_empty_string(self):
        channel = BaseChannel(0)
        channel.add_consumer_tag('travis-ci')
        channel.add_consumer_tag('')

        channel.remove_consumer_tag('')

        self.assertTrue(len(channel.consumer_tags))

    def test_base_channel_add_duplicate_consumer_tags(self):
        channel = BaseChannel(0)

        channel.add_consumer_tag('travis-ci')
        channel.add_consumer_tag('travis-ci')

        self.assertEqual(len(channel.consumer_tags), 1)
        self.assertEqual(channel.consumer_tags[0], 'travis-ci')

    def test_base_channel_remove_single_consumer_tag(self):
        channel = BaseChannel(0)

        self.assertEqual(len(channel.consumer_tags), 0)

        channel.add_consumer_tag('travis-ci-1')
        channel.add_consumer_tag('travis-ci-2')

        self.assertEqual(len(channel.consumer_tags), 2)

        channel.remove_consumer_tag('travis-ci-1')

        self.assertEqual(len(channel.consumer_tags), 1)
        self.assertEqual(channel.consumer_tags[0], 'travis-ci-2')

    def test_base_channel_remove_all_consumer_tags(self):
        channel = BaseChannel(0)

        self.assertEqual(len(channel.consumer_tags), 0)

        for _ in range(100):
            channel.add_consumer_tag('travis-ci')
        channel.remove_consumer_tag(None)

        self.assertEqual(len(channel.consumer_tags), 0)


class StatefulTests(unittest.TestCase):
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
