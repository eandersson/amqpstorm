__author__ = 'eandersson'

import ssl
import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm import Connection
from amqpstorm.exception import *


logging.basicConfig(level=logging.DEBUG)


class ConnectionTests(unittest.TestCase):
    def test_set_hostname(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection.parameters['username'], 'guest')

    def test_set_username(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection.parameters['username'], 'guest')

    def test_set_password(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertEqual(connection.parameters['username'], 'guest')

    def test_set_parameters(self):
        connection = Connection('127.0.0.1', 'guest', 'guest',
                                virtual_host='travis',
                                heartbeat=120,
                                timeout=180,
                                ssl=True,
                                ssl_options={
                                    'ssl_version': ssl.PROTOCOL_TLSv1
                                },
                                lazy=True)
        self.assertEqual(connection.parameters['virtual_host'], 'travis')
        self.assertEqual(connection.parameters['heartbeat'], 120)
        self.assertEqual(connection.parameters['timeout'], 180)
        self.assertEqual(connection.parameters['ssl'], True)
        self.assertEqual(connection.parameters['ssl_options']['ssl_version'],
                         ssl.PROTOCOL_TLSv1)

    def test_invalid_hostname(self):
        self.assertRaises(AMQPInvalidArgument, Connection, 1,
                          'guest', 'guest', lazy=True)

    def test_invalid_username(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          2, 'guest', lazy=True)

    def test_invalid_password(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 3, lazy=True)

    def test_invalid_virtual_host(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', virtual_host=4, lazy=True)

    def test_invalid_heartbeat(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', heartbeat='5', lazy=True)

    def test_invalid_timeout(self):
        self.assertRaises(AMQPInvalidArgument, Connection, '127.0.0.1',
                          'guest', 'guest', timeout='6', lazy=True)

    def test_close_state(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        connection.set_state(Connection.OPEN)
        connection.close()
        self.assertTrue(connection.is_closed)

    def test_open_channel_on_closed_connection(self):
        connection = Connection('127.0.0.1', 'guest', 'guest', lazy=True)
        self.assertRaises(AMQPConnectionError, connection.channel)
