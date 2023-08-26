# -*- coding: utf-8 -*-
import importlib
import ssl
import sys

from amqpstorm import compatibility
from amqpstorm.tests.utility import SslTLSNone
from amqpstorm.tests.utility import SslTLSv1
from amqpstorm.tests.utility import SslTLSv1_1
from amqpstorm.tests.utility import SslTLSv1_2
from amqpstorm.tests.utility import TestFramework
from amqpstorm.tests.utility import unittest


class CompatibilityTests(TestFramework):
    def test_compatibility_basic_integer(self):
        x = 100
        self.assertTrue(compatibility.is_integer(x))

    def test_compatibility_not_integer(self):
        x = ''
        self.assertFalse(compatibility.is_integer(x))

    def test_compatibility_normal_string(self):
        x = ''
        self.assertTrue(compatibility.is_string(x))

    def test_compatibility_byte_string(self):
        x = b''
        self.assertTrue(compatibility.is_string(x))

    def test_compatibility_is_not_string(self):
        x = 100
        self.assertFalse(compatibility.is_string(x))

    def test_compatibility_fail_silently_on_utf_16(self):
        x = 'hello'.encode('utf-16')
        self.assertEqual(compatibility.try_utf8_decode(x), x)

    def test_compatibility_fail_silently_on_utf_32(self):
        x = 'hello'.encode('utf-32')
        self.assertEqual(compatibility.try_utf8_decode(x), x)

    def test_compatibility_try_utf8_decode_on_integer(self):
        x = 100
        self.assertEqual(x, compatibility.try_utf8_decode(x))

    def test_compatibility_try_utf8_decode_on_dict(self):
        x = dict(hello='world')
        self.assertEqual(x, compatibility.try_utf8_decode(x))

    def test_compatibility_python_range(self):
        self.assertEqual(compatibility.RANGE, range)

    def test_compatibility_ssl_is_set(self):
        self.assertIsNotNone(compatibility.ssl)

    def test_compatibility_urlparse_is_set(self):
        self.assertIsNotNone(compatibility.urlparse)

    def test_compatibility_range_is_set(self):
        self.assertIsNotNone(compatibility.RANGE)

    def test_compatibility_patch_uri(self):
        self.assertEqual(compatibility.patch_uri('amqps://'), 'https://')
        self.assertEqual(compatibility.patch_uri('amqp://'), 'http://')
        self.assertEqual(compatibility.patch_uri('travis://'), 'travis://')


class CompatibilitySslTests(unittest.TestCase):
    @unittest.skipIf('ssl' not in sys.modules, 'Python not compiled '
                                               'with SSL support')
    def test_compatibility_default_ssl_version(self):
        self.assertTrue(compatibility.SSL_SUPPORTED)
        if hasattr(ssl, 'PROTOCOL_TLSv1_2'):
            self.assertEqual(compatibility.DEFAULT_SSL_VERSION,
                             ssl.PROTOCOL_TLSv1_2)
        else:
            self.assertEqual(compatibility.DEFAULT_SSL_VERSION,
                             ssl.PROTOCOL_TLSv1)

    def test_compatibility_default_ssl_none(self):
        restore_func = compatibility.ssl
        try:
            compatibility.ssl = None
            self.assertIsNone(compatibility.get_default_ssl_version())
        finally:
            compatibility.ssl = restore_func

    def test_compatibility_default_tls_1_2(self):
        restore_func = compatibility.ssl
        try:
            compatibility.ssl = SslTLSv1_2
            self.assertEqual(compatibility.get_default_ssl_version(), 5)
        finally:
            compatibility.ssl = restore_func

    def test_compatibility_default_tls_1_1(self):
        restore_func = compatibility.ssl
        try:
            compatibility.ssl = SslTLSv1_1
            self.assertEqual(compatibility.get_default_ssl_version(), 4)
        finally:
            compatibility.ssl = restore_func

    def test_compatibility_default_tls_1(self):
        restore_func = compatibility.ssl
        try:
            compatibility.ssl = SslTLSv1
            self.assertEqual(compatibility.get_default_ssl_version(), 3)
        finally:
            compatibility.ssl = restore_func

    def test_compatibility_default_tls_not_available(self):
        restore_func = compatibility.ssl
        try:
            compatibility.ssl = SslTLSNone
            self.assertIsNone(compatibility.get_default_ssl_version())
        finally:
            compatibility.ssl = restore_func

    def test_compatibility_ssl_not_defined(self):
        """This tests mimics the behavior of Python built locally without
        SSL support.
        """
        restore_func = sys.modules['ssl']
        try:
            sys.modules['ssl'] = None
            importlib.reload(compatibility)
            self.assertIsNone(compatibility.ssl)
            self.assertIsNone(compatibility.DEFAULT_SSL_VERSION)
            self.assertFalse(compatibility.SSL_SUPPORTED)
            self.assertFalse(compatibility.SSL_CERT_MAP)
            self.assertFalse(compatibility.SSL_VERSIONS)
        finally:
            sys.modules['ssl'] = restore_func
            importlib.reload(compatibility)

    def test_compatibility_no_supported_ssl_version(self):
        """This tests mimics the behavior of a Python build without
        support for TLS v1, v1_1 or v1_2.
        """
        restore_tls_v1_2 = sys.modules['ssl'].PROTOCOL_TLSv1_2
        restore_tls_v1_1 = sys.modules['ssl'].PROTOCOL_TLSv1_1
        restore_tls_v1 = sys.modules['ssl'].PROTOCOL_TLSv1
        try:
            del sys.modules['ssl'].PROTOCOL_TLSv1_2
            del sys.modules['ssl'].PROTOCOL_TLSv1_1
            del sys.modules['ssl'].PROTOCOL_TLSv1
            importlib.reload(compatibility)
            self.assertIsNone(compatibility.DEFAULT_SSL_VERSION)
            self.assertFalse(compatibility.SSL_SUPPORTED)
            self.assertFalse(compatibility.SSL_CERT_MAP)
            self.assertFalse(compatibility.SSL_VERSIONS)
        finally:
            sys.modules['ssl'].PROTOCOL_TLSv1_2 = restore_tls_v1_2
            sys.modules['ssl'].PROTOCOL_TLSv1_1 = restore_tls_v1_1
            sys.modules['ssl'].PROTOCOL_TLSv1 = restore_tls_v1
            importlib.reload(compatibility)

    def test_compatibility_only_tls_v1_supported(self):
        """This test mimics the behavior of earlier versions of Python that
        only supported TLS v1 and SSLv23.
        """
        restore_tls_v1_2 = sys.modules['ssl'].PROTOCOL_TLSv1_2
        restore_tls_v1 = sys.modules['ssl'].PROTOCOL_TLSv1_1
        try:
            del sys.modules['ssl'].PROTOCOL_TLSv1_2
            del sys.modules['ssl'].PROTOCOL_TLSv1_1
            importlib.reload(compatibility)
            self.assertEqual(compatibility.get_default_ssl_version(),
                             ssl.PROTOCOL_TLSv1)
        finally:
            sys.modules['ssl'].PROTOCOL_TLSv1_2 = restore_tls_v1_2
            sys.modules['ssl'].PROTOCOL_TLSv1_1 = restore_tls_v1
            importlib.reload(compatibility)
