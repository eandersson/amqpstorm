"""AMQP-Storm Uri wrapper for Connection."""
__author__ = 'eandersson'

import logging

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

try:
    import ssl
except ImportError:
    ssl = None

from amqpstorm.connection import Connection

LOGGER = logging.getLogger(__name__)

if ssl:
    SSL_VERSIONS = {}
    if hasattr(ssl, 'PROTOCOL_TLSv1_2'):
        SSL_VERSIONS['protocol_tlsv1_2'] = ssl.PROTOCOL_TLSv1_2
    if hasattr(ssl, 'PROTOCOL_TLSv1_1'):
        SSL_VERSIONS['protocol_tlsv1_1'] = ssl.PROTOCOL_TLSv1_1
    if hasattr(ssl, 'PROTOCOL_TLSv1'):
        SSL_VERSIONS['protocol_tlsv1'] = ssl.PROTOCOL_TLSv1
    if hasattr(ssl, 'PROTOCOL_SSLv3'):
        SSL_VERSIONS['protocol_sslv3'] = ssl.PROTOCOL_SSLv3

    SSL_CERT_MAP = {
        'cert_none': ssl.CERT_NONE,
        'cert_optional': ssl.CERT_OPTIONAL,
        'cert_required': ssl.CERT_REQUIRED
    }

    SSL_OPTIONS = [
        'keyfile',
        'certfile',
        'cert_reqs',
        'ssl_version',
        'ca_certs'
    ]


class UriConnection(Connection):
    """Wrapper of the Connection class that takes the AMQP uri schema."""

    def __init__(self, uri, lazy=False):
        """Create a new Connection instance using an AMQP Uri string.

            e.g.
                amqp://guest:guest@localhost:5672/%2F?heartbeat=60
                amqps://guest:guest@localhost:5671/%2F?heartbeat=60

        :param str uri: AMQP Connection string
        """
        uri = self._patch_uri(uri)
        parsed_uri = urlparse.urlparse(uri)
        use_ssl = parsed_uri.scheme == 'https'
        hostname = parsed_uri.hostname or 'localhost'
        port = parsed_uri.port or 5672
        username = parsed_uri.username or 'guest'
        password = parsed_uri.password or 'guest'
        kwargs = self._parse_uri_options(parsed_uri, use_ssl, lazy)
        super(UriConnection, self).__init__(hostname, username,
                                            password, port,
                                            **kwargs)

    @staticmethod
    def _patch_uri(uri):
        """If a custom uri schema is used with python 2.6 (e.g. amqps),
        it will ignore some of the parsing logic.

            As a work-around for this we change the amqp/amqps schema
            internally to use http/https.

        :param str uri: AMQP Connection string
        :rtype: str
        """
        index = uri.find(':')
        if uri[:index] == 'amqps':
            uri = uri.replace('amqps', 'https', 1)
        elif uri[:index] == 'amqp':
            uri = uri.replace('amqp', 'http', 1)
        return uri

    def _parse_uri_options(self, parsed_uri, use_ssl, lazy):
        """Parse the uri options.

        :param parsed_uri:
        :param bool use_ssl:
        :return:
        """
        kwargs = urlparse.parse_qs(parsed_uri.query)
        options = {
            'ssl': use_ssl,
            'virtual_host': urlparse.unquote(parsed_uri.path[1:]) or '/',
            'heartbeat': int(kwargs.get('heartbeat', [60])[0]),
            'timeout': int(kwargs.get('timeout', [30])[0]),
            'lazy': lazy
        }
        if ssl and use_ssl:
            options['ssl_options'] = self._parse_ssl_options(kwargs)
        return options

    def _parse_ssl_options(self, ssl_kwargs):
        """Parse SSL Options.

        :param ssl_kwargs:
        :rtype: dict
        """
        ssl_options = {}
        for key in ssl_kwargs:
            if key not in SSL_OPTIONS:
                continue
            if 'ssl_version' in key:
                value = self._get_ssl_version(ssl_kwargs[key][0])
            elif 'cert_reqs' in key:
                value = self._get_ssl_validation(ssl_kwargs[key][0])
            else:
                value = ssl_kwargs[key][0]
            ssl_options[key] = value
        return ssl_options

    @staticmethod
    def _get_ssl_version(value):
        """Get the SSL Version.

        :param value:
        :return:
        """
        for version in SSL_VERSIONS:
            if not version.endswith(value.lower()):
                continue
            return SSL_VERSIONS[version]
        LOGGER.warning('ssl_options: ssl_version \'%s\' not found '
                       'falling back to PROTOCOL_TLSv1.', value)
        return ssl.PROTOCOL_TLSv1

    @staticmethod
    def _get_ssl_validation(value):
        """Get the SSL Validation option.

        :param value:
        :return:
        """
        for cert in SSL_CERT_MAP:
            if not cert.endswith(value.lower()):
                continue
            return SSL_CERT_MAP[cert]
        LOGGER.warning('ssl_options: cert_reqs \'%s\' not found '
                       'falling back to CERT_NONE.', value)
        return ssl.CERT_NONE
