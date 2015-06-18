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
    SSL_VERSIONS = {
        'tlsv1': ssl.PROTOCOL_TLSv1,
        'sslv23': ssl.PROTOCOL_SSLv23,
        'sslv3': ssl.PROTOCOL_SSLv3
    }

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

    def __init__(self, uri):
        """Create a new Connection instance using an AMQP Uri string.

            e.g.
                amqp://guest:guest@localhost:5672/%2F?heartbeat=60
                amqps://guest:guest@localhost:5671/%2F?heartbeat=60

        :param str uri: AMQP Connection string
        """
        parsed = urlparse.urlparse(uri)
        use_ssl = parsed.scheme == 'amqps'
        hostname = parsed.hostname or 'localhost'
        port = parsed.port or 5672
        username = parsed.username or 'guest'
        password = parsed.password or 'guest'
        virtual_host = urlparse.unquote(parsed.path[1:]) or '/'
        kwargs = urlparse.parse_qs(parsed.query)
        heartbeat = kwargs.get('heartbeat', [60])[0]
        timeout = kwargs.get('timeout', [0])[0]

        ssl_options = {}
        if ssl and use_ssl:
            self._parse_ssl_options(kwargs, ssl_options)
        super(UriConnection, self).__init__(hostname, username,
                                            password, port,
                                            virtual_host=virtual_host,
                                            heartbeat=int(heartbeat),
                                            timeout=int(timeout),
                                            ssl=use_ssl,
                                            ssl_options=ssl_options)

    def _parse_ssl_options(self, options, ssl_options):
        """Parse SSL Options.

        :param options:
        :param ssl_options:
        :return:
        """
        for key in options:
            if key not in SSL_OPTIONS:
                continue
            if 'ssl_version' in key:
                value = self._get_ssl_version(options[key][0])
            elif 'cert_reqs' in key:
                value = self._get_ssl_validation(options[key][0])
            else:
                value = options[key][0]
            ssl_options[key] = value

    @staticmethod
    def _get_ssl_version(value):
        """Get the SSL Version.

        :param value:
        :return:
        """
        for version in SSL_VERSIONS:
            if version.endswith(value.lower()):
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
