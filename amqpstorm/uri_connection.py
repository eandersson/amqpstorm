"""AMQP-Storm Uri wrapper for Connection."""
__author__ = 'eandersson'

from amqpstorm.connection import Connection

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse


class UriConnection(Connection):
    """Wrapper of the Connection class that takes the AMQP uri schema."""

    def __init__(self, uri):
        """Create a new Connection instance using an AMQP Uri string.

            e.g.
                amqp://guest:guest@localhost:5672/%2F
                amqps://guest:guest@localhost:5671/%2F

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
        heartbeat = kwargs.get('heartbeat', [60])
        timeout = kwargs.get('timeout', [0])

        super(UriConnection, self).__init__(hostname, username,
                                            password, port,
                                            virtual_host=virtual_host,
                                            heartbeat=int(heartbeat[0]),
                                            timeout=int(timeout[0]),
                                            ssl=use_ssl)
