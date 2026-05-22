"""Python 2/3 Compatibility layer."""
from __future__ import annotations

from typing import Any

try:
    import ssl
except ImportError:
    ssl = None  # type: ignore[assignment]

import urllib.parse as urlparse  # noqa
from urllib.parse import quote  # noqa


RANGE = range


class DummyException(Exception):
    """
    Never raised by anything.

    This is used in except blocks if the intended
    exception cannot be imported.
    """


SSL_CERT_MAP: dict[str, int] = {}
SSL_VERSIONS: dict[str, int] = {}
SSL_OPTIONS: list[str] = [
    'keyfile',
    'certfile',
    'cert_reqs',
    'ca_certs',
    'server_hostname',
    'check_hostname',
]


def get_default_ssl_version() -> int | None:
    """Get the highest supported TLS version, if none is available, return None.

    :rtype: int,None
    """
    if hasattr(ssl, 'PROTOCOL_TLSv1_2'):
        return ssl.PROTOCOL_TLSv1_2
    elif hasattr(ssl, 'PROTOCOL_TLSv1_1'):
        return ssl.PROTOCOL_TLSv1_1
    elif hasattr(ssl, 'PROTOCOL_TLSv1'):
        return ssl.PROTOCOL_TLSv1
    return None


DEFAULT_SSL_VERSION = get_default_ssl_version()
SSL_SUPPORTED = DEFAULT_SSL_VERSION is not None
if SSL_SUPPORTED:
    SSL_CERT_MAP = {
        'cert_none': ssl.CERT_NONE,
        'cert_optional': ssl.CERT_OPTIONAL,
        'cert_required': ssl.CERT_REQUIRED
    }
    SSLWantReadError: type[BaseException] = ssl.SSLWantReadError
else:
    SSLWantReadError = DummyException


def is_string(obj: Any) -> bool:
    """Is this a string.

    :param object obj:
    :rtype: bool
    """
    return isinstance(obj, (bytes, str))


def is_integer(obj: Any) -> bool:
    """Is this an integer.

    :param object obj:
    :return:
    """
    return isinstance(obj, int)


def try_utf8_decode(value: Any) -> Any:
    """Try to decode a bytes value to UTF-8.

        Returns the value unchanged if it is not bytes, is empty, or
        cannot be decoded as UTF-8.

    :param value:
    :return:
    """
    if not isinstance(value, bytes) or not value:
        return value
    try:
        return value.decode('utf-8')
    except UnicodeDecodeError:
        return value


def patch_uri(uri: str) -> str:
    """If a custom uri schema is used with python 2.6 (e.g. amqps),
    it will ignore some of the parsing logic.

        As a work-around for this we change the amqp/amqps schema
        internally to use http/https.

    :param str uri: AMQP Connection string
    :rtype: str
    """
    if uri.startswith('amqps:'):
        return 'https:' + uri.removeprefix('amqps:')
    if uri.startswith('amqp:'):
        return 'http:' + uri.removeprefix('amqp:')
    return uri
