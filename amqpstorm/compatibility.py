"""Python 2/3 Compatibility layer."""
__author__ = 'eandersson'

import sys


PYTHON3 = sys.version_info >= (3, 0, 0)

try:
    import __pypy__
    PYPY = True
except ImportError:
    PYPY = False

if PYTHON3:
    RANGE = range
else:
    RANGE = xrange


def is_string(obj):
    """Is this a string.

    :param object obj:
    :rtype: bool
    """
    if PYTHON3:
        str_type = (bytes, str)
    else:
        str_type = (bytes, str, unicode)
    return isinstance(obj, str_type)


def is_integer(obj):
    if PYTHON3:
        return isinstance(obj, int)
    return isinstance(obj, (int, long))


def is_unicode(obj):
    """Is this a unicode string.

        This always returns False if running on Python 3.

    :param object obj:
    :rtype: bool
    """
    if PYTHON3:
        return False
    return isinstance(obj, unicode)


def try_utf8_decode(value):
    if not is_string(value):
        return value

    if PYTHON3 and not isinstance(value, bytes):
        return value
    if not PYTHON3 and not is_unicode(value):
        return value

    try:
        return value.decode('utf-8')
    except (UnicodeEncodeError, AttributeError):
        pass

    return value
