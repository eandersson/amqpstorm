"""Python 2/3 Compatibility layer"""
__author__ = 'eandersson'

from pamqp import PYTHON3


if PYTHON3:
    RANGE = range
else:
    RANGE = xrange


def is_string(object):
    """Is the object a valid string.

    :param object object:
    :rtype: bool
    """
    if PYTHON3:
        str_type = (bytes, str)
    else:
        str_type = (bytes, str, unicode)
    return isinstance(object, str_type)


def is_unicode(object):
    """Is this a unicode string.

        This always returns False if running on Python 3.

    :param object object:
    :rtype: bool
    """
    if PYTHON3:
        return False
    return isinstance(object, unicode)
