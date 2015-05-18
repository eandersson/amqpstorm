"""AMQP-Storm IO."""
__author__ = 'eandersson'

import select

from errno import EINTR


class Poller(object):
    """Socket Read/Write Poller."""

    def __init__(self, fileno, timeout=10):
        self._fileno = fileno
        self.timeout = timeout

    @property
    def fileno(self):
        """Socket Fileno.

        :return:
        """
        return self._fileno

    @property
    def is_ready(self):
        """Is Socket Ready.

        :rtype: tuple
        """
        try:
            ready, write, _ = select.select([self.fileno], [self.fileno], [],
                                            self.timeout)
            return bool(ready), bool(write)
        except select.error as why:
            if why.args[0] != EINTR:
                raise
