"""AMQPStorm Connection.Heartbeat."""
from __future__ import annotations

import logging
import threading
from typing import Callable

from amqpstorm.exception import AMQPConnectionError

LOGGER = logging.getLogger(__name__)


class Heartbeat:
    """Internal Heartbeat handler."""

    def __init__(
        self,
        timeout: float | None,
        send_heartbeat_impl: Callable[[], None],
        timer: Callable[..., threading.Timer] = threading.Timer,
    ) -> None:
        self.send_heartbeat_impl = send_heartbeat_impl
        self.timer_impl = timer
        self._lock = threading.Lock()
        self._running = threading.Event()
        self._timer: threading.Timer | None = None
        self._exceptions: list[Exception] | None = None
        self._reads_since_check = 0
        self._writes_since_check = 0
        self._interval: float | None = (
            None if timeout is None else max(timeout / 2, 0)
        )
        self._threshold = 0

    def register_read(self) -> None:
        """Register that a frame has been received.

        :return:
        """
        self._reads_since_check += 1

    def register_write(self) -> None:
        """Register that a frame has been sent.

        :return:
        """
        self._writes_since_check += 1

    def start(self, exceptions: list[Exception]) -> bool:
        """Start the Heartbeat Checker.

        :param list exceptions:
        :return:
        """
        if not self._interval:
            return False
        self._running.set()
        with self._lock:
            self._threshold = 0
            self._reads_since_check = 0
            self._writes_since_check = 0
        self._exceptions = exceptions
        LOGGER.debug('Heartbeat Checker Started')
        return self._start_new_timer()

    def stop(self) -> None:
        """Stop the Heartbeat Checker.

        :return:
        """
        self._running.clear()
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self._timer = None

    def _check_for_life_signs(self) -> bool:
        """Check Connection for life signs.

            First check if any data has been sent, if not send a heartbeat
            to the remote server.

            If we have not received any data what so ever within two
            intervals, we need to raise an exception so that we can
            close the connection.

        :rtype: bool
        """
        if not self._running.is_set():
            return False
        if self._writes_since_check == 0:
            self.send_heartbeat_impl()
        with self._lock:
            try:
                if self._reads_since_check == 0:
                    self._threshold += 1
                    if self._threshold >= 2:
                        self._running.clear()
                        self._raise_or_append_exception()
                        return False
                else:
                    self._threshold = 0
            finally:
                self._reads_since_check = 0
                self._writes_since_check = 0

        return self._start_new_timer()

    def _raise_or_append_exception(self) -> None:
        """The connection is presumably dead and we need to raise or
        append an exception.

            If we have a list for exceptions, append the exception and let
            the connection handle it, if not raise the exception here.

        :return:
        """
        interval = self._interval or 0
        message = (
            'Connection dead, no heartbeat or data received in >= '
            f'{int(interval * 2)}s'
        )
        why = AMQPConnectionError(message)
        if self._exceptions is None:
            raise why
        self._exceptions.append(why)

    def _start_new_timer(self) -> bool:
        """Create a timer that will be used to periodically check the
        connection for heartbeats.

        :return:
        """
        if not self._running.is_set():
            return False
        self._timer = self.timer_impl(
            interval=self._interval,
            function=self._check_for_life_signs
        )
        self._timer.daemon = True
        self._timer.start()
        return True
