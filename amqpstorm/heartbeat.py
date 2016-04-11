"""AMQP-Storm Connection.Heartbeat."""

import logging
import threading
import time

from amqpstorm.exception import AMQPConnectionError

LOGGER = logging.getLogger(__name__)


class Heartbeat(object):
    """AMQP Internal Heartbeat Checker"""

    def __init__(self, interval):
        if interval < 1:
            interval = 1
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._timer = None
        self._exceptions = None
        self._last_heartbeat = 0.0
        self._beats_since_check = 0
        self._interval = interval + 1
        self._threshold = (interval + 1) * 2

    def register_beat(self):
        """Register that a frame has been received.

        :return:
        """
        with self._lock:
            self._beats_since_check += 1

    def register_heartbeat(self):
        """Register a Heartbeat.

        :return:
        """
        with self._lock:
            self._last_heartbeat = time.time()

    def start(self, exceptions):
        """Start the Heartbeat Checker.

        :param list exceptions:
        :return:
        """
        LOGGER.debug('Heartbeat Checker Started')
        with self._lock:
            self._stopped = threading.Event()
            self._beats_since_check = 0
            self._last_heartbeat = time.time()
        self._exceptions = exceptions
        self._start_new_timer()

    def stop(self):
        """Stop the Heartbeat Checker.

        :return:
        """
        self._stopped.set()
        if self._timer:
            self._timer.cancel()
        self._timer = None

    def _check_for_life_signs(self):
        """Check if we have any sign of life.

            If we have not received a heartbeat, or any data what so ever
            we should raise an exception so that we can close the connection.

            RabbitMQ may not necessarily send heartbeats if the connection
            is busy, so we only raise if no frame has been received.

        :return:
        """
        if self._stopped.is_set():
            return False
        self._lock.acquire()
        try:
            elapsed = time.time() - self._last_heartbeat
            if self._beats_since_check == 0 and elapsed > self._threshold:
                self._stopped.set()
                message = ('Connection dead, no heartbeat or data received '
                           'in %ds' % round(elapsed, 3))
                why = AMQPConnectionError(message)
                if self._exceptions is None:
                    raise why
                self._exceptions.append(why)
            self._beats_since_check = 0
        finally:
            self._lock.release()
        self._start_new_timer()
        return True

    def _start_new_timer(self):
        """Create a timer that will check for life signs on our connection.

        :return:
        """
        if self._stopped.is_set():
            return
        self._timer = threading.Timer(interval=self._interval,
                                      function=self._check_for_life_signs)
        self._timer.daemon = True
        self._timer.start()
