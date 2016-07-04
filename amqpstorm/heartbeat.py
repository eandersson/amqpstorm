"""AMQP-Storm Connection.Heartbeat."""

import logging
import threading

from amqpstorm.exception import AMQPConnectionError

LOGGER = logging.getLogger(__name__)


class Heartbeat(object):
    """AMQP Internal Heartbeat Checker"""

    def __init__(self, interval, send_heartbeat=None):
        self._lock = threading.Lock()
        self._running = threading.Event()
        self._timer = None
        self._exceptions = None
        self._reads_since_check = 0
        self._writes_since_check = 0
        self._interval = interval
        self._threshold = 0
        self.send_heartbeat = send_heartbeat

    def register_read(self):
        """Register that a frame has been received.

        :return:
        """
        self._reads_since_check += 1

    def register_write(self):
        """Register that a frame has been sent.

        :return:
        """
        self._writes_since_check += 1

    def start(self, exceptions):
        """Start the Heartbeat Checker.

        :param list exceptions:
        :return:
        """
        LOGGER.debug('Heartbeat Checker Started')
        with self._lock:
            self._running.set()
            self._threshold = 0
            self._reads_since_check = 0
            self._writes_since_check = 0
        self._exceptions = exceptions
        self._start_new_timer()

    def stop(self):
        """Stop the Heartbeat Checker.

        :return:
        """
        self._running.clear()
        if self._timer:
            self._timer.cancel()
        self._timer = None

    def _check_for_life_signs(self):
        """Check if we have any sign of life.

            First check if any data has been sent, if not send a heartbeat.

            If we have not received a heartbeat, or any data what so ever
            we should raise an exception so that we can close the connection.

            RabbitMQ may not necessarily send heartbeats if the connection
            is busy, so we only raise if no frame has been received.

        :return:
        """
        if not self._running.is_set():
            return False
        self._lock.acquire()
        try:
            if self._writes_since_check == 0:
                self.send_heartbeat()
            if self._reads_since_check == 0:
                self._threshold += 1
                if self._threshold >= 2:
                    self._running.set()
                    message = ('Connection dead, no heartbeat or data received'
                               ' in >= %ds' % (self._interval * 2))
                    why = AMQPConnectionError(message)
                    if self._exceptions is None:
                        raise why
                    self._exceptions.append(why)
                    return
            else:
                self._threshold = 0
        finally:
            self._reads_since_check = 0
            self._writes_since_check = 0
            self._lock.release()
        self._start_new_timer()
        return True

    def _start_new_timer(self):
        """Create a timer that will check for life signs on our connection.

        :return:
        """
        if not self._running.is_set():
            return
        self._timer = threading.Timer(interval=self._interval,
                                      function=self._check_for_life_signs)
        self._timer.daemon = True
        self._timer.start()
