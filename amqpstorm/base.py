"""AMQP-Storm Base."""

import threading
import time
from uuid import uuid4

from amqpstorm.exception import AMQPChannelError

IDLE_WAIT = 0.01
FRAME_MAX = 131072


class Stateful(object):
    """Stateful"""
    CLOSED = 0
    CLOSING = 1
    OPENING = 2
    OPEN = 3

    def __init__(self):
        self._lock = threading.Lock()
        self._state = self.CLOSED
        self._exceptions = []

    @property
    def lock(self):
        return self._lock

    def set_state(self, state):
        """Set State.

        :param int state:
        :return:
        """
        self._state = state

    @property
    def is_closed(self):
        """Is Closed?

        :rtype: bool
        """
        return self._state == self.CLOSED

    @property
    def is_closing(self):
        """Is Closing?

        :rtype: bool
        """
        return self._state == self.CLOSING

    @property
    def is_opening(self):
        """Is Opening?

        :rtype: bool
        """
        return self._state == self.OPENING

    @property
    def is_open(self):
        """Is Open?

        :rtype: bool
        """
        return self._state == self.OPEN

    @property
    def exceptions(self):
        """Any exceptions thrown. This is useful for troubleshooting, and is
        used internally to check the health of the connection.

        :rtype: list
        """
        return self._exceptions

    def check_for_errors(self):
        """Check for critical errors.

        :return:
        """
        pass


class Rpc(object):
    """AMQP Channel.rpc"""

    def __init__(self, adapter, timeout=360):
        """
        :param Stateful adapter: Connection or Channel.
        :param int|float timeout: Rpc timeout.
        """
        self._lock = threading.Lock()
        self._adapter = adapter
        self._timeout = timeout
        self._response = {}
        self._request = {}

    @property
    def lock(self):
        return self._lock

    def on_frame(self, frame_in):
        """On RPC Frame.

        :param pamqp_spec.Frame frame_in: Amqp frame.
        :return:
        """
        if frame_in.name not in self._request:
            return False

        uuid = self._request[frame_in.name]
        if self._response[uuid]:
            self._response[uuid].append(frame_in)
        else:
            self._response[uuid] = [frame_in]
        return True

    def register_request(self, valid_responses):
        """Register a RPC request.

        :param list valid_responses: List of possible Responses that
                                     we should be waiting for.
        :return:
        """
        uuid = str(uuid4())
        self._response[uuid] = []
        for action in valid_responses:
            self._request[action] = uuid
        return uuid

    def remove(self, uuid):
        """Remove any data related to a specific RPC request.

        :param str uuid: Rpc Identifier.
        :return:
        """
        self.remove_request(uuid)
        self.remove_response(uuid)

    def remove_request(self, uuid):
        """Remove any RPC request(s) using this uuid.

        :param str uuid: Rpc Identifier.
        :return:
        """
        for key in list(self._request):
            if self._request[key] == uuid:
                del self._request[key]

    def remove_response(self, uuid):
        """Remove a RPC Response using this uuid.

        :param str uuid: Rpc Identifier.
        :return:
        """
        if uuid in self._response:
            del self._response[uuid]

    def get_request(self, uuid, raw=False, multiple=False):
        """Get a RPC request.

        :param str uuid: Rpc Identifier
        :param bool raw: If enabled return the frame as is, else return
                         result as a dictionary.
        :param bool multiple: Are we expecting multiple frames.
        :return:
        """
        if uuid not in self._response:
            return
        self._wait_for_request(uuid)
        frame = self._get_response_frame(uuid)
        if not multiple:
            self.remove(uuid)
        result = None
        if raw:
            result = frame
        elif frame is not None:
            result = dict(frame)
        return result

    def _get_response_frame(self, uuid):
        """Get a response frame.

        :param str uuid: Rpc Identifier
        :return:
        """
        frame = None
        frames = self._response.get(uuid, None)
        if frames:
            frame = frames.pop(0)
        return frame

    def _wait_for_request(self, uuid):
        """Wait for RPC request to arrive.

        :param str uuid: Rpc Identifier.
        :return:
        """
        start_time = time.time()
        while not self._response[uuid]:
            self._adapter.check_for_errors()
            if time.time() - start_time > self._timeout:
                self._raise_rpc_timeout_error(uuid)
            time.sleep(IDLE_WAIT)

    def _raise_rpc_timeout_error(self, uuid):
        """Gather information and raise an Rpc exception.

        :param str uuid: Rpc Identifier.
        :return:
        """
        requests = []
        for key, value in self._request.items():
            if value == uuid:
                requests.append(key)
        self.remove(uuid)
        message = ('rpc requests %s (%s) took too long'
                   % (uuid, ', '.join(requests)))
        raise AMQPChannelError(message)


class BaseChannel(Stateful):
    """AMQP BaseChannel"""

    def __init__(self, channel_id):
        super(BaseChannel, self).__init__()
        self._consumer_tags = []
        self._channel_id = channel_id

    @property
    def channel_id(self):
        """Get Channel id.

        :rtype: int
        """
        return self._channel_id

    @property
    def consumer_tags(self):
        """Get a list of consumer tags.

        :rtype: list
        """
        return self._consumer_tags

    def add_consumer_tag(self, tag):
        """Add a Consumer tag.

        :param str tag: Consumer tag.
        :return:
        """
        if tag not in self._consumer_tags:
            self._consumer_tags.append(tag)

    def remove_consumer_tag(self, tag=None):
        """Remove a Consumer tag.

            If no tag is specified, all all tags will be removed.

        :param str tag: Consumer tag.
        :return:
        """
        if tag:
            if tag in self._consumer_tags:
                self._consumer_tags.remove(tag)
        else:
            self._consumer_tags = []


class BaseMessage(object):
    """AMQP BaseMessage"""
    __slots__ = ['_body', '_channel', '_method', '_properties']

    def __init__(self, channel, **message):
        """
        :param Channel channel: amqp-storm Channel
        :param str|unicode body: Message body
        :param dict method: Message method
        :param dict properties: Message properties
        """
        self._channel = channel
        self._body = message.get('body', None)
        self._method = message.get('method', None)
        self._properties = message.get('properties', {'headers': {}})

    def __iter__(self):
        for attribute in ['_body', '_channel', '_method', '_properties']:
            yield (attribute[1::], getattr(self, attribute))

    def to_dict(self):
        """Message to Dictionary.

        :rtype: dict
        """
        return {
            'body': self._body,
            'method': self._method,
            'properties': self._properties,
            'channel': self._channel
        }

    def to_tuple(self):
        """Message to Tuple.

        :rtype: tuple
        """
        return self._body, self._channel, self._method, self._properties


class Handler(object):
    """Operations Handler (e.g. Queue, Exchange)"""
    __slots__ = ['_channel']

    def __init__(self, channel):
        self._channel = channel
