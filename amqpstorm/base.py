"""AMQPStorm Base."""
from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Iterator

from amqpstorm.compatibility import is_string
from amqpstorm.exception import AMQPChannelError

if TYPE_CHECKING:
    from amqpstorm.channel import Channel

AUTH_MECHANISM = 'PLAIN'
IDLE_WAIT = 0.01
MAX_FRAME_SIZE = 131072
MAX_CHANNELS = 65535


class Stateful:
    """Stateful implementation."""
    CLOSED = 0
    CLOSING = 1
    OPENING = 2
    OPEN = 3

    def __init__(self) -> None:
        self._state: int = self.CLOSED
        self._exceptions: list[Exception] = []

    def set_state(self, state: int) -> None:
        """Set State.

        :param int state:
        :return:
        """
        self._state = state

    @property
    def current_state(self) -> int:
        """Get the State.

        :rtype: int
        """
        return self._state

    @property
    def is_closed(self) -> bool:
        """Is Closed?

        :rtype: bool
        """
        return self._state == self.CLOSED

    @property
    def is_closing(self) -> bool:
        """Is Closing?

        :rtype: bool
        """
        return self._state == self.CLOSING

    @property
    def is_opening(self) -> bool:
        """Is Opening?

        :rtype: bool
        """
        return self._state == self.OPENING

    @property
    def is_open(self) -> bool:
        """Is Open?

        :rtype: bool
        """
        return self._state == self.OPEN

    @property
    def exceptions(self) -> list[Exception]:
        """Stores all exceptions thrown by this instance.

            This is useful for troubleshooting, and is used internally
            to check the health of the connection.

        :rtype: list
        """
        return self._exceptions


class BaseChannel(Stateful):
    """Channel base class."""
    __slots__ = [
        '_channel_id', '_consumer_tags'
    ]

    def __init__(self, channel_id: int) -> None:
        super().__init__()
        self._consumer_tags: list[str] = []
        self._channel_id: int = channel_id

    @property
    def channel_id(self) -> int:
        """Get Channel id.

        :rtype: int
        """
        return self._channel_id

    @property
    def consumer_tags(self) -> list[str]:
        """Get a list of consumer tags.

        :rtype: list
        """
        return self._consumer_tags

    def add_consumer_tag(self, tag: str) -> None:
        """Add a Consumer tag.

        :param str tag: Consumer tag.
        :return:
        """
        if not is_string(tag):
            raise AMQPChannelError('consumer tag needs to be a string')
        if tag not in self._consumer_tags:
            self._consumer_tags.append(tag)

    def remove_consumer_tag(self, tag: str | None = None) -> None:
        """Remove a Consumer tag.

            If no tag is specified, all tags will be removed.

        :param str,None tag: Consumer tag.
        :return:
        """
        if tag is not None:
            if tag in self._consumer_tags:
                self._consumer_tags.remove(tag)
        else:
            self._consumer_tags = []


class BaseMessage:
    """Message base class.

    :param Channel channel: AMQPStorm Channel
    :param str,unicode body: Message body
    :param dict method: Message method
    :param dict properties: Message properties
    :param bool auto_decode: This is not implemented in the base message class.
    """
    __slots__ = [
        '_auto_decode', '_body', '_channel', '_method', '_properties'
    ]

    def __init__(
        self,
        channel: Channel | None,
        body: bytes | str | None = None,
        method: dict[str, Any] | None = None,
        properties: dict[str, Any] | None = None,
        auto_decode: bool | None = None,
    ) -> None:
        self._auto_decode = auto_decode
        self._channel = channel
        self._body = body
        self._method = method
        self._properties: dict[str, Any] = properties or {}

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        for attribute in ['_body', '_channel', '_method', '_properties']:
            yield attribute[1::], getattr(self, attribute)

    def to_dict(self) -> dict[str, Any]:
        """Message to Dictionary.

        :rtype: dict
        """
        return {
            'body': self._body,
            'method': self._method,
            'properties': self._properties,
            'channel': self._channel
        }

    def to_tuple(
        self,
    ) -> tuple[bytes | str | None, Channel | None, dict[str, Any] | None, dict[str, Any]]:
        """Message to Tuple.

        :rtype: tuple
        """
        return self._body, self._channel, self._method, self._properties


class Handler:
    """Operations Handler (e.g. Queue, Exchange)"""
    __slots__ = [
        '_channel'
    ]

    def __init__(self, channel: Channel) -> None:
        self._channel = channel
