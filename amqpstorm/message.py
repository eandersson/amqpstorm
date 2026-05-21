"""AMQPStorm Message."""
from __future__ import annotations

import datetime
import json
import uuid
from typing import TYPE_CHECKING
from typing import Any

from amqpstorm.base import BaseMessage
from amqpstorm.compatibility import try_utf8_decode
from amqpstorm.exception import AMQPMessageError

if TYPE_CHECKING:
    from amqpstorm.channel import Channel


class Message(BaseMessage):
    """RabbitMQ Message.

    e.g.
    ::

        # Message Properties.
        properties = {
            'content_type': 'text/plain',
            'expiration': '3600',
            'headers': {'key': 'value'},
        }
        # Create a new message.
        message = Message.create(channel, 'Hello RabbitMQ!', properties)
        # Publish the message to a queue called, 'my_queue'.
        message.publish('my_queue')

    :param Channel channel: AMQPStorm Channel
    :param bytes,str,unicode body: Message payload
    :param dict method: Message method
    :param dict properties: Message properties
    :param bool auto_decode: Auto-decode strings when possible. Does not
                             apply to to_dict, or to_tuple.
    """
    __slots__ = [
        '_decode_cache'
    ]

    def __init__(
        self,
        channel: Channel | None,
        body: bytes | str | None = None,
        method: dict[str, Any] | None = None,
        properties: dict[str, Any] | None = None,
        auto_decode: bool = True,
    ) -> None:
        super().__init__(
            channel, body, method, properties, auto_decode
        )
        self._decode_cache: dict[str, Any] = {}

    @staticmethod
    def create(
        channel: Channel,
        body: bytes | str,
        properties: dict[str, Any] | None = None,
    ) -> Message:
        """Create a new Message.

        :param Channel channel: AMQPStorm Channel
        :param bytes,str,unicode body: Message payload
        :param dict properties: Message properties

        :rtype: Message
        """
        properties = dict(properties or {})
        if 'correlation_id' not in properties:
            properties['correlation_id'] = str(uuid.uuid4())
        if 'message_id' not in properties:
            properties['message_id'] = str(uuid.uuid4())
        if 'timestamp' not in properties:
            properties['timestamp'] = datetime.datetime.now(datetime.UTC)

        return Message(channel, auto_decode=False,
                       body=body, properties=properties)

    @property
    def body(self) -> bytes | str | None:
        """Return the Message Body.

            If auto_decode is enabled, the body will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: bytes,str,unicode
        """
        if not self._auto_decode:
            return self._body
        if 'body' in self._decode_cache:
            return self._decode_cache['body']
        body = try_utf8_decode(self._body)
        self._decode_cache['body'] = body
        return body

    @property
    def channel(self) -> Channel | None:
        """Return the Channel used by this message.

        :rtype: Channel
        """
        return self._channel

    @property
    def method(self) -> dict[str, Any] | None:
        """Return the Message Method.

            If auto_decode is enabled, all strings will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: dict
        """
        return self._try_decode_utf8_content(self._method, 'method')

    @property
    def properties(self) -> dict[str, Any]:
        """Returns the Message Properties.

            If auto_decode is enabled, all strings will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: dict
        """
        return self._try_decode_utf8_content(self._properties, 'properties')

    def ack(self) -> None:
        """Acknowledge Message.

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :return:
        """
        if not self._method:
            raise AMQPMessageError(
                'Message.ack only available on incoming messages'
            )
        self._channel.basic.ack(delivery_tag=self.delivery_tag)

    def nack(self, requeue: bool = True) -> None:
        """Negative Acknowledgement.

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :param bool requeue: Re-queue the message
        """
        if not self._method:
            raise AMQPMessageError(
                'Message.nack only available on incoming messages'
            )
        self._channel.basic.nack(delivery_tag=self.delivery_tag,
                                 requeue=requeue)

    def reject(self, requeue: bool = True) -> None:
        """Reject Message.

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :param bool requeue: Re-queue the message
        """
        if not self._method:
            raise AMQPMessageError(
                'Message.reject only available on incoming messages'
            )
        self._channel.basic.reject(delivery_tag=self.delivery_tag,
                                   requeue=requeue)

    def publish(
        self,
        routing_key: str,
        exchange: str = '',
        mandatory: bool = False,
        immediate: bool = False,
    ) -> bool | None:
        """Publish Message.

        :param str routing_key: Message routing key
        :param str exchange: The exchange to publish the message to
        :param bool mandatory: Requires the message is published
        :param bool immediate: Request immediate delivery

        :raises AMQPInvalidArgument: Invalid Parameters
        :raises AMQPChannelError: Raises if the channel encountered an error.
        :raises AMQPConnectionError: Raises if the connection
                                     encountered an error.

        :rtype: bool,None
        """
        return self._channel.basic.publish(body=self._body,
                                           routing_key=routing_key,
                                           exchange=exchange,
                                           properties=self._properties,
                                           mandatory=mandatory,
                                           immediate=immediate)

    @property
    def app_id(self) -> Any | None:
        """Get AMQP Message attribute: app_id.

        :return:
        """
        return self.properties.get('app_id')

    @app_id.setter
    def app_id(self, value: Any) -> None:
        """Set AMQP Message attribute: app_id.

        :return:
        """
        self._update_properties('app_id', value)

    @property
    def message_id(self) -> Any | None:
        """Get AMQP Message attribute: message_id.

        :return:
        """
        return self.properties.get('message_id')

    @message_id.setter
    def message_id(self, value: Any) -> None:
        """Set AMQP Message attribute: message_id.

        :return:
        """
        self._update_properties('message_id', value)

    @property
    def content_encoding(self) -> Any | None:
        """Get AMQP Message attribute: content_encoding.

        :return:
        """
        return self.properties.get('content_encoding')

    @content_encoding.setter
    def content_encoding(self, value: Any) -> None:
        """Set AMQP Message attribute: content_encoding.

        :return:
        """
        self._update_properties('content_encoding', value)

    @property
    def content_type(self) -> Any | None:
        """Get AMQP Message attribute: content_type.

        :return:
        """
        return self.properties.get('content_type')

    @content_type.setter
    def content_type(self, value: Any) -> None:
        """Set AMQP Message attribute: content_type.

        :return:
        """
        self._update_properties('content_type', value)

    @property
    def correlation_id(self) -> Any | None:
        """Get AMQP Message attribute: correlation_id.

        :return:
        """
        return self.properties.get('correlation_id')

    @correlation_id.setter
    def correlation_id(self, value: Any) -> None:
        """Set AMQP Message attribute: correlation_id.

        :return:
        """
        self._update_properties('correlation_id', value)

    @property
    def delivery_mode(self) -> Any | None:
        """Get AMQP Message attribute: delivery_mode.

        :return:
        """
        return self.properties.get('delivery_mode')

    @delivery_mode.setter
    def delivery_mode(self, value: Any) -> None:
        """Set AMQP Message attribute: delivery_mode.

        :return:
        """
        self._update_properties('delivery_mode', value)

    @property
    def timestamp(self) -> Any | None:
        """Get AMQP Message attribute: timestamp.

        :return:
        """
        return self.properties.get('timestamp')

    @timestamp.setter
    def timestamp(self, value: Any) -> None:
        """Set AMQP Message attribute: timestamp.

        :return:
        """
        self._update_properties('timestamp', value)

    @property
    def priority(self) -> Any | None:
        """Get AMQP Message attribute: priority.

        :return:
        """
        return self.properties.get('priority')

    @priority.setter
    def priority(self, value: Any) -> None:
        """Set AMQP Message attribute: priority.

        :return:
        """
        self._update_properties('priority', value)

    @property
    def reply_to(self) -> Any | None:
        """Get AMQP Message attribute: reply_to.

        :return:
        """
        return self.properties.get('reply_to')

    @reply_to.setter
    def reply_to(self, value: Any) -> None:
        """Set AMQP Message attribute: reply_to.

        :return:
        """
        self._update_properties('reply_to', value)

    @property
    def message_type(self) -> Any | None:
        """Get AMQP Message attribute: message_type.

        :return:
        """
        return self.properties.get('message_type')

    @message_type.setter
    def message_type(self, value: Any) -> None:
        """Set AMQP Message attribute: message_type.

        :return:
        """
        self._update_properties('message_type', value)

    @property
    def expiration(self) -> Any | None:
        """Get AMQP Message attribute: expiration.

        :return:
        """
        return self.properties.get('expiration')

    @expiration.setter
    def expiration(self, value: Any) -> None:
        """Set AMQP Message attribute: expiration.

        :return:
        """
        self._update_properties('expiration', value)

    @property
    def user_id(self) -> Any | None:
        """Get AMQP Message attribute: user_id.

        :return:
        """
        return self.properties.get('user_id')

    @user_id.setter
    def user_id(self, value: Any) -> None:
        """Set AMQP Message attribute: user_id.

        :return:
        """
        self._update_properties('user_id', value)

    @property
    def redelivered(self) -> bool | None:
        """Indicates if this message may have been delivered before (but not
        acknowledged).

        :rtype: bool,None
        """
        if not self._method:
            return None
        return self._method.get('redelivered')

    @property
    def delivery_tag(self) -> int | None:
        """Server-assigned delivery tag.

        :rtype: int,None
        """
        if not self._method:
            return None
        return self._method.get('delivery_tag')

    def json(self) -> Any:
        """Deserialize the message body, if it is JSON.

        :return:
        """
        return json.loads(self.body)

    def _update_properties(self, name: str, value: Any) -> None:
        """Update properties, and keep cache up-to-date if auto decode is
        enabled.

        :param str name: Key
        :param obj value: Value
        :return:
        """
        if self._auto_decode and 'properties' in self._decode_cache:
            self._decode_cache['properties'][name] = value
        self._properties[name] = value

    def _try_decode_utf8_content(self, content: Any, content_type: str) -> Any:
        """Generic function to decode content.

        :param object content:
        :return:
        """
        if not self._auto_decode or not content:
            return content
        if content_type in self._decode_cache:
            return self._decode_cache[content_type]
        if isinstance(content, dict):
            content = self._try_decode_dict(content)
        else:
            content = try_utf8_decode(content)
        self._decode_cache[content_type] = content
        return content

    def _try_decode_dict(self, content: dict[Any, Any]) -> dict[Any, Any]:
        """Decode content of a dictionary.

        :param dict content:
        :return:
        """
        result: dict[Any, Any] = {}
        for key, value in content.items():
            key = try_utf8_decode(key)
            if isinstance(value, dict):
                result[key] = self._try_decode_dict(value)
            elif isinstance(value, list):
                result[key] = self._try_decode_list(value)
            elif isinstance(value, tuple):
                result[key] = self._try_decode_tuple(value)
            else:
                result[key] = try_utf8_decode(value)
        return result

    @staticmethod
    def _try_decode_list(content: list[Any] | tuple[Any, ...]) -> list[Any]:
        """Decode content of a list.

        :param list,tuple content:
        :return:
        """
        result = list()
        for value in content:
            result.append(try_utf8_decode(value))
        return result

    @staticmethod
    def _try_decode_tuple(content: tuple[Any, ...]) -> tuple[Any, ...]:
        """Decode content of a tuple.

        :param tuple content:
        :return:
        """
        return tuple(Message._try_decode_list(content))
