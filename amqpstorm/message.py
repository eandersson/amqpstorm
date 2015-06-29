"""AMQP-Storm Message."""
__author__ = 'eandersson'

import json
import uuid

from datetime import datetime

from amqpstorm.exception import AMQPMessageError
from amqpstorm.compatibility import try_utf8_decode


class Message(object):
    """RabbitMQ Message Class."""
    __slots__ = ['_auto_decode', '_decode_cache', '_body', '_channel',
                 '_method', '_properties']

    def __init__(self, channel, auto_decode=True, **message):
        """
        :param Channel channel: amqp-storm Channel
        :param bool auto_decode: Auto-decode strings when possible. Does not
                                 apply to to_dict, or to_tuple.
        :param str|unicode body: Message body
        :param dict method: Message method
        :param dict properties: Message properties
        """
        self._decode_cache = dict()
        self._auto_decode = auto_decode
        self._channel = channel
        self._body = message.get('body', None)
        self._method = message.get('method', None)
        self._properties = message.get('properties', {'headers': {}})

    def __iter__(self):
        for attribute in ['_body', '_channel', '_method', '_properties']:
            yield (attribute[1::], getattr(self, attribute))

    @property
    def app_id(self):
        """AMQP attribute app_id.

        :return:
        """
        return self.properties.get('app_id')

    @property
    def user_id(self):
        """AMQP attribute user_id.

        :return:
        """
        return self.properties.get('user_id')

    @property
    def message_id(self):
        """AMQP attribute message_id.

        :return:
        """
        return self.properties.get('message_id')

    @property
    def content_encoding(self):
        """AMQP attribute content_encoding.

        :return:
        """
        return self.properties.get('content_encoding')

    @property
    def content_type(self):
        """AMQP attribute content_type.

        :return:
        """
        return self.properties.get('content_type')

    @property
    def correlation_id(self):
        """AMQP attribute correlation_id.

        :return:
        """
        return self.properties.get('correlation_id')

    @property
    def reply_to(self):
        """AMQP attribute reply_to.

        :return:
        """
        return self.properties.get('reply_to')

    @property
    def delivery_mode(self):
        """AMQP attribute delivery_mode.

        :return:
        """
        return self.properties.get('delivery_mode')

    @property
    def timestamp(self):
        """AMQP attribute timestamp.

        :return:
        """
        return self.properties.get('timestamp')

    @property
    def priority(self):
        """AMQP attribute priority.

        :return:
        """
        return self.properties.get('priority')

    @staticmethod
    def create(channel, body, properties=None, **kwargs):
        """Create a new Message.

        :param Channel channel: AMQP-Storm Channel
        :param bytes|str|unicode body: Message body
        :param dict properties: Message properties
        :param str app_id: Application id
        :param str message_id: Message id
        :param str user_id: User id
        :param str content_encoding: Message content encoding
        :param str correlation_id: Message correlation id
        :param str reply_to: Message reply to
        :param int delivery_mode: Message delivery mode
        :param int priority: Message priority
        :param datetime timestamp: Message timestamp
        :rtype: Message
        """
        properties = properties or {}
        if 'app_id' in kwargs:
            properties['app_id'] = kwargs['app_id']
        if 'user_id' in kwargs:
            properties['user_id'] = kwargs['user_id']
        if 'message_id' in kwargs:
            properties['message_id'] = kwargs['message_id']
        if 'content_encoding' in kwargs:
            properties['content_encoding'] = kwargs['content_encoding']
        if 'content_type' in kwargs:
            properties['content_type'] = kwargs['content_type']
        if 'reply_to' in kwargs:
            properties['reply_to'] = kwargs['reply_to']
        if 'delivery_mode' in kwargs:
            properties['delivery_mode'] = kwargs['delivery_mode']
        if 'priority' in kwargs:
            properties['priority'] = kwargs['priority']

        if 'correlation_id' in kwargs:
            properties['correlation_id'] = kwargs['correlation_id']
        elif 'correlation_id' not in properties:
            properties['correlation_id'] = str(uuid.uuid4())

        if 'timestamp' in kwargs:
            properties['timestamp'] = kwargs['timestamp']
        elif 'timestamp' not in properties:
            properties['timestamp'] = datetime.utcnow()

        return Message(channel, auto_decode=False,
                       body=body, properties=properties)

    @property
    def body(self):
        """Return the Message Body.

            If auto_decode is enabled, the body will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: bytes|str|unicode
        """
        if not self._auto_decode:
            return self._body
        if 'body' in self._decode_cache:
            return self._decode_cache['body']
        body = try_utf8_decode(self._body)
        self._decode_cache['body'] = body
        return body

    @property
    def channel(self):
        """Return the Channel.

        :rtype: Channel
        """
        return self._channel

    @property
    def method(self):
        """Return the Message Method.

            If auto_decode is enabled, the any strings will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: dict
        """
        return self._try_decode_utf8_content(self._method, 'method')

    @property
    def properties(self):
        """Returns the Message Properties.

            If auto_decode is enabled, the any strings will automatically be
            decoded using decode('utf-8') if possible.

        :rtype: dict
        """
        return self._try_decode_utf8_content(self._properties, 'properties')

    def ack(self):
        """Acknowledge Message.

        :return:
        """
        if not self._method:
            raise AMQPMessageError('Message.ack only available on '
                                   'incoming messages.')
        self._channel.basic.ack(delivery_tag=self._method['delivery_tag'])

    def nack(self, requeue=True):
        """Negative Acknowledgement.

        :param bool requeue:
        """
        if not self._method:
            raise AMQPMessageError('Message.nack only available on '
                                   'incoming messages.')
        self._channel.basic.nack(delivery_tag=self._method['delivery_tag'],
                                 requeue=requeue)

    def reject(self, requeue=True):
        """Reject Message.

        :param bool requeue: Requeue the message
        """
        if not self._method:
            raise AMQPMessageError('Message.reject only available on '
                                   'incoming messages.')
        self._channel.basic.reject(delivery_tag=self._method['delivery_tag'],
                                   requeue=requeue)

    def publish(self, routing_key, exchange='', mandatory=False,
                immediate=False):
        """Publish Message.

        :param str routing_key:
        :param str exchange:
        :param bool mandatory:
        :param bool immediate:
        """
        return self._channel.basic.publish(body=self._body,
                                           routing_key=routing_key,
                                           exchange=exchange,
                                           properties=self._properties,
                                           mandatory=mandatory,
                                           immediate=immediate)

    def json(self):
        """Deserialize the message body, if it is JSON.

        :return:
        """
        return json.loads(self.body)

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

    def _try_decode_utf8_content(self, content, content_type):
        """Generic function to decode content.

        :param object content:
        :return:
        """
        if not self._auto_decode or not content:
            return content
        if content_type in self._decode_cache:
            return self._decode_cache[content_type]
        if isinstance(content, dict):
            return self._try_decode_dict_content(content)
        content = try_utf8_decode(content)
        self._decode_cache[content_type] = content
        return content

    def _try_decode_dict_content(self, content):
        """Decode content of a dictionary.

        :param dict content:
        :return:
        """
        result = dict()
        for key, value in content.items():
            key = try_utf8_decode(key)
            if isinstance(value, dict):
                result[key] = self._try_decode_dict_content(value)
            elif isinstance(value, list):
                result[key] = self._try_decode_list_content(value)
            else:
                result[key] = try_utf8_decode(value)
        return result

    @staticmethod
    def _try_decode_list_content(content):
        """Decode content of a list.

        :param list content:
        :return:
        """
        result = list()
        for value in content:
            result.append(try_utf8_decode(value))
        return result
