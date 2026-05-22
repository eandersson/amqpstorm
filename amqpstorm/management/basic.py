from __future__ import annotations

import json
from typing import Any

from amqpstorm.compatibility import quote
from amqpstorm.management.base import ManagementHandler
from amqpstorm.message import Message

API_BASIC_GET_MESSAGE = 'queues/%s/%s/get'
API_BASIC_PUBLISH = 'exchanges/%s/%s/publish'


class Basic(ManagementHandler):
    def publish(
        self,
        body: str,
        routing_key: str,
        exchange: str = 'amq.default',
        virtual_host: str = '/',
        properties: dict[str, Any] | None = None,
        payload_encoding: str = 'string',
    ) -> dict[str, Any]:
        """Publish a Message.

        :param bytes,str,unicode body: Message payload
        :param str routing_key: Message routing key
        :param str exchange: The exchange to publish the message to
        :param str virtual_host: Virtual host name
        :param dict properties: Message properties
        :param str payload_encoding: Payload encoding.

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        exchange = quote(exchange, '')
        properties = properties or {}
        body = json.dumps(
            {
                'routing_key': routing_key,
                'payload': body,
                'payload_encoding': payload_encoding,
                'properties': properties,
                'vhost': virtual_host
            }
        )
        virtual_host = quote(virtual_host, '')
        return self.http_client.post(API_BASIC_PUBLISH %
                                     (
                                         virtual_host,
                                         exchange),
                                     payload=body)

    def get(
        self,
        queue: str,
        virtual_host: str = '/',
        requeue: bool = False,
        to_dict: bool = False,
        count: int = 1,
        truncate: int = 50000,
        encoding: str = 'auto',
    ) -> list[Message] | list[dict[str, Any]]:
        """Get Messages.

        :param str queue: Queue name
        :param str virtual_host: Virtual host name
        :param bool requeue: Re-queue message
        :param bool to_dict: Should incoming messages be converted to a
                             dictionary before delivery.
        :param int count: How many messages should we try to fetch.
        :param int truncate: The maximum length in bytes, beyond that the
                             server will truncate the message.
        :param str encoding: Message encoding.

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: list
        """
        ackmode = 'ack_requeue_false'
        if requeue:
            ackmode = 'ack_requeue_true'

        get_messages = json.dumps(
            {
                'count': count,
                'requeue': requeue,
                'ackmode': ackmode,
                'encoding': encoding,
                'truncate': truncate,
                'vhost': virtual_host
            }
        )
        virtual_host = quote(virtual_host, '')
        response = self.http_client.post(API_BASIC_GET_MESSAGE %
                                         (
                                             virtual_host,
                                             queue
                                         ),
                                         payload=get_messages)
        if to_dict:
            return response
        messages = []
        for message in response:
            body = message.get('body')
            if not body:
                body = message.get('payload')
            messages.append(Message(
                channel=None,
                body=body,
                properties=message.get('properties'),
                auto_decode=True,
            ))
        return messages
