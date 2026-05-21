from __future__ import annotations

from typing import Any
from typing import List

from amqpstorm.compatibility import json
from amqpstorm.compatibility import quote
from amqpstorm.management.base import ManagementHandler

API_EXCHANGE = 'exchanges/%s/%s'
API_EXCHANGE_BIND = 'bindings/%s/e/%s/e/%s'
API_EXCHANGE_BINDINGS = 'exchanges/%s/%s/bindings/source'
API_EXCHANGE_UNBIND = 'bindings/%s/e/%s/e/%s/%s'
API_EXCHANGES = 'exchanges'
API_EXCHANGES_VIRTUAL_HOST = 'exchanges/%s'


class Exchange(ManagementHandler):
    def get(self, exchange: str, virtual_host: str = '/') -> dict[str, Any]:
        """Get Exchange details.

        :param str exchange: Exchange name
        :param str virtual_host: Virtual host name

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        virtual_host = quote(virtual_host, '')
        return self.http_client.get(
            API_EXCHANGE
            % (
                virtual_host,
                exchange)
        )

    def list(
        self,
        virtual_host: str = '/',
        show_all: bool = False,
        name: str | None = None,
        page_size: int = 100,
        use_regex: bool = False,
    ) -> List[dict[str, Any]]:
        """List Exchanges.

        :param str virtual_host: Virtual host name
        :param bool show_all: List Exchanges across all virtual hosts
        :param name: Filter by name
        :param use_regex: Enables regular expression for the param name
        :param page_size: Number of elements per page

        :raises ApiError: Raises if the remote server encountered an error.
                          We also raise an exception if the exchange cannot
                          be found.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: list
        """
        if show_all:
            return self.http_client.list(
                API_EXCHANGES,
                name=name, use_regex=use_regex, page_size=page_size,
            )
        virtual_host = quote(virtual_host, '')
        return self.http_client.list(
            API_EXCHANGES_VIRTUAL_HOST % virtual_host,
            name=name, use_regex=use_regex, page_size=page_size,
        )

    def declare(
        self,
        exchange: str = '',
        exchange_type: str = 'direct',
        virtual_host: str = '/',
        passive: bool = False,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Declare an Exchange.

        :param str exchange: Exchange name
        :param str exchange_type: Exchange type
        :param str virtual_host: Virtual host name
        :param bool passive: Do not create
        :param bool durable: Durable exchange
        :param bool auto_delete: Automatically delete when not in use
        :param bool internal: Is the exchange for use by the broker only.
        :param dict,None arguments: Exchange key/value arguments

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: None
        """
        if passive:
            return self.get(exchange, virtual_host=virtual_host)
        exchange_payload = json.dumps(
            {
                'durable': durable,
                'auto_delete': auto_delete,
                'internal': internal,
                'type': exchange_type,
                'arguments': arguments or {},
                'vhost': virtual_host
            }
        )
        return self.http_client.put(API_EXCHANGE %
                                    (
                                        quote(virtual_host, ''),
                                        exchange
                                    ),
                                    payload=exchange_payload)

    def delete(self, exchange: str, virtual_host: str = '/') -> dict[str, Any]:
        """Delete an Exchange.

        :param str exchange: Exchange name
        :param str virtual_host: Virtual host name

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        virtual_host = quote(virtual_host, '')
        return self.http_client.delete(API_EXCHANGE %
                                       (
                                           virtual_host,
                                           exchange
                                       ))

    def bindings(
        self, exchange: str, virtual_host: str = '/',
    ) -> List[dict[str, Any]]:
        """Get Exchange bindings.

        :param str exchange: Exchange name
        :param str virtual_host: Virtual host name

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: list
        """
        virtual_host = quote(virtual_host, '')
        return self.http_client.get(API_EXCHANGE_BINDINGS %
                                    (
                                        virtual_host,
                                        exchange
                                    ))

    def bind(
        self,
        destination: str = '',
        source: str = '',
        routing_key: str = '',
        virtual_host: str = '/',
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind an Exchange.

        :param str source: Source Exchange name
        :param str destination: Destination Exchange name
        :param str routing_key: The routing key to use
        :param str virtual_host: Virtual host name
        :param dict,None arguments: Bind key/value arguments

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: None
        """
        bind_payload = json.dumps({
            'destination': destination,
            'destination_type': 'e',
            'routing_key': routing_key,
            'source': source,
            'arguments': arguments or {},
            'vhost': virtual_host
        })
        virtual_host = quote(virtual_host, '')
        return self.http_client.post(API_EXCHANGE_BIND %
                                     (
                                         virtual_host,
                                         source,
                                         destination
                                     ),
                                     payload=bind_payload)

    def unbind(
        self,
        destination: str = '',
        source: str = '',
        routing_key: str = '',
        virtual_host: str = '/',
        properties_key: str | None = None,
    ) -> None:
        """Unbind an Exchange.

        :param str source: Source Exchange name
        :param str destination: Destination Exchange name
        :param str routing_key: The routing key to use
        :param str virtual_host: Virtual host name
        :param str properties_key:

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: None
        """
        unbind_payload = json.dumps({
            'destination': destination,
            'destination_type': 'e',
            'properties_key': properties_key or routing_key,
            'source': source,
            'vhost': virtual_host
        })
        virtual_host = quote(virtual_host, '')
        return self.http_client.delete(API_EXCHANGE_UNBIND %
                                       (
                                           virtual_host,
                                           source,
                                           destination,
                                           properties_key or routing_key
                                       ),
                                       payload=unbind_payload)
