from __future__ import annotations

from typing import Any
from typing import List

from amqpstorm.compatibility import json
from amqpstorm.compatibility import quote
from amqpstorm.management.base import ManagementHandler

API_CONNECTION = 'connections/%s'
API_CONNECTIONS = 'connections'


class Connection(ManagementHandler):
    def get(self, connection: str) -> dict[str, Any]:
        """Get Connection details.

        :param str connection: Connection name

        :raises ApiError: Raises if the remote server encountered an error.
                          We also raise an exception if the connection cannot
                          be found.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        return self.http_client.get(API_CONNECTION % connection)

    def list(
        self,
        name: str | None = None,
        page_size: int = 100,
        use_regex: bool = False,
    ) -> List[dict[str, Any]]:
        """Get Connections.

        :param name: Filter by name
        :param use_regex: Enables regular expression for the param name
        :param page_size: Number of elements per page

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: list
        """
        return self.http_client.list(
            API_CONNECTIONS,
            name=name, use_regex=use_regex, page_size=page_size,
        )

    def close(
        self,
        connection: str,
        reason: str = 'Closed via management api',
    ) -> None:
        """Close Connection.

        :param str connection: Connection name
        :param str reason: Reason for closing connection.

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: None
        """
        close_payload = json.dumps({
            'name': connection,
            'reason': reason
        })
        connection = quote(connection, '')
        return self.http_client.delete(API_CONNECTION % connection,
                                       payload=close_payload,
                                       headers={
                                           'X-Reason': reason
                                       })
