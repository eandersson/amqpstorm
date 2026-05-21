from __future__ import annotations

from typing import Any
from typing import List

from amqpstorm.management.base import ManagementHandler

API_CHANNEL = 'channels/%s'
API_CHANNELS = 'channels'


class Channel(ManagementHandler):
    def get(self, channel: str) -> dict[str, Any]:
        """Get Channel details.

        :param channel: Channel name

        :raises ApiError: Raises if the remote server encountered an error.
                          We also raise an exception if the channel cannot
                          be found.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        return self.http_client.get(API_CHANNEL % channel)

    def list(
        self,
        name: str | None = None,
        page_size: int | None = None,
        use_regex: bool = False,
    ) -> List[dict[str, Any]]:
        """List all Channels.

        :param name: Filter by name
        :param use_regex: Enables regular expression for the param name
        :param page_size: Number of elements per page

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: list
        """
        return self.http_client.list(
            API_CHANNELS,
            name=name, use_regex=use_regex, page_size=page_size,
        )
