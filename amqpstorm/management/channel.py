from amqpstorm.management.base import ManagementHandler

API_CHANNEL = 'channels/%s'
API_CHANNELS = 'channels'


class Channel(ManagementHandler):
    def get(self, channel):
        """Get Connection details.

        :param channel: Channel name

        :raises ApiError: Raises if the remote server encountered an error.
                          We also raise an exception if the channel cannot
                          be found.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        return self.http_client.get(API_CHANNEL % channel)

    def list(self, name=None, page_size=None, use_regex=False):
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
