from __future__ import annotations

from typing import Any
from typing import List

from amqpstorm.compatibility import quote
from amqpstorm.management.base import ManagementHandler

API_VIRTUAL_HOST = 'vhosts/%s'
API_VIRTUAL_HOSTS = 'vhosts'
API_VIRTUAL_HOSTS_PERMISSION = 'vhosts/%s/permissions'


class VirtualHost(ManagementHandler):
    def get(self, virtual_host: str) -> dict[str, Any]:
        """Get Virtual Host details.

        :param str virtual_host: Virtual host name

        :raises ApiError: Raises if the remote server encountered an error.
                          We also raise an exception if the virtual host cannot
                          be found.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        virtual_host = quote(virtual_host, '')
        return self.http_client.get(API_VIRTUAL_HOST % virtual_host)

    def list(self) -> List[dict[str, Any]]:
        """List all Virtual Hosts.

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: list
        """
        return self.http_client.get(API_VIRTUAL_HOSTS)

    def create(self, virtual_host: str) -> dict[str, Any]:
        """Create a Virtual Host.

        :param str virtual_host: Virtual host name

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        virtual_host = quote(virtual_host, '')
        return self.http_client.put(API_VIRTUAL_HOST % virtual_host)

    def delete(self, virtual_host: str) -> dict[str, Any]:
        """Delete a Virtual Host.

        :param str virtual_host: Virtual host name

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        virtual_host = quote(virtual_host, '')
        return self.http_client.delete(API_VIRTUAL_HOST % virtual_host)

    def get_permissions(self, virtual_host: str) -> dict[str, Any]:
        """Get all Virtual hosts permissions.

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        virtual_host = quote(virtual_host, '')
        return self.http_client.get(API_VIRTUAL_HOSTS_PERMISSION %
                                    (
                                        virtual_host
                                    ))
