from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amqpstorm.management.http_client import HTTPClient


class ManagementHandler:
    """Management Api Operations Handler (e.g. Queue, Exchange)"""

    def __init__(self, http_client: HTTPClient) -> None:
        self.http_client = http_client
