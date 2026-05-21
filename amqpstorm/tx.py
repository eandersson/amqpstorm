"""AMQPStorm Channel.Tx."""
from __future__ import annotations

import logging
from types import TracebackType
from typing import TYPE_CHECKING
from typing import Any

from pamqp import commands

from amqpstorm.base import Handler

if TYPE_CHECKING:
    from amqpstorm.channel import Channel

LOGGER = logging.getLogger(__name__)


class Tx(Handler):
    """RabbitMQ Transactions.

        Server local transactions, in which the server will buffer published
        messages until the client commits (or rollback) the messages.

    """
    __slots__ = ['_tx_active']

    def __init__(self, channel: Channel) -> None:
        self._tx_active = True
        super().__init__(channel)

    def __enter__(self) -> Tx:
        self.select()
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        _: TracebackType | None,
    ) -> None:
        if exception_type:
            LOGGER.warning(
                'Leaving Transaction on exception: %s',
                exception_value
            )
            if self._tx_active:
                self.rollback()
            return
        if self._tx_active:
            self.commit()

    def select(self) -> dict[str, Any] | None:
        """Enable standard transaction mode.

            This will enable transaction mode on the channel. Meaning that
            messages will be kept in the remote server buffer until such a
            time that either commit or rollback is called.

        :return:
        """
        self._tx_active = True
        return self._channel.rpc_request(commands.Tx.Select())

    def commit(self) -> dict[str, Any] | None:
        """Commit the current transaction.

            Commit all messages published during the current transaction
            session to the remote server.

            A new transaction session starts as soon as the command has
            been executed.

        :return:
        """
        self._tx_active = False
        return self._channel.rpc_request(commands.Tx.Commit())

    def rollback(self) -> dict[str, Any] | None:
        """Abandon the current transaction.

            Rollback all messages published during the current transaction
            session to the remote server.

            Note that all messages published during this transaction session
            will be lost, and will have to be published again.

            A new transaction session starts as soon as the command has
            been executed.

        :return:
        """
        self._tx_active = False
        return self._channel.rpc_request(commands.Tx.Rollback())
