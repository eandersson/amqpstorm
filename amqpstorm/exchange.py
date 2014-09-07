"""AMQP-Storm Channel.Exchange."""
__author__ = 'eandersson'

import logging

from pamqp.specification import Exchange as pamqp_exchange


LOGGER = logging.getLogger(__name__)


class Exchange(object):
    """Channel.Exchange."""

    def __init__(self, channel):
        self._channel = channel

    def declare(self, exchange='', exchange_type='direct', passive=False,
                durable=False, auto_delete=False, arguments=None):
        """Declare exchange.

        :param str exchange:
        :param str exchange_type:
        :param bool passive:
        :param bool durable:
        :param bool auto_delete:
        :param dict arguments:
        :rtype: dict
        """
        declare_frame = pamqp_exchange.Declare(exchange=exchange,
                                               exchange_type=exchange_type,
                                               passive=passive,
                                               durable=durable,
                                               auto_delete=auto_delete,
                                               arguments=arguments)
        return self._channel.rpc_request(declare_frame)

    def delete(self, exchange='', if_unused=False):
        """Delete exchange.

        :param str exchange:
        :param bool if_unused:
        :rtype: dict
        """
        delete_frame = pamqp_exchange.Delete(exchange=exchange,
                                             if_unused=if_unused)
        return self._channel.rpc_request(delete_frame)

    def bind(self, destination='', source='', routing_key='',
             arguments=None):
        """Bind exchange.

        :param str destination:
        :param str source:
        :param str routing_key:
        :param dict arguments:
        :rtype: dict
        """
        bind_frame = pamqp_exchange.Bind(destination=destination,
                                         source=source,
                                         routing_key=routing_key,
                                         arguments=arguments)
        return self._channel.rpc_request(bind_frame)

    def unbind(self, destination='', source='', routing_key='',
               arguments=None):
        """Unbind exchange.

        :param str destination:
        :param str source:
        :param str routing_key:
        :param dict arguments:
        :rtype: dict
        """
        unbind_frame = pamqp_exchange.Unbind(destination=destination,
                                             source=source,
                                             routing_key=routing_key,
                                             arguments=arguments)
        return self._channel.rpc_request(unbind_frame)
