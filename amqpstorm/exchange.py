""" AMQP-Storm Channel.Exchange. """
__author__ = 'eandersson'

import logging

from pamqp.specification import Exchange as pamqp_exchange


LOGGER = logging.getLogger(__name__)


class Exchange(object):
    """ Channel.Exchange """

    def __init__(self, channel):
        self._channel = channel

    def declare(self, exchange='', exchange_type='direct', passive=False,
                durable=False, auto_delete=False, arguments=None):
        """ Declare exchange.

        :param str exchange:
        :param str exchange_type:
        :param bool passive:
        :param bool durable:
        :param bool auto_delete:
        :param dict arguments:
        :return:
        """
        declare_frame = pamqp_exchange.Declare(exchange=exchange,
                                               exchange_type=exchange_type,
                                               passive=passive,
                                               durable=durable,
                                               auto_delete=auto_delete,
                                               arguments=arguments)
        with self._channel.rpc.lock:
            uuid = self._channel.rpc.register_request(pamqp_exchange.Declare)
            self._channel.write_frame(declare_frame)
            return self._channel.rpc.get_request(uuid)

    def delete(self, exchange='', if_unused=False):
        """ Delete exchange.

        :param str exchange:
        :param bool if_unused:
        :return:
        """
        delete_frame = pamqp_exchange.Delete(exchange=exchange,
                                             if_unused=if_unused)
        with self._channel.rpc.lock:
            uuid = self._channel.rpc.register_request(pamqp_exchange.Delete)
            self._channel.write_frame(delete_frame)
            return self._channel.rpc.get_request(uuid)

    def bind(self, destination='', source='', routing_key='',
             arguments=None):
        """ Bind exchange.

        :param str destination:
        :param str source:
        :param str routing_key:
        :param dict arguments:
        :return:
        """
        bind_frame = pamqp_exchange.Bind(destination=destination,
                                         source=source,
                                         routing_key=routing_key,
                                         arguments=arguments)
        with self._channel.rpc.lock:
            uuid = self._channel.rpc.register_request(pamqp_exchange.Bind)
            self._channel.write_frame(bind_frame)
            return self._channel.rpc.get_request(uuid)

    def unbind(self, destination='', source='', routing_key='',
               arguments=None):
        """ Unbind exchange.

        :param str destination:
        :param str source:
        :param str routing_key:
        :param dict arguments:
        :return:
        """
        unbind_frame = pamqp_exchange.Unbind(destination=destination,
                                             source=source,
                                             routing_key=routing_key,
                                             arguments=arguments)
        with self._channel.rpc.lock:
            uuid = self._channel.rpc.register_request(pamqp_exchange.Unbind)
            self._channel.write_frame(unbind_frame)
            return self._channel.rpc.get_request(uuid)
