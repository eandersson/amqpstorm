__author__ = 'eandersson'

import logging

from amqpstorm import Connection

from examples import HOST
from examples import USERNAME
from examples import PASSWORD


logging.basicConfig(level=logging.DEBUG)


    with Connection(HOST, USERNAME, PASSWORD) as connection:
        with connection.channel() as channel:
            for message in channel.build_inbound_messages():
                message.ack()


if __name__ == '__main__':

