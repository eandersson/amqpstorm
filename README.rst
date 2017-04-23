AMQPStorm
=========
Thread-safe Python RabbitMQ Client & Management library.

|Version| |CodeClimate| |Travis| |Coverage|

Introduction
============
AMQPStorm is a library designed to be consistent, stable and thread-safe.

- 100% Unit-test Coverage!
- Supports Python 2.6, 2.7 and Python 3.3+.
- Fully tested against Python Implementations; CPython, PyPy and Pyston.
- When using a SSL connection, TLSv1 or higher is required.

Documentation
=============

Additional documentation is available on `amqpstorm.io <https://www.amqpstorm.io>`_.

Changelog
=========

Version 2.2.2
-------------
- Fixed potential Heartbeat deadlock when forcefully killing process - Thanks Charles Pierre.

Version 2.2.1
-------------
- Fixed potential Channel leak [#36] - Thanks Adam Mills.
- Fixed threading losing select module during python shutdown [#37] - Thanks Adam Mills.

Version 2.2.0
-------------
- Connection.close should now be more responsive.
- Channels are now reset when re-opening an existing connection.
- Re-wrote large portions of the Test suit.

Version 2.1.4
-------------
- Added parameter to override auto-decode on incoming Messages - Thanks Travis Griggs.
- Fixed a rare bug that could cause the consumer to get stuck if the connection unexpectedly dies - Thanks Connor Wolf.

Version 2.1.3
-------------
- Fixed a potential recursion error in Connection.close.

Version 2.1.1
-------------
- Reduced default TCP Timeout from 30s to 10s.
- Connection Open/Close timeout is now three times the value of TCP Timeout.
- Connection will now wait for a response from the remote server before closing.

Version 2.1.0
-------------
- [Experimental] Added support for the RabbitMQ Management Api.
    - Documentation https://www.amqpstorm.io/#management-api-documentation
    - Examples https://github.com/eandersson/amqpstorm/tree/master/examples/management_api

- Connection/Channel function check_for_errors now behave more consistently.

Version 2.0.0
-------------
- Messages are now delivered as Message objects by default.
    - to_tuple and to_dict are now set to False by default.

        This is a breaking change that affects the following function:

            - channel.process_data_events
            - channel.start_consuming
            - channel.basic.get

Credits
=======
Special thanks to gmr (Gavin M. Roy) for creating pamqp, and in addition amqpstorm is heavily influenced by his pika and rabbitpy libraries.

.. |Version| image:: https://badge.fury.io/py/amqpstorm.svg?
   :target: http://badge.fury.io/py/amqpstorm

.. |CodeClimate| image:: https://codeclimate.com/github/eandersson/amqpstorm/badges/gpa.svg
   :target: https://codeclimate.com/github/eandersson/amqpstorm

.. |Travis| image:: https://travis-ci.org/eandersson/amqpstorm.svg
   :target: https://travis-ci.org/eandersson/amqpstorm

.. |Coverage| image:: https://codecov.io/gh/eandersson/amqpstorm/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/eandersson/amqpstorm
