AMQPStorm
=========
Thread-safe Python RabbitMQ Client & Management library.

|Version| |CodeClimate| |Travis| |Coverage|

Introduction
============
AMQPStorm is a library designed to be consistent, stable and thread-safe.

- 100% Test Coverage!
- Supports Python 2.7 and Python 3.3+.
- Fully tested against Python Implementations; CPython, PyPy and Pyston.

Documentation
=============

Additional documentation is available on `amqpstorm.io <https://www.amqpstorm.io>`_.

Changelog
=========

Version 2.6.1
-------------
- Fixed minor issue with the last channel id not being available.

Version 2.6.0
-------------
- Re-use closed channel ids [#55] - Thanks mikemrm.
- Changed Poller Timeout to be a constant.
- Improved Connection Close performance.
- Channels is now a publicly available variable in Connections.

Version 2.5.0
-------------
- Upgraded pamqp to v2.0.0.
    - Python 3 keys will now always be of type str.
    - For more information see https://pamqp.readthedocs.io/en/latest/history.html
- Properly wait until the inbound queue is empty when break_on_empty is set [#63] - Thanks TomGudman.
- Fixed issue with Management queue/exchange declare when the passive flag was set to True.

Version 2.4.2
-------------
- Added support for External Authentication - Thanks Bernd Höhl.
- Fixed typo in setup.py extra requirements - Thanks Bernd Höhl.
- LICENSE file now included in package - Thanks Tomáš Chvátal.

Version 2.4.1
-------------
- Added client/server negotiation to better determine the maximum supported channels and maximum allowed frame size [#52] - Thanks gastlich.
- We now raise an exception if the maximum allowed channel count is reached.

Version 2.4.0
-------------
- basic.consume now allows for multiple callbacks [#48].

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
