AMQPStorm
=========
Thread-safe Python RabbitMQ Client & Management library.

|Version| |Travis| |Coverage|

Introduction
============
AMQPStorm is a library designed to be consistent, stable and thread-safe.

- 100% Test Coverage!
- Supports Python 3.6+.

Documentation
=============

Additional documentation is available on `amqpstorm.io <https://www.amqpstorm.io>`_.

Changelog
=========

Version 2.9.0
-------------
- Added support for custom Message implementations - Thanks Jay Hogg.
- Fixed a bug with confirm_delivery not working after closing and re-opening an existing channel.
- Re-worked the channel re-use code.

.. |Version| image:: https://badge.fury.io/py/AMQPStorm.svg
  :target: https://badge.fury.io/py/AMQPStorm

.. |Travis| image:: https://travis-ci.org/eandersson/amqpstorm.svg
  :target: https://travis-ci.org/eandersson/amqpstorm

.. |Coverage| image:: https://codecov.io/gh/eandersson/amqpstorm/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/eandersson/amqpstorm
