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

Version 2.4.1
-------------
- Added client/server negotiation to better determine the maximum supported channels and maximum allowed frame size [#52] - Thanks gastlich.
- We now raise an exception if the maximum allowed channel count is reached.

Version 2.4.0
-------------
- basic.consume now allows for multiple callbacks [#48].

Version 2.3.0
-------------
- Added delivery_tag property to message.
- Added redelivered property to message [#41] - Thanks tkram01.
- Added support for Management Api Healthchecks [#39] - Thanks Julien Carpentier.
- Fixed incompatibility with Sun Solaris 10 [#46] - Thanks Giuliox.
- Fixed delivery_tag being set to None by default [#47] - tkram01.
- Exposed requests verify and certs flags to Management Api [#40] - Thanks Julien Carpentier.

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
