AMQPStorm
=========
Thread-safe Python RabbitMQ Client & Management library.

|Version| |CodeClimate| |Travis| |Coverage|

Introduction
============
AMQPStorm is a library designed to be consistent, stable and thread-safe.

- 100% Test Coverage!
- Supports Python 2.7 and Python 3.3+.
- Fully tested against Python Implementations; CPython and PyPy.

Documentation
=============

Additional documentation is available on `amqpstorm.io <https://www.amqpstorm.io>`_.

Changelog
=========

Version 2.8.5
-------------
- Fixed a potential deadlock when opening a channel with a broken connection [#97] - Thanks mehdigmira.

Version 2.8.4
-------------
- Fixed a bug in Message.create where it would mutate the properties dict [#92] - Thanks Killerama.

Version 2.8.3
-------------
- Fixed pip sdist circular dependency [#88] - Thanks Jay Hogg.
- Fixed basic.consume argument type in documentation [#86] - Thanks TechmarkDavid.

Version 2.8.2
-------------
- Retry on SSLWantReadErrors [#82] - Thanks Bernhard Thiel.
- Added getter/setter methods for Message properties expiration, message_type and user_id [#86] - Thanks Jay Hogg.

Version 2.8.1
-------------
- Cleaned up documentation.

Version 2.8.0
-------------
- Introduced a new channel function called check_for_exceptions.
- Fixed issue where publish was successful but raises an error because connection was closed [#80] - Thanks Pavol Plaskoň.
- Updated SSL handling to use the non-deprecated way of creating a SSL Connection [#79] - Thanks Carl Hörberg from CloudAMQP.
- Enabled SNI for SSL connections by default [#79] - Thanks Carl Hörberg from CloudAMQP.

Version 2.7.2
-------------
- Added ability to override client_properties [#77] - Thanks tkram01.

Version 2.7.1
-------------
- Fixed Connection close taking longer than intended when using SSL [#75]- Thanks troglas.
- Fixed an issue with closing Channels taking too long after the server initiated it.

Version 2.7.0
-------------
- Added support for passing your own ssl context [#71] - Thanks troglas.
- Improved logging verbosity on connection failures [#72] - Thanks troglas.
- Fixed occasional error message when closing a SSL connection [#68] - Thanks troglas.

Version 2.6.2
-------------
- Set default TCP Timeout to 10s on UriConnection to match Connection [#67] - Thanks josemonteiro.
- Internal RPC Timeout for Opening and Closing Connections are now set to a fixed 30s [#67] - Thanks josemonteiro.

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

Credits
=======
Special thanks to gmr (Gavin M. Roy) for creating pamqp, and in addition amqpstorm is heavily influenced by his pika and rabbitpy libraries.

.. |Version| image:: https://badge.fury.io/py/AMQPStorm.svg
  :target: https://badge.fury.io/py/AMQPStorm

.. |CodeClimate| image:: https://codeclimate.com/github/eandersson/amqpstorm/badges/gpa.svg
  :target: https://codeclimate.com/github/eandersson/amqpstorm

.. |Travis| image:: https://travis-ci.org/eandersson/amqpstorm.svg
  :target: https://travis-ci.org/eandersson/amqpstorm

.. |Coverage| image:: https://codecov.io/gh/eandersson/amqpstorm/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/eandersson/amqpstorm
