### AMQP-Storm 1.1.0 Beta 1
Thread-safe Python AMQP Client Library based on pamqp.

Introduction
-------------
AMQP-Storm is designed to be an easy to use and thread-safe library.
- Supports Python 2.6, 2.7 and Python 3+.

Changelog
-------------

#### 1.1.0 Beta
- Python 3 Support.
- Added support for Connection.Blocked and Connection.Unblocked.
    - Added Connection.is_blocked property.
- Changed default RPC Timeout to 360s.
- Added additional information to Rpc Timeout errors.


### Credits
Special thanks to gmr (Gavin M. Roy) for creating pamqp, and in addition amqp-storm is heavily influenced by pika and rabbitpy.
