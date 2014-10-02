AMQP-Storm 1.1.2
-------------
Thread-safe Python AMQP Client Library based on pamqp.

Introduction
-------------
AMQP-Storm is designed to be an easy to use and thread-safe library.
- Supports Python 2.6, 2.7 and Python 3+.

##### Changelog
###### Version 1.1.2
- Fixed typo in _close_socket exception handling causing an unwanted exception.

###### Version 1.1.1
- Fixed bug with Consumer callback not being accepted when it should.
- Minor improvements to error handling and cleaned up shutdown process.

###### Version 1.1.0
- Python 3 Support.
- Added support for Connection.Blocked and Connection.Unblocked.
- Improved Rpc Error Handling.


##### Credits
Special thanks to gmr (Gavin M. Roy) for creating pamqp, and in addition amqp-storm is heavily influenced by pika and rabbitpy.
