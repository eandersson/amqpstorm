##### Changelog
###### Version 1.1.2
- Fixed typo in _close_socket exception handling possibly causing an unexpected exception when the connection is forcefully closed.

###### Version 1.1.1
- Fixed a bug with the Consumer callback not being accepted in very specific scenarios.
- Minor improvements to the error handling and cleaned up shutdown process.

###### Version 1.1.0
- Python 3 Support.
- Added support for Connection.Blocked and Connection.Unblocked.
- Improved Rpc Error Handling.