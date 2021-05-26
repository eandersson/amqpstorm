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
- Fixed issue where a publish was successful but raised an error because connection was closed [#80] - Thanks Pavol Plaskoň.
- Updated TLS handling to use the non-deprecated way of creating a TLS Connection [#79] - Thanks Carl Hörberg from CloudAMQP.
- Enabled SNI for TLS connections by default [#79] - Thanks Carl Hörberg from CloudAMQP.

Version 2.7.2
-------------
- Added ability to override client_properties [#77] - Thanks tkram01.

Version 2.7.1
-------------
- Fixed Connection close taking longer than intended when using SSL [#75] - Thanks troglas.
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

Version 2.4.2
-------------
- Added support for External Authentication - Thanks Bernd Höhl.
- Fixed typo in setup.py extra requirements - Thanks Bernd Höhl.
- LICENSE file now included in package - Thanks Tomáš Chvátal.

Version 2.4.1
-------------
- Added client/server negotiation to better determine the maximum supported channels and frame size [#52] - Thanks gastlich.
- We now raise an exception if the maximum allowed channel count is ever reached.

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
    - Documentation https://amqpstorm.readthedocs.io/en/latest/#management-api-documentation
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

Version 1.5.0
-------------
- Added support for Channel.Tx (Server local transactions). [#27]
- Added support for Heartbeat interval 0 (disabled). [#26]
- Added Python implementation to platform string, e.g. Python 2.7.0 (Jython).
- Fixed Jython bug. [#25]
- Fixed incorrect log line for the Connection and Channel Context Manager.
- Removed TCP Keepalive.

Version 1.4.1
-------------
- Heartbeats are now only sent when there is no outgoing traffic - Thanks Tom.

Version 1.4.0
-------------
- 100% Unit-test Coverage!
- All classes are now slotted.
- New improved Heartbeat Monitor.
    - If no data has been sent within the Heartbeat interval, the client will now send a Heartbeat to the server - Thanks David Schneider.
- Reduced default RPC timeout from 120s to 60s.

Version 1.3.4
-------------
- Dropped Python 3.2 Support.
- Fixed incorrect SSL warning when adding heartbeat or timeout to uri string [#18] - Thanks Adam Mills.

Version 1.3.3
-------------
- Fixed bug causing messages without a body to not be consumed properly [#16] - Thanks Adam Mills.

Version 1.3.2
-------------
- Fixed minor bug in the Poller error handling.
- Fixed issue where network corruption could caused a connection error to throw the wrong exception.

Version 1.3.1
-------------
- Fixed SSL bug that could trigger an exception when running multiple threads [#14] - Thanks Adam Mills.
- Fixed bug when using channel.basic.get to retrieve large payloads.
- Reduced default RPC timeout from 360s to 120s.

Version 1.3.0
-------------
- Removed noisy logging.
- Fixed Runtime exception caused by listener trying to join itself [#11] - Thanks ramonz.
- Channels are no longer closed after RabbitMQ throws a recoverable exception.
- Added Error mapping based on the AMQP 0.9.1 specifications (when applicable).
    Introduced three new variables to the AMQP-Storm Exceptions.
        - error_code: This provides HTTP style error codes based on the AMQP Specification.
        - error_type: This provides the full AMQP Error name; e.g. NO-ROUTE.
        - documentation: This provides the official AMQP Specification documentation string.

    These variables are available on all AMQP-Storm exceptions, but if no error code was
    provided by RabbitMQ, they will be empty.

    Usage:
        except AMQPChannelError as why:
            if why.error_code == 312:
                self.channel.queue.declare(queue_name)
