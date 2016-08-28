Changelog
-------------

Version 2.1.0
-------------
- Added Management Api.
    A complete Management Api that can be used for Testing or DevOps tasks.

- Connection and Channel function check_for_errors now behave more consistently.

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