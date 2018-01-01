Changelog
=========

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
