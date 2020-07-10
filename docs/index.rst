.. amqpstorm documentation master file, created by
   sphinx-quickstart on Sun Apr 10 16:25:24 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

AMQPStorm Documentation
=======================
Thread-safe Python RabbitMQ Client & Management library.

Installation
------------
The latest version can be installed using `pip <https://pip.pypa.io/en/stable/quickstart/>`_ and is available at pypi `here <https://pypi.org/project/AMQPStorm/>`_
::

    pip install amqpstorm

Basic Example
-------------

::

   with amqpstorm.Connection('rmq.amqpstorm.io', 'guest', 'guest') as connection:
       with connection.channel() as channel:
           channel.queue.declare('fruits')
           message = amqpstorm.Message.create(
               channel, body='Hello RabbitMQ!', properties={
                   'content_type': 'text/plain'
               })
           message.publish('fruits')

Additional Examples
-------------------

A wide verity of examples are available on Github at `here <https://github.com/eandersson/amqpstorm/tree/master/examples>`_

.. toctree::
   :caption: Usage
   :name: usage

   usage/connection
   usage/channel
   usage/exceptions
   usage/message

.. toctree::
   :caption: Management API Usage
   :name: api_usage

   api_usage/api
   api_usage/exception

.. toctree::
   :glob:
   :caption: Examples
   :name: examples

   examples/*

Issues
------
Please report any issues on Github `here <https://github.com/eandersson/amqpstorm/issues>`_

Source
------
AMQPStorm source code is available on Github `here <https://github.com/eandersson/amqpstorm>`_

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`