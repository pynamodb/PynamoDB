Signals
=======
Starting with Pynamo 4.0.0, there is support for signalling in Pynamo.  This support is provided by the excellent `blinker`_ library and will gracefully fall back if it is not available. `blinker`_  is not installed by default, as we didn't want to additional overhead to network calls if it wasn't needed.

Signals allow certain senders to notify subscribers that something happened. PynamoDB only sends signals before and after every DynamoDB API call.

We purposely only send the operation name and the table name to the callback, as we want to prevent users from doing business logic in the signal. Keep in mind that signals are intended to notify subscribers. Subscribers should not try to modify data or do database calls within the callback. Also, adding too much logic in the callback will degrade the performance of your database writes.


Subscribing to Signals
----------------------

We fire `pre_dynamo_send` before the network call and `post_dynamo_send` after the network call to DynmaoDB.

The callback must taking the following arguments:

================  ===========
Arguments         Description
================  ===========
*sender*          The object that fired that method.
*operation_name*  The string name of the `Dynamo action`_
*table_name*      The name of the table the operation is called upon.
*req_uuid*        A unique identifer so subscribers can correlate the before and after events.
================  ===========

To subscribe to a signal, just need to import the signal object and connect your callback, like so.

.. code:: python
    from pynamodb.connection.signals import pre_dynamo_send, post_dynamo_send

    def record_pre_dynamo_send(sender, operation_name, table_name, req_uuid):
        pre_recorded.append((operation_name, table_name, req_uuid))

    def record_post_dynamo_send(sender, operation_name, table_name, req_uuid):
        post_recorded.append((operation_name, table_name, req_uuid))

    pre_dynamo_send.connect(record_pre_dynamo_send)
    post_dynamo_send.connect(record_post_dynamo_send)
 
.. _blinker: https://pypi.python.org/pypi/blinker
.. _Dynamo action: https://github.com/pynamodb/PynamoDB/blob/cd705cc4e0e3dd365c7e0773f6bc02fe071a0631/
