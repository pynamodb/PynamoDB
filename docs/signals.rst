Signals
=======
Starting with PynamoDB 3.1.0, there is support for signalling.  This support is provided by the `blinker`_ library, which is not installed by default. In order to ensure blinker is installed, specify your PynamoDB requirement like so:

::

	pynamodb[signals]==<YOUR VERSION NUMBER>

Signals allow certain senders to notify subscribers that something happened. PynamoDB currently sends signals before and after every DynamoDB API call.

.. note::

    It is recommended to avoid business logic in signal callbacks, as this can have performance implications. To reinforce this, only the operation name and table name are available in the signal callback.


Subscribing to Signals
----------------------

PynamoDB fires two signal calls, `pre_dynamodb_send` before the network call and `post_dynamodb_send` after the network call to DynamoDB.

The callback must taking the following arguments:

================  ===========
Arguments         Description
================  ===========
*sender*          The object that fired that method.
*operation_name*  The string name of the `DynamoDB action`_
*table_name*      The name of the table the operation is called upon.
*req_uuid*        A unique identifer so subscribers can correlate the before and after events.
================  ===========

To subscribe to a signal, the user needs to import the signal object and connect your callback, like so.

.. code:: python

    from pynamodb.signals import pre_dynamodb_send, post_dynamodb_send

    def record_pre_dynamodb_send(sender, operation_name, table_name, req_uuid):
        pre_recorded.append((operation_name, table_name, req_uuid))

    def record_post_dynamodb_send(sender, operation_name, table_name, req_uuid):
        post_recorded.append((operation_name, table_name, req_uuid))

    pre_dynamodb_send.connect(record_pre_dynamodb_send)
    post_dynamodb_send.connect(record_post_dynamodb_send)

.. _blinker: https://pypi.python.org/pypi/blinker
.. _Dynamo action: https://github.com/pynamodb/PynamoDB/blob/cd705cc4e0e3dd365c7e0773f6bc02fe071a0631/
