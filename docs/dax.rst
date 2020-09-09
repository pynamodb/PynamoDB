.. _dax:

Use DAX
====================

Amazon DynamoDB Accelerator (DAX) is a write-through caching service that is designed to simplify the process of adding a cache to DynamoDB tables.


.. note::

    'query' and 'scan' requests will not hit DAX due to serious consistent issues.

    Because DAX operates separately from DynamoDB, it is important that you understand the consistency models of both DAX and DynamoDB to ensure that your applications behave as you expect.
    See
    `the documentation for more information <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.consistency.html>`__.


.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute


    class Thread(Model):
        class Meta:
            table_name = "Thread"
            dax_read_endpoints = ['xxxx:8111']
            dax_write_endpoints = ['xxxx:8111']

        forum_name = UnicodeAttribute(hash_key=True)

