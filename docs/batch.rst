Batch Operations
================

Batch operations are supported using context managers, and iterators. The DynamoDB API has limits for each batch operation
that it supports, but PynamoDB removes the need implement your own grouping or pagination. Instead, it handles
pagination for you automatically.


.. note::

    DynamoDB limits batch write operations to 25 `PutRequests` and `DeleteRequests` combined. `PynamoDB` automatically
    groups your writes 25 at a time for you.

Suppose that you have defined a `Thread` Model for the examples below.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import (
        UnicodeAttribute, NumberAttribute
    )


    class Thread(Model):
        class Meta:
            table_name = 'Thread'

        forum_name = UnicodeAttribute(hash_key=True)
        subject = UnicodeAttribute(range_key=True)
        views = NumberAttribute(default=0)


Batch Writes
^^^^^^^^^^^^

Here is an example using a context manager for a bulk write operation:

.. code-block:: python

    with Thread.batch_write() as batch:
        items = [TestModel('forum-{0}'.format(x), 'thread-{0}'.format(x)) for x in range(1000)]
        for item in items:
            batch.save(item)

Batch Gets
^^^^^^^^^^

Here is an example using an iterator for retrieving items in bulk:

.. code-block:: python

    item_keys = [('forum-{0}'.format(x), 'thread-{0}'.format(x)) for x in range(1000)]
    for item in Thread.batch_get(item_keys):
        print(item)

.. _filtering:

Query Filters
^^^^^^^^^^^^^

You can query items from your table using a simple syntax, similar to other Python ORMs:

.. code-block:: python

    for item in Thread.query('ForumName', subject__begins_with='mygreatprefix'):
        print("Query returned item {0}".format(item))

Query filters are translated from an ORM like syntax to DynamoDB API calls, and use
the following syntax: `attribute__operator=value`, where `attribute` is the name of an attribute
and `operator` can be one of the following:

 * eq
 * le
 * lt
 * ge
 * gt
 * begins_with
 * between

Scan Filters
^^^^^^^^^^^^

Scan filters have the same syntax as Query filters, but support different operations (a consequence of the
DynamoDB API - not PynamoDB). The supported operators are:

 * eq
 * ne
 * le
 * lt
 * gt
 * not_null
 * null
 * contains
 * not_contains
 * begins_with
 * between

You can even specify multiple filters:

.. code-block:: python

    >>> for item in Thread.scan(forum_name__begins_with='Prefix', views__gt=10):
            print(item)

Limiting results
^^^^^^^^^^^^^^^^

Both `Scan` and `Query` results can be limited to a maximum number of items using the `limit` argument.

.. code-block:: python

    for item in Thread.query('ForumName', subject__begins_with='mygreatprefix', limit=5):
        print("Query returned item {0}".format(item))
