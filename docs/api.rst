PynamoDB Tutorial
===================

PynamoDB is attempt to be a Pythonic interface to DynamoDB that supports all of DynamoDB's
powerful features in *both* Python 3, and Python 2. This includes support for unicode and
binary attributes.

But why stop there? PynamoDB also supports:

* Sets for Binary, Number, and Unicode attributes
* Automatic pagination for bulk operations
* Global secondary indexes
* Local secondary indexes
* Complex queries

Why PynamoDB?
^^^^^^^^^^^^^

It all started when I needed to use Global Secondary Indexes, a new and powerful feature of
DynamoDB. I quickly realized that my go to library, `dynamodb-mapper <http://dynamodb-mapper.readthedocs.org/en/latest/>`__, didn't support them.
In fact, it won't be supporting them anytime soon because dynamodb-mapper relies on another
library, `boto.dynamodb <http://docs.pythonboto.org/en/latest/migrations/dynamodb_v1_to_v2.html>`__,
which itself won't support them. In fact, boto doesn't support
Python 3 either.

Installation
^^^^^^^^^^^^

::

    $ pip install pynamodb

Getting Started
^^^^^^^^^^^^^^^

PynamoDB provides three API levels, a ``Connection``, a ``TableConnection``, and a ``Model``.
Each API is built on top of the previous, and adds higher level features. Each API level is
fully featured, and can be used directly. Before you begin, you should already have an
`Amazon Web Services account <http://aws.amazon.com/>`__, and have your
`AWS credentials configured your boto <http://boto.readthedocs.org/en/latest/boto_config_tut.html>`__.

Defining a Model
----------------

The most powerful feature of PynamoDB is the ``Model`` API. You start using it by defining a model
class that inherits from ``pynamodb.models.Model``. Then, you add attributes to the model that
inherit from ``pynamodb.attributes.Attribute``. The most common attributes have already been defined for you.

Here is an example, using the same table structure as shown in `Amazon's DynamoDB Thread example <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleTablesAndData.html>`__.

::

    from pynamodb.models import Model
    from pynamodb.attributes import (
        UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute
    )


    class Thread(Model):
        table_name = 'Thread'
        forum_name = UnicodeAttribute(hash_key=True)
        subject = UnicodeAttribute(range_key=True)
        views = NumberAttribute(default=0)
        replies = NumberAttribute(default=0)
        answered = NumberAttribute(default=0)
        tags = UnicodeSetAttribute()
        last_post_datetime = UTCDateTimeAttribute()

The ``table_name`` class attribute is required, and tells the model which DynamoDB table to use. The ``forum_name`` attribute
is specified as the hash key for this table with the ``hash_key`` argument. Specifying that an attribute is a range key is done
with the ``range_key`` attribute.

Creating the table
------------------

If your table doesn't already exist, you will have to create it. This can be done with easily:

    >>> Thread.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

The ``wait`` argument tells PynamoDB to wait until the table is ready for use before returning.

Connection
----------

The `Connection`  API provides a Pythonic wrapper over the Amazon DynamoDB API. All operations
are supported, and it provides a primitive starting point for the other two APIs.

TableConnection
---------------

The `TabelConnection` API is a small convenience layer built on the `Connection` that provides
 all of the DynamoDB API for a given table.

Model
-----

This is where the fun begins, with bulk operations, query filters, context managers, and automatic
attribute binding. The `Model` class provides very high level features for interacting with DynamoDB.

