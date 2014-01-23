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
DynamoDB. I quickly realized that my go to library, dynamodb-mapper, didn't support them.
In fact, it won't be supporting them anytime soon because dynamodb-mapper relies on another
library, boto.dynamodb, which itself won't support them. In fact, boto doesn't support
Python 3 either.

Installation
^^^^^^^^^^^^

::

    $ pip install pynamodb

PynamoDB Layers
^^^^^^^^^^^^^^^

PynamoDB provides three API levels, a `Connection`, a `TableConnection`, and a `Model`.
Each API is built on top of the previous, and adds higher level features.

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

