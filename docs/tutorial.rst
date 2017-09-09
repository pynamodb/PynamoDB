Basic Tutorial
==============

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
DynamoDB. I quickly realized that my go to library, `dynamodb-mapper <https://dynamodb-mapper.readthedocs.io/en/latest/>`__, didn't support them.
In fact, it won't be supporting them anytime soon because dynamodb-mapper relies on another
library, `boto.dynamodb <http://docs.pythonboto.org/en/latest/migrations/dynamodb_v1_to_v2.html>`__,
which itself won't support them. In fact, boto doesn't support
Python 3 either. If you want to know more, `I blogged about it <http://jlafon.io/pynamodb.html>`__.

Installation
^^^^^^^^^^^^

::

    $ pip install pynamodb


Don't have pip? `Here are instructions for installing pip. <https://pip.readthedocs.io/en/latest/installing.html>`_.

Getting Started
^^^^^^^^^^^^^^^

PynamoDB provides three API levels, a ``Connection``, a ``TableConnection``, and a ``Model``.
Each API is built on top of the previous, and adds higher level features. Each API level is
fully featured, and can be used directly. Before you begin, you should already have an
`Amazon Web Services account <http://aws.amazon.com/>`__, and have your
`AWS credentials configured your boto <https://boto.readthedocs.io/en/latest/boto_config_tut.html>`__.

Defining a Model
----------------

The most powerful feature of PynamoDB is the ``Model`` API. You start using it by defining a model
class that inherits from ``pynamodb.models.Model``. Then, you add attributes to the model that
inherit from ``pynamodb.attributes.Attribute``. The most common attributes have already been defined for you.

Here is an example, using the same table structure as shown in `Amazon's DynamoDB Thread example <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleTablesAndData.html>`__.

.. note::

    The table that your model represents must exist before you can use it. It can be created in this example
    by calling `Thread.create_table(...)`. Any other operation on a non existent table will cause a `TableDoesNotExist`
    exception to be raised.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import (
        UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute
    )


    class Thread(Model):
        class Meta:
            table_name = 'Thread'

        forum_name = UnicodeAttribute(hash_key=True)
        subject = UnicodeAttribute(range_key=True)
        views = NumberAttribute(default=0)
        replies = NumberAttribute(default=0)
        answered = NumberAttribute(default=0)
        tags = UnicodeSetAttribute()
        last_post_datetime = UTCDateTimeAttribute()

All DynamoDB tables have a hash key, and you must specify which attribute is the hash key for each ``Model`` you define.
The ``forum_name`` attribute in this example is specified as the hash key for this table with the ``hash_key`` argument;
similarly the ``subject`` attribute is specified as the range key with the ``range_key`` argument.

Model Settings
--------------

The ``Meta`` class is required with at least the ``table_name`` class attribute to tell the model which DynamoDB table to use -
``Meta`` can be used to configure the model in other ways too. You can specify which DynamoDB region to use with the  ``region``,
and the URL endpoint for DynamoDB can be specified using the  ``host`` attribute. You can also specify the table's read and write
capacity by adding ``read_capacity_units`` and ``write_capacity_units`` attributes.

Here is an example that specifies both the ``host`` and the ``region`` to use:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute


    class Thread(Model):
        class Meta:
            table_name = 'Thread'
            # Specifies the region
            region = 'us-west-1'
            # Specifies the hostname
            host = 'http://localhost'
            # Specifies the write capacity
            write_capacity_units = 10
            # Specifies the read capacity
            read_capacity_units = 10
        forum_name = UnicodeAttribute(hash_key=True)

Defining Model Attributes
-------------------------

A ``Model`` has attributes, which are mapped to attributes in DynamoDB. Attributes are responsible for serializing/deserializing
values to a format that DynamoDB accepts, optionally specifying whether or not an attribute may be empty using the `null` argument,
and optionally specifying a default value with the `default` argument. You can specify a default value for any field, and ``default``
can even be a function.

.. note::

    `DynamoDB will not store empty attributes <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html>`_.
    By default, an ``Attribute`` cannot be ``None`` unless you specify ``null=True`` in the
    attribute constructor.

DynamoDB attributes can't be null and set attributes can't be empty.
PynamoDB attempts to do the right thing by pruning null attributes when serializing an item to be put into DynamoDB.
By default, PynamoDB attributes can't be null either - but you can easily override that by adding ``null=True`` to the constructor of the attribute.
When you make an attribute nullable, PynamoDB will omit that value if the value is ``None`` when saving to DynamoDB.
It is not recommended to give every attribute a value if those values can represent null, as those values representing null take up space - which literally costs you money
(DynamoDB pricing is based on reads and writes per second per KB).
Instead, treat the absence of a value as equivalent to being null (which is what PynamoDB does).
The only exception of course, are hash and range keys which must always have a value.

Here is an example of an attribute with a default value:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute


    class Thread(Model):
        class Meta:
            table_name = 'Thread'
        forum_name = UnicodeAttribute(hash_key=True, default='My Default Value')

Here is an example of an attribute with a default *callable* value:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute

    def my_default_value():
        return 'My default value'

    class Thread(Model):
        class Meta:
            table_name = 'Thread'
        forum_name = UnicodeAttribute(hash_key=True, default=my_default_value)

Here is an example of an attribute that can be empty:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute

    class Thread(Model):
        class Meta:
            table_name = 'Thread'
        forum_name = UnicodeAttribute(hash_key=True)
        my_nullable_attribute = UnicodeAttribute(null=True)

By default, PynamoDB assumes that the attribute name used on a Model has the same
name in DynamoDB. For example, if you define a `UnicodeAttribute` called 'username' then
PynamoDB will use 'username' as the field name for that attribute when interacting with DynamoDB.
If you wish to have custom attribute names, they can be overidden. One such use case is the ability to
use human readable attribute names in PynamoDB that are stored in DynamoDB using shorter, terse attribute
to save space.

Here is an example of customizing an attribute name:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute

    class Thread(Model):
        class Meta:
            table_name = 'Thread'
        forum_name = UnicodeAttribute(hash_key=True)
        # This attribute will be called 'tn' in DynamoDB
        thread_name = UnicodeAttribute(null=True, attr_name='tn')


PynamoDB comes with several built in attribute types for convenience, which include the following:

* :py:class:`UnicodeAttribute <pynamodb.attributes.UnicodeAttribute>`
* :py:class:`UnicodeSetAttribute <pynamodb.attributes.UnicodeSetAttribute>`
* :py:class:`NumberAttribute <pynamodb.attributes.NumberAttribute>`
* :py:class:`NumberSetAttribute <pynamodb.attributes.NumberSetAttribute>`
* :py:class:`BinaryAttribute <pynamodb.attributes.BinaryAttribute>`
* :py:class:`BinarySetAttribute <pynamodb.attributes.BinarySetAttribute>`
* :py:class:`UTCDateTimeAttribute <pynamodb.attributes.UTCDateTimeAttribute>`
* :py:class:`BooleanAttribute <pynamodb.attributes.BooleanAttribute>`
* :py:class:`JSONAttribute <pynamodb.attributes.JSONAttribute>`

All of these built in attributes handle serializing and deserializng themselves, in both Python 2 and Python 3.

Creating the table
------------------

If your table doesn't already exist, you will have to create it. This can be done with easily:

.. code-block:: python

    >>> if not Thread.exists():
            Thread.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

The ``wait`` argument tells PynamoDB to wait until the table is ready for use before returning.


Deleting a table
----------------

Deleting is made quite simple when using a `Model`:

.. code-block:: python

    >>> Thread.delete_table()

Using the Model
^^^^^^^^^^^^^^^

Now that you've defined a model (referring to the example above), you can start interacting with
your DynamoDB table. You can create a new `Thread` item by calling the `Thread` constructor.

Creating Items
--------------
.. code-block:: python

    >>> thread_item = Thread('forum_name', 'forum_subject')

The first two arguments are automatically assigned to the item's hash and range keys. You can
specify attributes during construction as well:

.. code-block:: python

    >>> thread_item = Thread('forum_name', 'forum_subject', replies=10)

The item won't be added to your DynamoDB table until you call save:

.. code-block:: python

    >>> thread_item.save()

If you want to retrieve an item that already exists in your table, you can do that with `get`:

.. code-block:: python

    >>> thread_item = Thread.get('forum_name', 'forum_subject')

If the item doesn't exist, `Thread.DoesNotExist` will be raised.

Updating Items
--------------

You can update an item with the latest data from your table:

.. code-block:: python

    >>> thread_item.refresh()

Updates to table items are supported too, even atomic updates. Here is an example of
atomically updating the view count of an item + updating the value of the last post.

.. code-block:: python

    >>> thread_item.update(actions=[
            Thread.views.set(Thread.views + 1),
            Thread.last_post_datetime.set(datetime.now()),
        ])


.. deprecated:: 2.0

    :func:`update_item` is replaced with :func:`update`


.. code-block:: python

    >>> thread_item.update_item('views', 1, action='add')

