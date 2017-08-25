Index Queries
======================

DynamoDB supports two types of indexes: global secondary indexes, and local secondary indexes.
Indexes can make accessing your data more efficient, and should be used when appropriate. See
`the documentation for more information <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html>`__.

Index Settings
^^^^^^^^^^^^^^

The ``Meta`` class is required with at least the ``projection`` class attribute to specify the projection type. For Global secondary indexes,
the ``read_capacity_units`` and ``write_capacity_units`` also need to be provided. By default, PynamoDB will use the class attribute
name that you provide on the model as the ``index_name`` used when making requests to the DynamoDB API. You can override the default
name by providing the ``index_name`` class attribute in the ``Meta`` class of the index.


Global Secondary Indexes
^^^^^^^^^^^^^^^^^^^^^^^^

Indexes are defined as classes, just like models. Here is a simple index class:

.. code-block:: python

    from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
    from pynamodb.attributes import NumberAttribute


    class ViewIndex(GlobalSecondaryIndex):
        """
        This class represents a global secondary index
        """
        class Meta:
            # index_name is optional, but can be provided to override the default name
            index_name = 'foo-index'
            read_capacity_units = 2
            write_capacity_units = 1
            # All attributes are projected
            projection = AllProjection()

        # This attribute is the hash key for the index
        # Note that this attribute must also exist
        # in the model
        view = NumberAttribute(default=0, hash_key=True)


Global indexes require you to specify the read and write capacity, as we have done
in this example. Indexes are said to *project* attributes from the main table into the index.
As such, there are three styles of projection in DynamoDB, and PynamoDB provides three corresponding
projection classes.

* :py:class:`AllProjection <pynamodb.indexes.AllProjection>`: All attributes are projected.
* :py:class:`KeysOnlyProjection <pynamodb.indexes.KeysOnlyProjection>`: Only the index and primary keys are projected.
* :py:class:`IncludeProjection(attributes) <pynamodb.indexes.IncludeProjection>`: Only the specified ``attributes`` are projected.

We still need to attach the index to the model in order for us to use it. You define it as
a class attribute on the model, as in this example:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute


    class TestModel(Model):
        """
        A test model that uses a global secondary index
        """
        class Meta:
            table_name = 'TestModel'
        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        view_index = ViewIndex()
        view = NumberAttribute(default=0)


Local Secondary Indexes
^^^^^^^^^^^^^^^^^^^^^^^

Local secondary indexes are defined just like global ones, but they inherit from ``LocalSecondaryIndex`` instead:

.. code-block:: python

    from pynamodb.indexes import LocalSecondaryIndex, AllProjection
    from pynamodb.attributes import NumberAttribute


    class ViewIndex(LocalSecondaryIndex):
        """
        This class represents a local secondary index
        """
        class Meta:
            # All attributes are projected
            projection = AllProjection()
        forum = UnicodeAttribute(hash_key=True)
        view = NumberAttribute(range_key=True)


You must specify the same hash key on the local secondary index and the model. The range key can be any attribute.


Querying an index
^^^^^^^^^^^^^^^^^^

Index queries use the same syntax as model queries. Continuing our example, we can query
the ``view_index``  global secondary index simply by calling ``query``:

.. code-block:: python

    for item in TestModel.view_index.query(1):
        print("Item queried from index: {0}".format(item))

This example queries items from the table using the global secondary index, called ``view_index``, using
a hash key value of 1 for the index. This would return all ``TestModel`` items that have a ``view`` attribute
of value 1.

Local secondary index queries have a similar syntax. They require a hash key, and can include conditions on the
range key of the index. Here is an example that queries the index for values of ``view`` greater than zero:

.. code-block:: python

    for item in TestModel.view_index.query('foo', TestModel.view > 0):
        print("Item queried from index: {0}".format(item.view))
