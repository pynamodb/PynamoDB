PynamoDB Index Queries
======================

DynamoDB supports two types of indexes: global secondary indexes, and local secondary indexes.
Indexes can make accessing your data more efficient, and should be used when appropriate. See
`the documentation for more information <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html>`__.

Defining an index
^^^^^^^^^^^^^^^^^

Indexes are defined as classes, just like models. Here is a simple index class:

::

    class ViewIndex(GlobalSecondaryIndex):
        """
        This class represents a global secondary index
        """
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

* ``AllProjection``: All attributes are projected.
* ``KeysOnlyProjection``: Only the index and primary keys are projected.
* ``IncludeProjection(attributes)``: Only the specified ``attributes`` are projected.

We still need to attach the index to the model in order for us to use it. You define it as
a class attribute on the model, as in this example::

    class TestModel(Model):
        """
        A test model that uses a global secondary index
        """
        table_name = 'TestModel'
        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        view_index = ViewIndex()
        view = NumberAttribute(default=0)

Querying an index
^^^^^^^^^^^^^^^^^^

Index queries use the same syntax as model queries. Continuing our example, we can query
the ``view_index`` simply by calling ``query``::

    for item in TestModel.view_index.query(1):
        print("Item queried from index: {0}".format(item))

This example queries items from the table using the global secondary index, called ``view_index``, using
a hash key value of 1 for the index. This would return all ``TestModel`` items that have a ``view`` attribute
of value 1.
