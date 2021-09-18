========
PynamoDB
========

.. image:: https://img.shields.io/pypi/v/pynamodb.svg
    :target: https://pypi.python.org/pypi/pynamodb/
.. image:: https://img.shields.io/conda/vn/conda-forge/pynamodb.svg
    :target: https://anaconda.org/conda-forge/pynamodb
.. image:: https://github.com/pynamodb/PynamoDB/workflows/Tests/badge.svg
    :target: https://github.com/pynamodb/PynamoDB/actions
.. image:: https://img.shields.io/coveralls/pynamodb/PynamoDB/master.svg
    :target: https://coveralls.io/r/pynamodb/PynamoDB

A Pythonic interface for Amazon's `DynamoDB <http://aws.amazon.com/dynamodb/>`_.

DynamoDB is a great NoSQL service provided by Amazon, but the API is verbose.
PynamoDB presents you with a simple, elegant API.

Useful links:

* See the full documentation at https://pynamodb.readthedocs.io/
* Ask questions in the `GitHub issues <https://github.com/pynamodb/PynamoDB/issues>`_
* See release notes at https://pynamodb.readthedocs.io/en/latest/release_notes.html

Installation
============
From PyPi::

    $ pip install pynamodb

From GitHub::

    $ pip install git+https://github.com/pynamodb/PynamoDB#egg=pynamodb

From conda-forge::

    $ conda install -c conda-forge pynamodb


Basic Usage
===========

Create a model that describes your DynamoDB table.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute

    class UserModel(Model):
        """
        A DynamoDB User
        """
        class Meta:
            table_name = "dynamodb-user"
        email = UnicodeAttribute(null=True)
        first_name = UnicodeAttribute(range_key=True)
        last_name = UnicodeAttribute(hash_key=True)

PynamoDB allows you to create the table if needed (it must exist before you can use it!):

.. code-block:: python

    UserModel.create_table(read_capacity_units=1, write_capacity_units=1)

Create a new user:

.. code-block:: python

    user = UserModel("John", "Denver")
    user.email = "djohn@company.org"
    user.save()

Now, search your table for all users with a last name of 'Denver' and whose
first name begins with 'J':

.. code-block:: python

    for user in UserModel.query("Denver", UserModel.first_name.startswith("J")):
        print(user.first_name)

Examples of ways to query your table with filter conditions:

.. code-block:: python

    for user in UserModel.query("Denver", UserModel.email=="djohn@company.org"):
        print(user.first_name)

Retrieve an existing user:

.. code-block:: python

    try:
        user = UserModel.get("John", "Denver")
        print(user)
    except UserModel.DoesNotExist:
        print("User does not exist")

Upgrade Warning
===============

The behavior of 'UnicodeSetAttribute' has changed in backwards-incompatible ways
as of the 1.6.0 and 3.0.1 releases of PynamoDB.

See `UnicodeSetAttribute upgrade docs <https://pynamodb.readthedocs.io/en/latest/release_notes.html>`_
for detailed instructions on how to safely perform the upgrade.

Advanced Usage
==============

Want to use indexes? No problem:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
    from pynamodb.attributes import NumberAttribute, UnicodeAttribute

    class ViewIndex(GlobalSecondaryIndex):
        class Meta:
            read_capacity_units = 2
            write_capacity_units = 1
            projection = AllProjection()
        view = NumberAttribute(default=0, hash_key=True)

    class TestModel(Model):
        class Meta:
            table_name = "TestModel"
        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        view = NumberAttribute(default=0)
        view_index = ViewIndex()

Now query the index for all items with 0 views:

.. code-block:: python

    for item in TestModel.view_index.query(0):
        print("Item queried from index: {0}".format(item))

It's really that simple.


Want to use DynamoDB local? Just add a ``host`` name attribute and specify your local server.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute

    class UserModel(Model):
        """
        A DynamoDB User
        """
        class Meta:
            table_name = "dynamodb-user"
            host = "http://localhost:8000"
        email = UnicodeAttribute(null=True)
        first_name = UnicodeAttribute(range_key=True)
        last_name = UnicodeAttribute(hash_key=True)

Want to enable streams on a table? Just add a ``stream_view_type`` name attribute and specify
the type of data you'd like to stream.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute
    from pynamodb.constants import STREAM_NEW_AND_OLD_IMAGE

    class AnimalModel(Model):
        """
        A DynamoDB Animal
        """
        class Meta:
            table_name = "dynamodb-user"
            host = "http://localhost:8000"
            stream_view_type = STREAM_NEW_AND_OLD_IMAGE
        type = UnicodeAttribute(null=True)
        name = UnicodeAttribute(range_key=True)
        id = UnicodeAttribute(hash_key=True)

Features
========

* Python >= 3.6 support
* An ORM-like interface with query and scan filters
* Compatible with DynamoDB Local
* Supports the entire DynamoDB API
* Support for Unicode, Binary, JSON, Number, Set, and UTC Datetime attributes
* Support for Global and Local Secondary Indexes
* Provides iterators for working with queries, scans, that are automatically paginated
* Automatic pagination for bulk operations
* Complex queries
* Batch operations with automatic pagination
* Iterators for working with Query and Scan operations
