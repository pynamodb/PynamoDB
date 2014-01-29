========
PynamoDB
========

.. image:: https://pypip.in/v/pynamodb/badge.png
:target: https://pypi.python.org/pypi/pynamodb/
    :alt: Latest Version
.. image:: https://travis-ci.org/jlafon/PynamoDB.png?branch=devel
:target: https://travis-ci.org/jlafon/PynamoDB
.. image:: https://coveralls.io/repos/jlafon/PynamoDB/badge.png?branch=devel
:target: https://coveralls.io/r/jlafon/PynamoDB
.. image:: https://pypip.in/wheel/pynamodb/badge.png
:target: https://pypi.python.org/pypi/pynamodb/
.. image:: https://pypip.in/license/pynamodb/badge.png
:target: https://pypi.python.org/pypi/pynamodb/

A Pythonic interface for Amazon's `DynamoDB <http://aws.amazon.com/dynamodb/>`_ that supports
Python 2 and 3.

DynamoDB is a great NoSQL service provided by Amazon, but the API is verbose.
PynamoDB presents you with a simple, elegant API.

See documentation at http://pynamodb.readthedocs.org/

Basic Usage
^^^^^^^^^^^

Create a model that describes your DynamoDB table.
::

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute

    class UserModel(Model):
            """
            A DynamoDB User
            """
            table_name = 'dynamodb-user'
            email = UnicodeAttribute(null=True)
            first_name = UnicodeAttribute(range_key=True)
            last_name = UnicodeAttribute(hash_key=True)

Now, search your table for all users with a last name of 'Smith' and whose
first name begins with 'J':
::

    for user in UserModel.query('Smith', first_name__begins_with='J'):
        print(user.first_name)

Create a new user::

    user = UserModel('John', 'Denver')
    user.save()

Advanced Usage
^^^^^^^^^^^^^^

Wan't to use indexes? No problem::

    from pynamodb.models import Model
    from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
    from pynamodb.attributes import NumberAttribute, UnicodeAttribute

    class ViewIndex(GlobalSecondaryIndex):
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()
        view = NumberAttribute(default=0, hash_key=True)

    class TestModel(Model):
        table_name = 'TestModel'
        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        view = NumberAttribute(default=0)
        view_index = ViewIndex()

Now query the index for all items with 0 views::

    for item in TestModel.view_index.query(0):
        print("Item queried from index: {0}".format(item))

It's really that simple.

Installation::

    $ pip install pynamodb

or install the development version::

    $ pip install git+https://github.com/jlafon/PynamoDB#egg=pynamodb

Features
========

* Python 3 support
* Python 2 support
* An ORM-like interface with query and scan filters
* Includes the entire DynamoDB API
* Supports both unicode and binary DynamoDB attributes
* Support for global secondary indexes, local secondary indexes, and batch operations
* Provides iterators for working with queries, scans, that are automatically paginated
* Automatic pagination for bulk operations
* Complex queries
