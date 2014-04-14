Release Notes
=============
v1.1.0
------

:date: 2014-04-14

* Support for expected value conditions in item delete, update, and save
* Support for limit argument to queries
* Support for aliased attribute names
* Minor bug fixes

v1.0.0
------

:date: 2014-03-28

* Major update: New syntax for specifying models that is not backward compatible.

.. important::
    The syntax for models has changed!

The old way:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute


    class Thread(Model):
        table_name = 'Thread'
        forum_name = UnicodeAttribute(hash_key=True)

The new way:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute


    class Thread(Model):
        class Meta:
            table_name = 'Thread'
        forum_name = UnicodeAttribute(hash_key=True)

Other, less important changes:

* Added explicit support for specifying the server hostname in models
* Added documentation for using DynamoDB Local and dynalite
* Made examples runnable with DynamoDB Local and dynalite by default
* Added documentation for the use of ``default`` and ``null`` on model attributes
* Improved testing for index queries


v0.1.13
-------

:date: 2014-03-20

* Bug fix release. Proper handling of update_item attributes for atomic item updates, with tests. Fixes #7.

v0.1.12
-------

:date: 2014-03-18

* Added a region attribute to model classes, allowing users to specify the AWS region, per model. Fixes #6.

v0.1.11
-------

:date: 2014-02-26

* New exception behavior: Model.get and Model.refresh will now raise DoesNotExist if the item is not found in the table.
* Correctly deserialize complex key types. Fixes #3
* Correctly construct keys for tables that don't have both a hash key and a range key in batch get operations. Fixes #5
* Better PEP8 Compliance
* More tests
* Removed session and endpoint caching to avoid using stale IAM role credentials
