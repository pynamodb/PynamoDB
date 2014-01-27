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


A Pythonic interface for `DynamoDB <http://aws.amazon.com/dynamodb/>`_.

A rich API that is compatible with Python 2 and Python 3.

See documentation at http://pynamodb.readthedocs.org/

Installation::

    $ pip install pynamodb

Features
========

* Python 2 support
* Python 3 support
* Fully tested
* Includes the entire DynamoDB API
* Supports both unicode and binary DynamoDB attributes
* Support for global secondary indexes, local secondary indexes, and batch operations
* Provides iterators for working with queries, scans, that are automatically paginated
* Automatic pagination for bulk operations
* Complex queries
