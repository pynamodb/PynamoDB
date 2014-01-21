========
pynamodb
========

A Pythonic interface for DynamoDB

A rich API that is compatible with Python 2 and Python 3.

Features
========

* Python 2 support
* Python 3 support
* Fully tested
* All DynamoDB operations are supported
* Support for Global Secondary Indexes, Batch operations
* Generators for working with queries, scans, that are automatically paginated
* Relies on botocore, not boto
* Flexible

API Levels
==========

* High level Model API for working with objects
* Table API for working with tables
* Low level API, as Pythonic interface to the DynamoDB REST API.

Why?
====

It all started when I needed to use Global Secondary Indexes, but realized that dynamodb-mapper
didn't support them. In fact, it won't be supporting them anytime soon because dynamodb-mapper
relies on boto.dynamodb, which itself won't support them. In fact, boto doesn't support
Python 3 either.