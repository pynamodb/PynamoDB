.. _settings:

Settings
========

Settings reference
~~~~~~~~~~~~~~~~~~


Here is a complete list of settings which control default PynamoDB behavior.

connect_timeout_seconds
-----------------------

Default: ``15``

The time in seconds till a ``ConnectTimeoutError`` is thrown when attempting to make a connection.


read_timeout_seconds
-----------------------

Default: ``30``

The time in seconds till a ``ReadTimeoutError`` is thrown when attempting to read from a connection.


max_retry_attempts
------------------

Default: ``3``

The number of times to retry certain failed DynamoDB API calls. The most common cases eligible for
retries include ``ProvisionedThroughputExceededException`` and ``5xx`` errors.


base_backoff_ms
---------------

Default: ``25``

The base number of milliseconds used for `exponential backoff and jitter
<https://www.awsarchitectureblog.com/2015/03/backoff.html>`_ on retries.


region
------

Default: ``"us-east-1"``

The default AWS region to connect to.


max_pool_connections
--------------------

Default: ``10``

The maximum number of connections to keep in a connection pool.


extra_headers
--------------------

Default: ``None``

A dictionary of headers that should be added to every request. This is only useful
when interfacing with DynamoDB through a proxy, where headers are stripped by the
proxy before forwarding along. Failure to strip these headers before sending to AWS
will result in an ``InvalidSignatureException`` due to request signing.


allow_rate_limited_scan_without_consumed_capacity
-------------------------------------------------

Default: ``False``

If ``True``, ``rate_limited_scan()`` will proceed silently (without
rate limiting) if the DynamoDB server does not return consumed
capacity information in responses. If ``False``, scans will fail
should the server not return consumed capacity information in an
effort to prevent unintentional capacity usage..

Overriding settings
~~~~~~~~~~~~~~~~~~~

Default settings may be overridden by providing a Python module which exports the desired new values.
Set the ``PYNAMODB_CONFIG`` environment variable to an absolute path to this module or write it to
``/etc/pynamodb/global_default_settings.py`` to have it automatically discovered.
