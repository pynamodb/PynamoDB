Settings
========

Settings reference
~~~~~~~~~~~~~~~~~~


Here is a complete list of settings which control default PynamoDB behavior.


request_timeout_seconds
-----------------------

Default: ``60``

The default timeout for HTTP requests in seconds.


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


session_cls
-----------

Default: ``botocore.vendored.requests.Session``

A class which implements the Session_ interface from requests, used for making API requests
to DynamoDB.

.. _Session: http://docs.python-requests.org/en/master/api/#request-sessions

allow_rate_limited_scan_without_consumed_capacity
-------------------------------------------------

Default: ``False``

If ``True``, ``rate_limited_scan()`` will proceed silently (without
rate limiting) if the DynamoDB server does not return consumed
capacity information in responses.

Overriding settings
~~~~~~~~~~~~~~~~~~~

Default settings may be overridden by providing a Python module which exports the desired new values.
Set the ``PYNAMODB_CONFIG`` environment variable to an absolute path to this module or write it to
``/etc/pynamodb/global_default_settings.py`` to have it automatically discovered.

See an example of specifying a custom ``session_cls`` to configure the connection pool below.

.. code-block:: python

    from botocore.vendored import requests
    from botocore.vendored.requests import adapters

    class CustomPynamoSession(requests.Session):
        super(CustomPynamoSession, self).__init__()
        self.mount('http://', adapters.HTTPAdapter(pool_maxsize=100))

    session_cls = CustomPynamoSession
