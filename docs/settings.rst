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


host
------

Default: automatically constructed by boto to account for region

The URL endpoint for DynamoDB. This can be used to use a local implementation of DynamoDB such as DynamoDB Local or dynalite.


retry_configuration
-------------------

Default: ``"LEGACY"``

This controls the PynamoDB retry behavior. The default of ``"LEGACY"`` keeps the
existing PynamoDB retry behavior. If set to ``None``, this will use botocore's default
retry configuration discovery mechanism as documented
`in boto3 <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#retries>`_
and
`in the AWS SDK docs <https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html>`_.
If set to a retry configuration dictionary as described
`here <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#defining-a-retry-configuration-in-a-config-object-for-your-boto3-client>`_
it will be used directly in the botocore client configuration.

Overriding settings
~~~~~~~~~~~~~~~~~~~

Default settings may be overridden by providing a Python module which exports the desired new values.
Set the ``PYNAMODB_CONFIG`` environment variable to an absolute path to this module or write it to
``/etc/pynamodb/global_default_settings.py`` to have it automatically discovered.

