.. _release_notes:

Release Notes
=============

v6.1.0
------

Features:

* Add the ability to set or unset the boto retry configuration (:pr:`1271`)

    * This adds the ability to directly set the boto retry configuration dictionary, or
      to leave it unset and allow botocore to automatically discover the configuration
      from the environment or `~/.aws/config` files.
    * A new setting `retry_configuration` was added to configure this behavior.
    * The default behavior of PynamoDB in regards to configuring retries remains the
      same as before.

v6.0.2
------

Fixes:

* Fix a warning about `datetime.utcfromtimestamp` deprecation (:pr:`1261`)

v6.0.1
------

Features:

* For failed transaction, return the underlying item in :code:`cancellation_reasons[...].raw_item` (:pr:`1226`).
  This only applies when passing :code:`return_values=ALL_OLD`.

Fixes:

* Fixing (:pr:`1242`) regression to the :code:`extra_headers` feature. These headers are intended for proxies that strip them,
  so they should be excluded from the AWS signature.

v6.0.0
------

This is a major release and contains breaking changes. Please read the notes below carefully.

Breaking changes:

* :py:class:`~pynamodb.attributes.BinaryAttribute` and :py:class:`~pynamodb.attributes.BinarySetAttribute` have undergone breaking changes:

  * The attributes' internal encoding has changed. To prevent this change going unnoticed, a new required :code:`legacy_encoding` parameter was added: see :doc:`upgrading_binary` for details.
    If your codebase uses :py:class:`~pynamodb.attributes.BinaryAttribute` or :py:class:`~pynamodb.attributes.BinarySetAttribute`,
    go over the attribute declarations and mark them accordingly.
  * When using binary attributes, the return value of :meth:`~pynamodb.models.Model.serialize` will no longer be JSON-serializable
    since it will contain :code:`bytes` objects. Use :meth:`~pynamodb.attributes.AttributeContainer.to_dynamodb_dict` and :meth:`~pynamodb.attributes.AttributeContainer.to_simple_dict` for JSON-serializable mappings.
    for a safe JSON-serializable representation.

* Python 3.6 is no longer supported.
* PynamoDB no longer has a default AWS region (used to be us-east-1) (:pr:`1003`).
  If needed, update your models' `Meta` or set the `AWS_DEFAULT_REGION` environment variable.
* :py:class:`~pynamodb.models.Model`'s JSON serialization helpers were changed:

  * :code:`to_json` was renamed to :meth:`~pynamodb.attributes.AttributeContainer.to_simple_dict` (:pr:`1126`). Additionally, :meth:`~pynamodb.attributes.AttributeContainer.to_dynamodb_dict`
    and :meth:`~pynamodb.attributes.AttributeContainer.from_dynamodb_dict` were added for round-trip JSON serialization.
  * :code:`pynamodb.util.attribute_value_to_json` was removed (:pr:`1126`)

* :py:class:`~pynamodb.attributes.Attribute`'s :code:`default` parameter must be either an immutable value
  (of one of the built-in immutable types) or a callable.
  This prevents a common class of errors caused by unintentionally mutating the default value.
  A simple workaround is to pass an initializer (e.g. change :code:`default={}` to
  :code:`default=dict`) or wrap in a lambda (e.g. change :code:`default={'foo': 'bar'}` to
  :code:`default=lambda: {'foo': 'bar'}`).
* :meth:`~pynamodb.indexes.Index.count`, :meth:`~pynamodb.indexes.Index.query`,
  and :meth:`~pynamodb.indexes.Index.scan` are now instance methods.
* :py:class:`~pynamodb.settings.OperationSettings` has been removed.

Major changes:

* We are now compatible with `opentelemetry botocore instrumentation <https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation/opentelemetry-instrumentation-botocore>`_.
* We've reduced our usage of botocore private APIs (:pr:`1079`). On multiple occasions, new versions
  of botocore broke PynamoDB, and this change lessens the likelihood of that happening in the future
  by reducing (albeit not eliminating) our reliance on private botocore APIs.

Minor changes:

* :meth:`~pynamodb.models.Model.save`, :meth:`~pynamodb.models.Model.update`, :meth:`~pynamodb.models.Model.delete_item`,
  and :meth:`~pynamodb.models.Model.delete` now accept a ``add_version_condition`` parameter.
  See :ref:`optimistic_locking_version_condition` for more details.
* :meth:`~pynamodb.models.Model.batch_get`, has guard rails defending against items without a hash_key and range_key.
* :meth:`~pynamodb.attributes.Attribute.set`, can remove attribute by assigning an empty value in the update expression.

v5.5.1
----------
* Fix compatibility with botocore 1.33.2 (#1205)

v5.5.0
----------
* :meth:`~pynamodb.models.Model.save`, :meth:`~pynamodb.models.Model.update`, :meth:`~pynamodb.models.Model.delete_item`,
  and :meth:`~pynamodb.models.Model.delete` now accept a ``add_version_condition`` parameter.
  See :ref:`optimistic_locking_version_condition` for more details.

v5.4.1
----------
* Use model's AWS credentials in threads (#1164)

  A model can specify custom AWS credentials in the ``Meta`` class (in lieu of "global"
  AWS credentials from the environment). Previously those model-specific credentials
  were not used from within new threads.

Contributors to this release:

* @atsuoishimoto

v5.4.0
----------
* Expose transaction cancellation reasons in
  :meth:`~pynamodb.exceptions.TransactWriteError.cancellation_reasons` and
  :meth:`~pynamodb.exceptions.TransactGetError.cancellation_reasons` (#1144).

v5.3.2
----------
* Prevent ``typing_tests`` from being installed into site-packages (:pr:`1118`)

Contributors to this release:

* :user:`musicinmybrain`


v5.3.1
----------
* Fixed issue introduced in 5.3.0: using :py:class:`~pynamodb.connection.table.TableConnection` directly (not through a model)
  raised the following exception::

    pynamodb.exceptions.TableError: Meta-table for '(table-name)' not initialized

* Fix typing on :py:class:`~pynamodb.transactions.TransactGet` (backport of :pr:`1057`)


v5.3.0
----------
* No longer call ``DescribeTable`` API before first operation

  Before this change, we would call ``DescribeTable`` before the first operation
  on a given table in order to discover its schema. This slowed down bootstrap
  (particularly important for lambdas), complicated testing and could potentially
  cause inconsistent behavior since queries were serialized using the table's
  (key) schema but deserialized using the model's schema.

  With this change, both queries and models now use the model's schema.


v5.2.3
----------
* Update for botocore 1.28 private API change (:pr:`1087`) which caused the following exception::

    TypeError: Cannot mix str and non-str arguments


v5.2.2
----------
* Update for botocore 1.28 private API change (:pr:`1083`) which caused the following exception::

    TypeError: _convert_to_request_dict() missing 1 required positional argument: 'endpoint_url'


v5.2.1
----------
* Fix issue from 5.2.0 with attempting to set GSI provisioned throughput on PAY_PER_REQUEST billing mode (:pr:`1018`)


v5.2.0
----------
* The ``IndexMeta`` class has been removed. Now ``type(Index) == type`` (:pr:`998`)
* JSON serialization support (``Model.to_json`` and ``Model.from_json``) has been added (:pr:`857`)
* Improved type annotations for expressions and transactions (:pr:`951`, :pr:`991`)
* Always use Model attribute definitions in create table schema (:pr:`996`)


v5.1.0
----------

:date: 2021-06-29

* Introduce ``DynamicMapAttribute`` to enable partially defining attributes on a ``MapAttribute`` (:pr:`868`)
* Quality of life improvements: Type annotations, better comment, more resilient test (:pr:`934`, :pr:`936`, :pr:`948`)
* Fix type annotation of ``is_in`` conditional expression (:pr:`947`)
* Null errors should include full attribute path (:pr:`915`)
* Fix for serializing and deserializing dates prior to year 1000 (:pr:`949`)


v5.0.3
----------

:date: 2021-02-14

This version has an unintentional breaking change:

* Propagate ``Model.serialize``'s ``null_check`` parameter to nested MapAttributes (:pr:`908`)

  Previously null errors (persisting ``None`` into an attribute defined as ``null=False``)
  were ignored for attributes in map attributes that were nested in maps or lists. After upgrade,
  these will resulted in an :py:class:`~pynamodb.exceptions.AttributeNullError` being raised.

v5.0.2
----------

:date: 2021-02-11

* Do not serialize all attributes for updates and deletes (:pr:`905`)


v5.0.1
----------

:date: 2021-02-10

* Fix type errors when deriving from a MapAttribute and another type (:pr:`904`)


v5.0.0
----------

:date: 2021-01-26

This is major release and contains breaking changes. Please read the notes below carefully.

Breaking changes:

* Python 2 is no longer supported. Python 3.6 or greater is now required.
* :py:class:`~pynamodb.attributes.UnicodeAttribute` and :py:class:`~pynamodb.attributes.BinaryAttribute` now support empty values (:pr:`830`)

  In previous versions, assigning an empty value to would be akin to assigning ``None``: if the attribute was defined with ``null=True`` then it would be omitted, otherwise an error would be raised.

  As of May 2020, DynamoDB `supports <https://aws.amazon.com/about-aws/whats-new/2020/05/amazon-dynamodb-now-supports-empty-values-for-non-key-string-and-binary-attributes-in-dynamodb-tables/>`_ empty values for String and Binary attributes. This release of PynamoDB starts treating empty values like any other values. If existing code unintentionally assigns empty values to StringAttribute or BinaryAttribute, this may be a breaking change: for example, the code may rely on the fact that in previous versions empty strings would be "read back" as ``None`` values when reloaded from the database.
* :py:class:`~pynamodb.attributes.UTCDateTimeAttribute` now strictly requires the date string format ``'%Y-%m-%dT%H:%M:%S.%f%z'`` to ensure proper ordering.
  PynamoDB has always written values with this format but previously would accept reading other formats.
  Items written using other formats must be rewritten before upgrading.
* Table backup functionality (``Model.dump[s]`` and ``Model.load[s]``) has been removed.
* ``Model.query`` no longer converts unsupported range key conditions into filter conditions.
* Internal attribute type constants are replaced with their "short" DynamoDB version (:pr:`827`)
* Remove ``ListAttribute.remove_indexes`` (added in v4.3.2) and document usage of remove for list elements (:pr:`838`)
* Remove ``pynamodb.connection.util.pythonic`` (:pr:`753`) and (:pr:`865`)
* Remove ``ModelContextManager`` class (:pr:`861`)

Features:

* **Polymorphism**

  This release introduces :ref:`polymorphism` support via :py:class:`DiscriminatorAttribute <pynamodb.attributes.DiscriminatorAttribute>`.
  Discriminator values are written to DynamoDB and used during deserialization to instantiate the desired class.

* **Model Serialization**

  The ``Model`` class now includes public methods for serializing and deserializing its attributes.
  ``Model.serialize`` and ``Model.deserialize`` convert the model to/from a dictionary of DynamoDB attribute values.

Other changes in this release:

* Typed list attributes can now support any Attribute subclass (:pr:`833`)
* Most API operation methods now accept a ``settings`` argument to customize settings of individual operations.
  This currently allow adding or overriding HTTP headers. (:pr:`887`)
* Add the attribute name to error messages when deserialization fails (:pr:`815`)
* Add the table name to error messages for transactional operations (:pr:`835`)

Contributors to this release:

* :user:`jpinner`
* :user:`ikonst`
* :user:`rchilaka`-amzn
* :user:`jonathantan`

v4.4.1
----------
* Fix compatibility with botocore 1.33.2 (#1235)

v4.4.0
----------
* Update for botocore 1.28 private API change (#1130) which caused the following exception::

    TypeError: _convert_to_request_dict() missing 1 required positional argument: 'endpoint_url'

v4.3.3
----------

* Add type stubs for indexing into a ``ListAttribute`` for forming conditional expressions (:pr:`774`)

  ::

    class MyModel(Model):
      ...
      my_list = ListAttribute()

    MyModel.query(..., condition=MyModel.my_list[0] == 42)

* Fix a warning about ``collections.abc`` deprecation (:pr:`782`)


v4.3.2
----------

* Fix discrepancy between runtime and type-checker's perspective of ``Index`` and derived types (:pr:`769`)
* Add ``ListAttribute.remove_indexes`` action for removing specific indexes from a ``ListAttribute`` (:pr:`754`)
* Type stub fixes:

  * Add missing parameters of ``Model.scan`` (:pr:`750`)
  * Change ``Model.get``'s ``hash_key`` parameter to be typed ``Any`` (:pr:`756`)

* Prevent integration tests from being packaged (:pr:`758`)
* Various documentation fixes (:pr:`762`, :pr:`765`, :pr:`766`)

Contributors to this release:

* :user:`mxr`
* :user:`sodre`
* :user:`biniow`
* :user:`MartinAltmayer`
* :user:`dotpmrcunha`
* :user:`meawoppl`

v4.3.1
----------

* Fix Index.query and Index.scan typing regressions introduced in 4.2.0, which were causing false errors
  in type checkers


v4.3.0
----------

* Implement exponential backoff for batch writes (:pr:`728`)
* Avoid passing 'PROVISIONED' BillingMode for compatibility with some AWS AZs (:pr:`721`)
* On Python >= 3.3, use importlib instead of deprecated imp (:pr:`723`)
* Update in-memory object correctly on ``REMOVE`` update expressions (:pr:`741`)

Contributors to this release:

* :user:`hallie`
* :user:`bit`-bot-bit
* :user:`edholland`
* :user:`reginalin`
* :user:`MichelML`
* :user:`timgates42`
* :user:`sunaoka`
* :user:`conjmurph`


v4.2.0
------

:date: 2019-10-17

This is a backwards compatible, minor release.

* Add ``attributes_to_get`` parameter to ``Model.scan`` (:pr:`431`)
* Disable botocore parameter validation for performance (:pr:`711`)

Contributors to this release:

* :user:`ButtaKnife`


v4.1.0
------

:date: 2019-10-17

This is a backwards compatible, minor release.

* In the Model's Meta, you may now provide an AWS session token, which is mostly useful for assumed roles (:pr:`700`)::

    sts_client = boto3.client("sts")
    role_object = sts_client.assume_role(RoleArn=role_arn, RoleSessionName="role_name", DurationSeconds=BOTO3_CLIENT_DURATION)
    role_credentials = role_object["Credentials"]

    class MyModel(Model):
      class Meta:
        table_name = "table_name"
        aws_access_key_id = role_credentials["AccessKeyId"]
        aws_secret_access_key = role_credentials["SecretAccessKey"]
        aws_session_token = role_credentials["SessionToken"]

      hash = UnicodeAttribute(hash_key=True)
      range = UnicodeAttribute(range_key=True)

* Fix warning about `inspect.getargspec` (:pr:`701`)
* Fix provisioning GSIs when using pay-per-request billing (:pr:`690`)
* Suppress Python 3 exception chaining when "re-raising" botocore errors as PynamoDB model exceptions (:pr:`705`)

Contributors to this release:

* :user:`asottile`
* :user:`julienduchesne`


v4.0.0
--------

:date: 2019-04-10

This is major release and contains breaking changes. Please read the notes below carefully.

**Requests Removal**

Given that ``botocore`` has moved to using ``urllib3`` directly for making HTTP requests, we'll be doing the same (via ``botocore``). This means the following:

* The ``session_cls`` option is no longer supported.
* The ``request_timeout_seconds`` parameter is no longer supported. ``connect_timeout_seconds`` and ``read_timeout_seconds`` are available instead.

  + Note that the timeouts for connection and read are now ``15`` and ``30`` seconds respectively. This represents a change from the previous ``60`` second combined ``requests`` timeout.
* *Wrapped* exceptions (i.e ``exc.cause``) that were from ``requests.exceptions`` will now be comparable ones from ``botocore.exceptions`` instead.

**Key attribute types must match table**

The previous release would call `DescribeTable` to discover table metadata
and would use the key types as defined in the DynamoDB table. This could obscure
type mismatches e.g. where a table's hash key is a number (`N`) in DynamoDB,
but defined in PynamoDB as a `UnicodeAttribute`.

With this release, we're always using the PynamoDB model's definition
of all attributes including the key attributes.

**Deprecation of old APIs**

Support for `Legacy Conditional Parameters <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.html>`_ has been
removed. See a complete list of affected ``Model`` methods below:

.. list-table::
   :widths: 10 90
   :header-rows: 1

   * - Method
     - Changes
   * - ``update_item``
     - removed in favor of ``update``
   * - ``rate_limited_scan``
     - removed in favor of ``scan`` and ``ResultIterator``
   * - ``delete``
     - ``conditional_operator`` and ``**expected_values`` kwargs removed. Use ``condition`` instead.
   * - ``update``
     - ``attributes``, ``conditional_operator`` and ``**expected_values`` kwargs removed. Use ``actions`` and ``condition`` instead.
   * - ``save``
     - ``conditional_operator`` and ``**expected_values`` kwargs removed. Use ``condition`` instead.
   * - ``count``
     - ``**filters`` kwargs removed. Use ``range_key_condition``/``filter_condition`` instead.
   * - ``query``
     - ``conditional_operator`` and ``**filters`` kwargs removed. Use ``range_key_condition``/``filter_condition`` instead.
   * - ``scan``
     -
       - ``conditional_operator`` and ``**filters`` kwargs removed. Use ``filter_condition`` instead.
       - ``allow_rate_limited_scan_without_consumed_capacity`` was removed


When upgrading, pay special attention to use of ``**filters`` and ``**expected_values``, as you'll need to check for arbitrary names that correspond to
attribute names. Also keep an eye out for kwargs like ``user_id__eq=5`` or ``email__null=True``, which are no longer supported. A type check can help you catch cases like these.

New features in this release:

* Support for transactions (``TransactGet`` and ``TransactWrite``) (:pr:`618`)
* Support for versioned optimistic locking (:pr:`664`)

Other changes in this release:

* Python 2.6 is no longer supported. 4.x.x will be the last major release to support Python 2.7 given the upcoming EOL.
* Added the ``max_pool_connection`` and ``extra_headers`` settings to replace common use cases for ``session_cls``
* Added support for `moto <https://github.com/spulec/moto>`_ through implementing the botocore "before-send" hook.
* Performance improvements to ``UTCDateTimeAttribute`` deserialization. (:pr:`610`)
* The ``MapAttributeMeta`` class has been removed. Now ``type(MapAttribute) == AttributeContainerMeta``.
* Removed ``LegacyBooleanAttribute`` and the read-compatibility for it in ``BooleanAttribute``.
* `None` can now be used to bootstrap condition chaining (:pr:`653`)
* Allow specifying timedeltas in expressions involving TTLAttributes (:pr:`665`)


v3.4.1
------

:date: 2019-06-28

This is a backwards compatible, minor release.

Changes in this release:

* Fix type stubs to include new methods and parameters introduced with time-to-live support


v3.4.0
------

:date: 2019-06-13

This is a backwards compatible, minor release.

Changes in this release:

* Adds a TTLAttribute that specifies when items expire (:pr:`259`)
* Enables time-to-live on a DynamoDB table if the corresponding model has a TTLAttribute
* Adds a default_for_new parameter for Attribute which is a default that applies to new items only

Contributors to this release:

* :user:`irhkang`
* :user:`ikonst`


v3.3.3
------

:date: 2019-01-15

This is a backwards compatible, minor release.

Fixes in this release:

* Legacy boolean attribute migration fix. (:pr:`538`)
* Correctly package type stubs. (:pr:`585`)

Contributors to this release:

* :user:`vo`-va


v3.3.2
------

:date: 2019-01-03

This is a backwards compatible, minor release.

Changes in this release:

* Built-in support for mypy type stubs, superseding those in python/typeshed. (:pr:`537`)


v3.3.1
------

:date: 2018-08-30

This is a backwards compatible, minor bug fix release.

Fixes in this release:

* Clearer error message on missing consumed capacity during rate-limited scan. (:pr:`506`)
* Python 3 compatibility in PageIterator. (:pr:`535`)
* Proxy configuration changes in botocore>=1.11.0. (:pr:`531`)

Contributors to this release:

* :user:`ikonst`
* :user:`zetaben`
* :user:`ningirsu`


v3.3.0
------

:date: 2018-05-09

This is a backwards compatible, major bug fix release.

New features in this release:


* Support scan operations on secondary indexes. (:pr:`141`, :pr:`392`)
* Support projections in model get function. (:pr:`337`, :pr:`403`)
* Handle values from keys when batch get returns unprocessed keys. (:pr:`252`, :pr:`376`)
* Externalizes AWS Credentials. (:pr:`426`)
* Add migration support for LegacyBooleanAttribute. (:pr:`404`, :pr:`405`)
* Rate limited Page Iterator. (:pr:`481`)

Fixes in this release:

* Thread-safe client creation in botocore. (:pr:`153`, :pr:`393`)
* Use attr.get_value(value) when deserialize. (:pr:`450`)
* Skip null attributes post serialization for maps. (:pr:`455`)
* Fix deserialization bug in BinaryAttribute and BinarySetAttribute. (:pr:`459`, :pr:`480`)
* Allow MapAttribute instances to be used as the RHS in expressions. (:pr:`488`)
* Return the correct last_evaluated_key for limited queries/scans. (:pr:`406`, :pr:`410`)
* Fix exclusive_start_key getting lost in PageIterator. (:pr:`421`)
* Add python 3.5 for Travis ci builds. (:pr:`437`)

Contributors to this release:

* :user:`jpinner`-lyft
* :user:`scode`
* :user:`behos`
* :user:`jmphilli`
* :user:`drewisme`
* :user:`nicysneiros`
* :user:`jcomo`
* :user:`kevgliss`
* :user:`asottile`
* :user:`harleyk`
* :user:`betamoo`


v3.2.1
------

:date: 2017-10-25

This is a backwards compatible, minor bug fix release.

Removed features in this release:

* Remove experimental Throttle api. (:pr:`378`)

Fixes in this release:

* Handle attributes that cannot be retrieved by getattr. Fixes :pr:`104` (:pr:`385`)
* Model.refresh() should reset all model attribuets. Fixes :pr:`166` (:pr:`388`)
* Model.loads() should deserialize using custom attribute names. Fixes :pr:`168` (:pr:`387`)
* Deserialize hash key during table loads. Fixes :pr:`143` (:pr:`386`)
* Support pagination in high-level api query and scan methods. Fixes :pr:`50`, :pr:`118`, :pr:`207`, and :pr:`248` (:pr:`379`)
* Don't serialize null nested attributed. Fixes :pr:`240` and :pr:`309` (:pr:`375`)
* Legacy update item subset removal using DELETE operator. Fixes :pr:`132` (:pr:`374`)

Contributors to this release:

* :user:`jpinner`-lyft


v3.2.0
------

:date: 2017-10-13

This is a backwards compatible, minor release.

This release updates PynamoDB to interact with Dynamo via the current version of Dynamo's API.
Condition and update expressions can now be created from attributes and used in model operations.
Legacy filter and attribute update keyword arguments have been deprecated. Using these arguments
will cause a warning to be logged.

New features in this release:

* Add support for current version of DynamoDB API
* Improved ``MapAttribute`` item assignment and access.

Contributors to this release:

* :user:`jpinner`-lyft


v3.2.0rc2
---------

:date: 2017-10-09

This is a backwards compatible, release candidate.

This release candidate allows dereferencing raw ``MapAttributes`` in condition expressions.
It also improves ``MapAttribute`` assignment and access.

Contributors to this release:

* :user:`jpinner`-lyft


v3.2.0rc1
---------

:date: 2017-09-22

This is a backwards compatible, release candidate.

This release candidate updates PynamoDB to interact with Dynamo via the current version of Dynamo's API.
It deprecates some internal methods that were used to interact with Dynamo that are no longer relevant.
If your project was calling those low level methods a warning will be logged.

New features in this release:

* Add support for current version of DynamoDB API

Contributors to this release:

* :user:`jpinner`-lyft


v3.1.0
------

:date: 2017-07-07

This is a backwards compatible, minor release.

Note that we now require ``botocore>=1.2.0``; this is required to support the
``consistent_read`` parameter when scanning.

Calling ``Model.count()`` without a ``hash_key`` and *with* ``filters`` will
raise a ``ValueError``, as it was previously returning incorrect results.

New features in this release:

* Add support for signals via blinker (:pr:`278`)

Fixes in this release:

* Pass batch parameters down to boto/dynamo (:pr:`308`)
* Raise a ValueError if count() is invoked with no hash key AND filters (:pr:`313`)
* Add consistent_read parameter to Model.scan (:pr:`311`)

Contributors to this release:

* :user:`jmphilli`
* :user:`Lordnibbler`
* :user:`lita`


v3.0.1
------

:date: 2017-06-09

This is a major release with breaking changes.

``MapAttribute`` now allows pythonic access when recursively defined.
If you were not using the ``attr_name=`` kwarg then you should have no problems upgrading.
Previously defined non subclassed ``MapAttributes`` (raw ``MapAttributes``) that were members of a subclassed ``MapAttribute`` (typed ``MapAttributes``) would have to be accessed like a dictionary.
Now object access is possible and recommended.
Access via the ``attr_name``, also known as the DynamoDB name, will now throw an ``AttributeError``.

``UnicodeSetAttributes`` do not json serialize or deserialize anymore.
We deprecated the functionality of json serializing as of ``1.6.0`` but left the deserialization functionality in there so people could migrate away from the old functionality.
If you have any ``UnicodeSetAttributes`` that have not been persisted since version ``1.6.0`` you will need to migrate your data or manage the json encoding and decoding with a custom attribute in application.

* Performance enhancements for the ``UTCDateTimeAttribute`` deserialize method. (:pr:`277`)
* There was a regression with attribute discovery. Fixes attribute discovery for model classes with inheritance (:pr:`280`)
* Fix to ignore null checks for batch delete (:pr:`283`)
* Fix for ``ListAttribute`` and ``MapAttribute`` serialize (:pr:`286`)
* Fix for ``MapAttribute`` pythonic access (:pr:`292`) This is a breaking change.
* Deprecated the json decode in ``UnicodeSetAttribute`` (:pr:`294`) This is a breaking change.
* Raise ``TableDoesNotExist`` error instead of letting json decoding ``ValueErrors`` raise (:pr:`296`)

Contributors to this release:

* :user:`jcbertin`
* :user:`johnliu`
* :user:`scode`
* :user:`rowilla`
* :user:`lita`
* :user:`garretheel`
* :user:`jmphilli`


v2.2.0
------

:date: 2017-10-25

This is a backwards compatible, minor release.

The purpose of this release is to prepare users to upgrade to v3.0.1+
(see issue :pr:`377` for details).

Pull request :pr:`294` removes the backwards compatible deserialization of
UnicodeSetAttributes introduced in :pr:`151`.

This release introduces a migration function on the Model class to help
re-serialize any data that was written with v1.5.4 and below.

Temporary feature in this release:

* Model.fix_unicode_set_attributes() migration helper
* Model.needs_unicode_set_fix() migration helper


v2.1.6
------

:date: 2017-05-10

This is a backwards compatible, minor release.

Fixes in this release:

* Replace Delorean with dateutil (:pr:`208`)
* Fix a bug with count -- consume all pages in paginated response (:pr:`256`)
* Update mock lib (:pr:`262`)
* Use pytest instead of nose (:pr:`263`)
* Documentation changes (:pr:`269`)
* Fix null deserialization in MapAttributes (:pr:`272`)

Contributors to this release:

* :user:`funkybob`
* :user:`garrettheel`
* :user:`lita`
* :user:`jmphilli`


v2.1.5
------

:date: 2017-03-16

This is a backwards compatible, minor release.

Fixes in this release:

* Apply retry to ProvisionedThroughputExceeded (:pr:`222`)
* rate_limited_scan fix to handle consumed capacity (:pr:`235`)
* Fix for test when dict ordering differs (:pr:`237`)

Contributors to this release:

* :user:`anandswaminathan`
* :user:`jasonfriedland`
* :user:`JohnEmhoff`


v2.1.4
------

:date: 2017-02-14

This is a minor release, with some changes to `MapAttribute` handling. Previously,
when accessing a `MapAttribute` via `item.attr`, the type of the object used during
instantiation would determine the return value. `Model(attr={...})` would return
a `dict` on access. `Model(attr=MapAttribute(...))` would return an instance of
`MapAttribute`. After :pr:`223`, a `MapAttribute` will always be returned during
item access regardless of the type of the object used during instantiation. For
convenience, a `dict` version can be accessed using `.as_dict()` on the `MapAttribute`.

New features in this release:

* Support multiple attribute update (:pr:`194`)
* Rate-limited scan (:pr:`205`)
* Always create map attributes when setting a dict (:pr:`223`)

Fixes in this release:

* Remove AttributeDict and require explicit attr names (:pr:`220`)
* Add distinct DoesNotExist classes per model (:pr:`206`)
* Ensure defaults are respected for MapAttribute (:pr:`221`)
* Add docs for GSI throughput changes (:pr:`224`)

Contributors to this release:

* :user:`anandswaminathan`
* :user:`garrettheel`
* :user:`ikonst`
* :user:`jasonfriedland`
* :user:`yedpodtrzitko`


v2.0.3
------

:date: 2016-11-18

This is a backwards compatible, minor release.

Fixes in this release:

* Allow longs as members of maps + lists in python 2 (:pr:`200`)
* Allow raw map attributes in subclassed map attributes (:pr:`199`)

Contributors to this release:

* :user:`jmphilli`


v2.0.2
------

:date: 2016-11-10

This is a backwards compatible, minor release.

Fixes in this release:

* add BOOL into SHORT_ATTR_TYPES (:pr:`190`)
* deserialize map attributes correctly (:pr:`192`)
* prepare request with requests session so session properties are applied (:pr:`197`)

Contributors to this release:

* :user:`anandswaminathan`
* :user:`jmphilli`
* :user:`yedpodtrzitko`


v2.0.1
------

:date: 2016-11-04

This is a backwards compatible, minor release.

Fixes in this release:

* make "unprocessed keys for batch operation" log at info level (:pr:`180`)
* fix RuntimeWarning during imp_load in custom settings file (:pr:`185`)
* allow unstructured map attributes (:pr:`186`)

Contributors to this release:

* :user:`danielhochman`
* :user:`jmphilli`
* :user:`bedge`


v2.0.0
------

:date: 2016-11-01

This is a major release, which introduces support for native DynamoDB maps and lists. There are no
changes which are expected to break backwards compatibility, but you should test extensively before
upgrading in production due to the volume of changes.

New features in this release:

* Add support for native map and list attributes (:pr:`175`)

Contributors to this release:

* :user:`jmphilli`
* :user:`berdim99`


v1.6.0
------

:date: 2016-10-20

This is a minor release, with some changes to BinaryAttribute handling and new options for configuration.

BooleanAttribute now uses the native API type "B". BooleanAttribute is also compatible with the legacy BooleanAttributes
on read. On save, they will be rewritten with the native type. If you wish to avoid this behavior, you can continue
to use LegacyBooleanAttribute. LegacyBooleanAttribute is also forward compatible with native boolean
attributes to allow for migration.

New features in this release:

* Add support for native boolean attributes (:pr:`149`)
* Parse legacy and native bool in legacy bool (:pr:`158`)
* Allow override of settings from global configuration file (:pr:`147`)

Fixes in this release:

* Serialize UnicodeSetAttributes correctly (:pr:`151`)
* Make update_item respect attr_name differences (:pr:`160`)

Contributors to this release:

* :user:`anandswaminathan`
* :user:`jmphilli`
* :user:`lita`


v1.5.4
------

:date: 2017-10-25

This is a backwards compatible, minor bug fix release.

The purpose of this release is to prepare users to upgrade to v1.6.0+
(see issue :pr:`377` for details).

Pull request :pr:`151` introduces a backwards incompatible change to how
UnicodeSetAttributes are serialized. While the commit attempts to
provide compatibility by deserializing values written with v1.5.3 and
below, it prevents users from upgrading because it starts writing non
JSON-encoded values to dynamo.

Anyone using UnicodeSetAttribute must first deploy this version.

Fixes in this release:

* Backport UnicodeSetAttribute deserialization code from :pr:`151`


v1.5.3
------

:date: 2016-08-08

This is a backwards compatible, minor release.

Fixes in this release:

* Introduce concept of page_size, separate from num items returned limit (:pr:`139`)

Contributors to this release:

* :user:`anandswaminathan`


v1.5.2
------

:date: 2016-06-23

This is a backwards compatible, minor release.

Fixes in this release:

* Additional retry logic for HTTP Status Code 5xx, usually attributed to InternalServerError (:pr:`135`)

Contributors to this release:

* :user:`danielhochman`


v1.5.1
------

:date: 2016-05-11

This is a backwards compatible, minor release.

Fixes in this release:

* Fix for binary attribute handling of unprocessed items data corruption affecting users of 1.5.0 (:pr:`126` fixes :pr:`125`)

Contributors to this release:

* :user:`danielhochman`


v1.5.0
------

:date: 2016-05-09

This is a backwards compatible, minor release.

Please consider the fix for limits before upgrading. Correcting for off-by-one when querying is
no longer necessary.

Fixes in this release:

* Fix off-by-one error for limits when querying (:pr:`123` fixed :pr:`95`)
* Retry on ConnectionErrors and other types of RequestExceptions (:pr:`121` fixes :pr:`98`)
* More verbose logging when receiving errors e.g. InternalServerError from the DynamoDB API (:pr:`115`)
* Prevent permanent poisoning of credential cache due to botocore bug (:pr:`113` fixes :pr:`99`)
* Fix for UnprocessedItems serialization error (:pr:`114` fixes :pr:`103`)
* Fix parsing issue with newer version of dateutil and UTCDateTimeAttributes (:pr:`110` fixes :pr:`109`)
* Correctly handle expected value generation for set types (:pr:`107` fixes :pr:`102`)
* Use HTTP proxies configured by botocore (:pr:`100` fixes :pr:`92`)

New features in this release:

* Return the cause of connection exceptions to the caller (:pr:`108` documented by :pr:`112`)
* Configurable session class for custom connection pool size, etc (:pr:`91`)
* Add attributes_to_get and consistent_read to more of the API (:pr:`79`)

Contributors to this release:

* :user:`ab`
* :user:`danielhochman`
* :user:`jlafon`
* :user:`joshowen`
* :user:`jpinner`-lyft
* :user:`mxr`
* :user:`nickgravgaard`


v1.4.4
------

:date: 2015-11-10

This is a backward compatible, minor release.

Changes in this release:

* Support for enabling table streams at table creation time (thanks to :user:`brln`)
* Fixed bug where a value was always required for update_item when action was 'delete' (:pr:`90`)


v1.4.3
------

:date: 2015-10-12

This is a backward compatible, minor release. Included are bug fixes and performance improvements.

A huge thank you to all who contributed to this release:

* Daniel Hochman
* Josh Owen
* Keith Mitchell
* Kevin Wilson

Changes in this release:

* Fixed bug where models without a range key weren't handled correctly
* Botocore is now only used for preparing requests (for performance reasons)
* Removed the dependency on OrderedDict
* Fixed bug for zope interface compatibility (:pr:`71`)
* Fixed bug where the range key was handled incorrectly for integer values

v1.4.2
------

:date: 2015-06-26

This is a backward compatible, minor bug fix release.

Bugs fixed in this release:

* Fixed bug where botocore exceptions were not being reraised.


v1.4.1
------

:date: 2015-06-26

This is a backward compatible, minor bug fix release.

Bugs fixed in this release:

* Fixed bug where a local variable could be unbound (:pr:`67`).


v1.4.0
------

:date: 2015-06-23

This is a minor release, with backward compatible bug fixes.

Bugs fixed in this release:

* Added support for botocore 1.0.0 (:pr:`63`)
* Fixed bug where Model.get() could fail in certain cases (:pr:`64`)
* Fixed bug where JSON strings weren't being encoded properly (:pr:`61`)


v1.3.7
------

:date: 2015-04-06

This is a backward compatible, minor bug fix release.

Bugs fixed in this release:

* Fixed bug where range keys were not included in update_item (:pr:`59`)
* Fixed documentation bug (:pr:`58`)


v1.3.6
------

:date: 2015-04-06

This is a backward compatible, minor bug fix release.

Bugs fixed in this release:

* Fixed bug where arguments were used incorrectly in update_item (:pr:`54`)
* Fixed bug where falsy values were used incorrectly in model constructors (:pr:`57`), thanks :user:`pior`
* Fixed bug where the limit argument for scan and query was not always honored.

New features:

* Table counts with optional filters can now be queried using ``Model.count(**filters)``


v1.3.5
------

This is a backward compatible, minor bug fix release.

Bugs fixed in this release.

* Fixed bug where scan did not properly limit results (:pr:`45`)
* Fixed bug where scan filters were not being preserved (:pr:`44`)
* Fixed bug where items were mutated as an unexpected side effect (:pr:`47`)
* Fixed bug where conditional operator wasn't used in scan


v1.3.4
------

:date: 2014-10-06

This is a backward compatible, minor bug fix release.

Bugs fixed in this release.

* Fixed bug where attributes could not be used in multiple indexes when creating a table.
* Fixed bug where a dependency on mock was accidentally introduced.

v1.3.3
------

:date: 2014-9-18

This is a backward compatible, minor bug fix release, fixing the following issues

* Fixed bug with Python 2.6 compatibility (:pr:`28`)
* Fixed bug where update_item was incorrectly checking attributes for null (:pr:`34`)

Other minor improvements

* New API for backing up and restoring tables
* Better support for custom attributes (https://github.com/pynamodb/PynamoDB/commit/0c2ba5894a532ed14b6c14e5059e97dbb653ff12)
* Explicit Travis CI testing of Python 2.6, 2.7, 3.3, 3.4, and PyPy
* Tests added for round tripping unicode values


v1.3.2
------

:date: 2014-7-02

* This is a minor bug fix release, fixing a bug where query filters were incorrectly parsed (:pr:`26`).

v1.3.1
------

:date: 2014-05-26

* This is a bug fix release, ensuring that KeyCondition and QueryFilter arguments are constructed correctly (:pr:`25`).
* Added an example URL shortener to the examples.
* Minor documentation fixes.


v1.3.0
------

:date: 2014-05-20

* This is a minor release, with new backward compatible features and bug fixes.
* Fixed bug where NULL and NOT_NULL were not set properly in query and scan operations (:pr:`24`)
* Support for specifying the index_name as a Index.Meta attribute (:pr:`23`)
* Support for specifying read and write capacity in Model.Meta (:pr:`22`)


v1.2.2
------

:date: 2014-05-14

* This is a minor bug fix release, resolving :pr:`21` (key_schema ordering for create_table).

v1.2.1
------

:date: 2014-05-07

* This is a minor bug fix release, resolving :pr:`20`.

v1.2.0
------

:date: 2014-05-06

* Numerous documentation improvements
* Improved support for conditional operations
* Added support for filtering queries on non key attributes (https://aws.amazon.com/blogs/aws/improved-queries-and-updates-for-dynamodb/)
* Fixed issue with JSON loading where escaped characters caused an error (:pr:`17`)
* Minor bug fixes

v1.1.0
------

:date: 2014-04-14

* PynamoDB now requires botocore version 0.42.0 or greater
* Improved documentation
* Minor bug fixes
* New API endpoint for deleting model tables
* Support for expected value conditions in item delete, update, and save
* Support for limit argument to queries
* Support for aliased attribute names

Example of using aliased attribute names:

.. code-block:: python

    class AliasedModel(Model):
        class Meta:
            table_name = "AliasedModel"
        forum_name = UnicodeAttribute(hash_key=True, attr_name='fn')
        subject = UnicodeAttribute(range_key=True, attr_name='s')

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

* Bug fix release. Proper handling of update_item attributes for atomic item updates, with tests. Fixes :pr:`7`.

v0.1.12
-------

:date: 2014-03-18

* Added a region attribute to model classes, allowing users to specify the AWS region, per model. Fixes :pr:`6`.

v0.1.11
-------

:date: 2014-02-26

* New exception behavior: Model.get and Model.refresh will now raise DoesNotExist if the item is not found in the table.
* Correctly deserialize complex key types. Fixes :pr:`3`
* Correctly construct keys for tables that don't have both a hash key and a range key in batch get operations. Fixes :pr:`5`
* Better PEP8 Compliance
* More tests
* Removed session and endpoint caching to avoid using stale IAM role credentials
