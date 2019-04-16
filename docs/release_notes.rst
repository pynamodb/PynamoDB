Release Notes
=============

v4.0.0a1
--------

:date: 2019-04-10

NB: This is an alpha release and these notes are subject to change.

This is major release and contains breaking changes. Please read the notes below carefully.

Given that ``botocore`` has moved to using ``urllib3`` directly for making HTTP requests, we'll be doing the same (via ``botocore``). This means the following:

* The ``session_cls`` option is no longer supported.

* The ``request_timeout_seconds`` parameter is no longer supported. ``connect_timeout_seconds`` and ``read_timeout_seconds`` are now available instead.

  + Note that the timeout for connection and read are now ``15`` and ``30`` seconds respectively. This represents a change from the previous ``60`` second combined ``requests`` timeout.
  
* *Wrapped* exceptions (i.e ``exc.cause``) that were from ``requests.exceptions`` will now be comparable ones from ``botocore.exceptions`` instead.

Other changes in this release:

* Python 2.6 is no longer supported. 4.x.x will likely be the last major release to support Python 2.7, given the upcoming EOL.
* Added the ``max_pool_connection`` and ``extra_headers`` settings to replace common use cases for ``session_cls``
* Added support for `moto <https://github.com/spulec/moto>`_ through implementing the botocore "before-send" hook. Other botocore hooks remain unimplemented.


v3.3.3
------

:date: 2019-01-15

This is a backwards compatible, minor release.

Fixes in this release:

* Legacy boolean attribute migration fix. (#538)
* Correctly package type stubs. (#585)

Contributors to this release:

* @vo-va


v3.3.2
------

:date: 2019-01-03

This is a backwards compatible, minor release.

Changes in this release:

* Built-in support for mypy type stubs, superseding those in python/typeshed. (#537)


v3.3.1
------

:date: 2018-08-30

This is a backwards compatible, minor bug fix release.

Fixes in this release:

* Clearer error message on missing consumed capacity during rate-limited scan. (#506)
* Python 3 compatibility in PageIterator. (#535)
* Proxy configuration changes in botocore>=1.11.0. (#531)

Contributors to this release:

* @ikonst
* @zetaben
* @ningirsu


v3.3.0
------

:date: 2018-05-09

This is a backwards compatible, major bug fix release.

New features in this release:


* Support scan operations on secondary indexes. (#141, #392)
* Support projections in model get function. (#337, #403)
* Handle values from keys when batch get returns unprocessed keys. (#252, #376)
* Externalizes AWS Credentials. (#426)
* Add migration support for LegacyBooleanAttribute. (#404, #405)
* Rate limited Page Iterator. (#481)


Fixes in this release:

* Thread-safe client creation in botocore. (#153, #393)
* Use attr.get_value(value) when deserialize. (#450) 
* Skip null attributes post serialization for maps. (#455)
* Fix deserialization bug in BinaryAttribute and BinarySetAttribute. (#459, #480)
* Allow MapAttribute instances to be used as the RHS in expressions. (#488)
* Return the correct last_evaluated_key for limited queries/scans. (#406, #410)
* Fix exclusive_start_key getting lost in PageIterator. (#421)
* Add python 3.5 for Travis ci builds. (#437)

Contributors to this release:

* @jpinner-lyft
* @scode
* @behos
* @jmphilli
* @drewisme
* @nicysneiros
* @jcomo
* @kevgliss
* @asottile
* @harleyk
* @betamoo


v3.2.1
------

:date: 2017-10-25

This is a backwards compatible, minor bug fix release.

Removed features in this release:

* Remove experimental Throttle api. (#378)

Fixes in this release:

* Handle attributes that cannot be retrieved by getattr. Fixes #104 (#385)
* Model.refresh() should reset all model attribuets. Fixes #166 (#388)
* Model.loads() should deserialize using custom attribute names. Fixes #168 (#387)
* Deserialize hash key during table loads. Fixes #143 (#386)
* Support pagination in high-level api query and scan methods. Fixes #50, #118, #207, and #248 (#379)
* Don't serialize null nested attributed. Fixes #240 and #309 (#375)
* Legacy update item subset removal using DELETE operator. Fixes #132 (#374)

Contributors to this release:

* @jpinner-lyft


v3.2.0
------

:date: 2017-10-13

This is a backwards compatible, minor release.

This release updates PynamoDB to interact with Dynamo via the current version of Dynamo's API.
Condition and update expressions can now be created from attributes and used in model operations.
Legacy filter and attribute update keyword arguments have been deprecated. Using these arguments
will cause a warning to be logged.

New features in this release:

* Add support for current version of `DynamoDB API <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Appendix.CurrentAPI.html>`_
* Improved ``MapAttribute`` item assignment and access.

Contributors to this release:

* @jpinner-lyft


v3.2.0rc2
---------

:date: 2017-10-09

This is a backwards compatible, release candidate.

This release candidate allows dereferencing raw ``MapAttributes`` in condition expressions.
It also improves ``MapAttribute`` assignment and access.

Contributors to this release:

* @jpinner-lyft


v3.2.0rc1
---------

:date: 2017-09-22

This is a backwards compatible, release candidate.

This release candidate updates PynamoDB to interact with Dynamo via the current version of Dynamo's API. 
It deprecates some internal methods that were used to interact with Dynamo that are no longer relevant. 
If your project was calling those low level methods a warning will be logged.

New features in this release:

* Add support for current version of `DynamoDB API <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Appendix.CurrentAPI.html>`_

Contributors to this release:

* @jpinner-lyft


v3.1.0
------

:date: 2017-07-07

This is a backwards compatible, minor release.

Note that we now require ``botocore>=1.2.0``; this is required to support the 
``consistent_read`` parameter when scanning.

Calling ``Model.count()`` without a ``hash_key`` and *with* ``filters`` will
raise a ``ValueError``, as it was previously returning incorrect results.

New features in this release:

* Add support for signals via blinker (#278)

Fixes in this release:

* Pass batch parameters down to boto/dynamo (#308)
* Raise a ValueError if count() is invoked with no hash key AND filters (#313)
* Add consistent_read parameter to Model.scan (#311)

Contributors to this release:

* @jmphilli
* @Lordnibbler
* @lita


v3.0.1
------

:date: 2017-06-09

This is a major release with breaking changes.

``MapAttribute`` now allows pythonic access when recursively defined.
If you were not using the ``attr_name=`` kwarg then you should have no problems upgrading.
Previously defined non subclassed ``MapAttributes`` (raw ``MapAttributes``) that were members of a subclassed ``MapAttribute`` (typed ``MapAttributes``) would have to be accessed like a dictionary.
Now object access is possible and recommended. See [here](https://github.com/pynamodb/PynamoDB/blob/master/pynamodb/tests/test_attributes.py#L671) for a test example.
Access via the ``attr_name``, also known as the DynamoDB name, will now throw an ``AttributeError``.

``UnicodeSetAttributes`` do not json serialize or deserialize anymore.
We deprecated the functionality of json serializing as of ``1.6.0`` but left the deserialization functionality in there so people could migrate away from the old functionality. 
If you have any ``UnicodeSetAttributes`` that have not been persisted since version ``1.6.0`` you will need to migrate your data or manage the json encoding and decoding with a custom attribute in application. 

* Performance enhancements for the ``UTCDateTimeAttribute`` deserialize method. (#277)
* There was a regression with attribute discovery. Fixes attribute discovery for model classes with inheritance (#280)
* Fix to ignore null checks for batch delete (#283)
* Fix for ``ListAttribute`` and ``MapAttribute`` serialize (#286)
* Fix for ``MapAttribute`` pythonic access (#292) This is a breaking change.
* Deprecated the json decode in ``UnicodeSetAttribute`` (#294) This is a breaking change.
* Raise ``TableDoesNotExist`` error instead of letting json decoding ``ValueErrors`` raise (#296)

Contributors to this release:

* @jcbertin
* @johnliu
* @scode
* @rowilla
* @lita
* @garretheel
* @jmphilli


v2.2.0
------

:date: 2017-10-25

This is a backwards compatible, minor release.

The purpose of this release is to prepare users to upgrade to v3.0.1+
(see issue #377 for details).

Pull request #294 removes the backwards compatible deserialization of
UnicodeSetAttributes introduced in #151.

This release introduces a migration function on the Model class to help
re-serialize any data that was written with v1.5.4 and below.

Temporary feature in this release:

* Model.fix_unicode_set_attributes() migration helper
# Model.needs_unicode_set_fix() migration helper


v2.1.6
------

:date: 2017-05-10

This is a backwards compatible, minor release.

Fixes in this release:

* Replace Delorean with dateutil (#208)
* Fix a bug with count -- consume all pages in paginated response (#256)
* Update mock lib (#262)
* Use pytest instead of nose (#263)
* Documentation changes (#269)
* Fix null deserialization in MapAttributes (#272)

Contributors to this release:

* @funkybob
* @garrettheel
* @lita
* @jmphilli


v2.1.5
------

:date: 2017-03-16

This is a backwards compatible, minor release.

Fixes in this release:

* Apply retry to ProvisionedThroughputExceeded (#222)
* rate_limited_scan fix to handle consumed capacity (#235)
* Fix for test when dict ordering differs (#237)

Contributors to this release:

* @anandswaminathan
* @jasonfriedland
* @JohnEmhoff


v2.1.4
------

:date: 2017-02-14

This is a minor release, with some changes to `MapAttribute` handling. Previously,
when accessing a `MapAttribute` via `item.attr`, the type of the object used during
instantiation would determine the return value. `Model(attr={...})` would return
a `dict` on access. `Model(attr=MapAttribute(...))` would return an instance of
`MapAttribute`. After #223, a `MapAttribute` will always be returned during
item access regardless of the type of the object used during instantiation. For
convenience, a `dict` version can be accessed using `.as_dict()` on the `MapAttribute`.

New features in this release:

* Support multiple attribute update (#194)
* Rate-limited scan (#205)
* Always create map attributes when setting a dict (#223)

Fixes in this release:

* Remove AttributeDict and require explicit attr names (#220)
* Add distinct DoesNotExist classes per model (#206)
* Ensure defaults are respected for MapAttribute (#221)
* Add docs for GSI throughput changes (#224)

Contributors to this release:

* @anandswaminathan
* @garrettheel
* @ikonst
* @jasonfriedland
* @yedpodtrzitko


v2.0.3
------

:date: 2016-11-18

This is a backwards compatible, minor release.

Fixes in this release:

* Allow longs as members of maps + lists in python 2 (#200)
* Allow raw map attributes in subclassed map attributes (#199)

Contributors to this release:

* @jmphilli


v2.0.2
------

:date: 2016-11-10

This is a backwards compatible, minor release.

Fixes in this release:

* add BOOL into SHORT_ATTR_TYPES (#190)
* deserialize map attributes correctly (#192)
* prepare request with requests session so session properties are applied (#197)

Contributors to this release:

* @anandswaminathan
* @jmphilli
* @yedpodtrzitko


v2.0.1
------

:date: 2016-11-04

This is a backwards compatible, minor release.

Fixes in this release:

* make "unprocessed keys for batch operation" log at info level (#180)
* fix RuntimeWarning during imp_load in custom settings file (#185)
* allow unstructured map attributes (#186)

Contributors to this release:

* @danielhochman
* @jmphilli
* @bedge


v2.0.0
------

:date: 2016-11-01

This is a major release, which introduces support for native DynamoDB maps and lists. There are no
changes which are expected to break backwards compatibility, but you should test extensively before
upgrading in production due to the volume of changes.

New features in this release:

* Add support for native map and list attributes (#175)

Contributors to this release:

* @jmphilli
* @berdim99


v1.6.0
------

:date: 2016-10-20

This is a minor release, with some changes to BinaryAttribute handling and new options for configuration.

BooleanAttribute now uses the native API type "B". BooleanAttribute is also compatible with the legacy BooleanAttributes
on read. On save, they will be rewritten with the native type. If you wish to avoid this behavior, you can continue
to use LegacyBooleanAttribute. LegacyBooleanAttribute is also forward compatible with native boolean
attributes to allow for migration.

New features in this release:

* Add support for native boolean attributes (#149)
* Parse legacy and native bool in legacy bool (#158)
* Allow override of settings from global configuration file (#147)

Fixes in this release:

* Serialize UnicodeSetAttributes correctly (#151)
* Make update_item respect attr_name differences (#160)

Contributors to this release:

* @anandswaminathan
* @jmphilli
* @lita


v1.5.4
------

:date: 2017-10-25

This is a backwards compatible, minor bug fix release.

The purpose of this release is to prepare users to upgrade to v1.6.0+
(see issue #377 for details).

Pull request #151 introduces a backwards incompatible change to how
UnicodeSetAttributes are serialized. While the commit attempts to
provide compatibility by deserializing values written with v1.5.3 and
below, it prevents users from upgrading because it starts writing non
JSON-encoded values to dynamo.

Anyone using UnicodeSetAttribute must first deploy this version.

Fixes in this release:

* Backport UnicodeSetAttribute deserialization code from #151


v1.5.3
------

:date: 2016-08-08

This is a backwards compatible, minor release.

Fixes in this release:

* Introduce concept of page_size, separate from num items returned limit (#139)

Contributors to this release:

* @anandswaminathan


v1.5.2
------

:date: 2016-06-23

This is a backwards compatible, minor release.

Fixes in this release:

* Additional retry logic for HTTP Status Code 5xx, usually attributed to InternalServerError (#135)

Contributors to this release:

* @danielhochman


v1.5.1
------

:date: 2016-05-11

This is a backwards compatible, minor release.

Fixes in this release:

* Fix for binary attribute handling of unprocessed items data corruption affecting users of 1.5.0 (#126 fixes #125)

Contributors to this release:

* @danielhochman


v1.5.0
------

:date: 2016-05-09

This is a backwards compatible, minor release.

Please consider the fix for limits before upgrading. Correcting for off-by-one when querying is
no longer necessary.

Fixes in this release:

* Fix off-by-one error for limits when querying (#123 fixed #95)
* Retry on ConnectionErrors and other types of RequestExceptions (#121 fixes #98)
* More verbose logging when receiving errors e.g. InternalServerError from the DynamoDB API (#115)
* Prevent permanent poisoning of credential cache due to botocore bug (#113 fixes #99)
* Fix for UnprocessedItems serialization error (#114 fixes #103)
* Fix parsing issue with newer version of dateutil and UTCDateTimeAttributes (#110 fixes #109)
* Correctly handle expected value generation for set types (#107 fixes #102)
* Use HTTP proxies configured by botocore (#100 fixes #92)

New features in this release:

* Return the cause of connection exceptions to the caller (#108 documented by #112)
* Configurable session class for custom connection pool size, etc (#91)
* Add attributes_to_get and consistent_read to more of the API (#79)

Contributors to this release:

* @ab
* @danielhochman
* @jlafon
* @joshowen
* @jpinner-lyft
* @mxr
* @nickgravgaard


v1.4.4
------

:date: 2015-11-10

This is a backward compatible, minor release.

Changes in this release:

* Support for enabling table streams at table creation time (thanks to @brln)
* Fixed bug where a value was always required for update_item when action was 'delete' (#90)


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
* Fixed bug for zope interface compatibility (#71)
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

* Fixed bug where a local variable could be unbound (#67).


v1.4.0
------

:date: 2015-06-23

This is a minor release, with backward compatible bug fixes.

Bugs fixed in this release:

* Added support for botocore 1.0.0 (#63)
* Fixed bug where Model.get() could fail in certain cases (#64)
* Fixed bug where JSON strings weren't being encoded properly (#61)


v1.3.7
------

:date: 2015-04-06

This is a backward compatible, minor bug fix release.

Bugs fixed in this release:

* Fixed bug where range keys were not included in update_item (#59)
* Fixed documentation bug (#58)


v1.3.6
------

:date: 2015-04-06

This is a backward compatible, minor bug fix release.

Bugs fixed in this release:

* Fixed bug where arguments were used incorrectly in update_item (#54)
* Fixed bug where falsy values were used incorrectly in model constructors (#57), thanks @pior
* Fixed bug where the limit argument for scan and query was not always honored.

New features:

* Table counts with optional filters can now be queried using ``Model.count(**filters)``


v1.3.5
------

This is a backward compatible, minor bug fix release.

Bugs fixed in this release.

* Fixed bug where scan did not properly limit results (#45)
* Fixed bug where scan filters were not being preserved (#44)
* Fixed bug where items were mutated as an unexpected side effect (#47)
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

* Fixed bug with Python 2.6 compatibility (#28)
* Fixed bug where update_item was incorrectly checking attributes for null (#34)

Other minor improvements

* New API for backing up and restoring tables
* Better support for custom attributes (https://github.com/pynamodb/PynamoDB/commit/0c2ba5894a532ed14b6c14e5059e97dbb653ff12)
* Explicit Travis CI testing of Python 2.6, 2.7, 3.3, 3.4, and PyPy
* Tests added for round tripping unicode values


v1.3.2
------

:date: 2014-7-02

* This is a minor bug fix release, fixing a bug where query filters were incorrectly parsed (#26).

v1.3.1
------

:date: 2014-05-26

* This is a bug fix release, ensuring that KeyCondition and QueryFilter arguments are constructed correctly (#25).
* Added an example URL shortener to the examples.
* Minor documentation fixes.


v1.3.0
------

:date: 2014-05-20

* This is a minor release, with new backward compatible features and bug fixes.
* Fixed bug where NULL and NOT_NULL were not set properly in query and scan operations (#24)
* Support for specifying the index_name as a Index.Meta attribute (#23)
* Support for specifying read and write capacity in Model.Meta (#22)


v1.2.2
------

:date: 2014-05-14

* This is a minor bug fix release, resolving #21 (key_schema ordering for create_table).

v1.2.1
------

:date: 2014-05-07

* This is a minor bug fix release, resolving #20.

v1.2.0
------

:date: 2014-05-06

* Numerous documentation improvements
* Improved support for conditional operations
* Added support for filtering queries on non key attributes (http://aws.amazon.com/blogs/aws/improved-queries-and-updates-for-dynamodb/)
* Fixed issue with JSON loading where escaped characters caused an error (#17)
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
