Upgrading
=========

This file complements the :ref:`release notes <release_notes>`, focusing on helping safe upgrades of the library
in production scenarios.

PynamoDB 5.x to 6.x
-------------------

BinaryAttribute is no longer double base64-encoded
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :ref:`upgrading_binary` for details.

PynamoDB 4.x to 5.x
-------------------

Null checks enforced where they weren't previously
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously null errors (persisting ``None`` into an attribute defined as ``null=False``) were ignored inside **nested** map attributes, e.g.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import ListAttribute, MapAttribute, UnicodeAttribute

    class Employee(MapAttribute):
      name = UnicodeAttribute(null=False)

    class Team(Model):
      employees = ListAttribute(of=Employee)


    team = Team()
    team.employees = [Employee(name=None)]
    team.save()  # this will raise now


Now these will resulted in an :py:class:`~pynamodb.exceptions.AttributeNullError` being raised.

This was an unintentional breaking change introduced in 5.0.3.

Empty values are now meaningful
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:class:`~pynamodb.attributes.UnicodeAttribute` and :py:class:`~pynamodb.attributes.BinaryAttribute` now support empty values (:pr:`830`)

In previous versions, assigning an empty value to would be akin to assigning ``None``: if the attribute was defined with ``null=True`` then it would be omitted, otherwise an error would be raised.
DynamoDB `added support <https://aws.amazon.com/about-aws/whats-new/2020/05/amazon-dynamodb-now-supports-empty-values-for-non-key-string-and-binary-attributes-in-dynamodb-tables/>`_ empty values
for String and Binary attributes. This release of PynamoDB starts treating empty values like any other values. If existing code unintentionally assigns empty values to StringAttribute or BinaryAttribute,
this may be a breaking change: for example, the code may rely on the fact that in previous versions empty strings would be "read back" as ``None`` values when reloaded from the database.

No longer parsing date-time strings leniently
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:class:`~pynamodb.attributes.UTCDateTimeAttribute` now strictly requires the date string format ``'%Y-%m-%dT%H:%M:%S.%f%z'`` to ensure proper ordering.
PynamoDB has always written values with this format but previously would accept reading other formats.
Items written using other formats must be rewritten before upgrading.

Removed functionality
~~~~~~~~~~~~~~~~~~~~~

The following changes are breaking but are less likely to go unnoticed:

* Python 2 is no longer supported. Python 3.6 or greater is now required.
* Table backup functionality (``Model.dump[s]`` and ``Model.load[s]``) has been removed.
* ``Model.query`` no longer converts unsupported range key conditions into filter conditions.
* Internal attribute type constants are replaced with their "short" DynamoDB version (:pr:`827`)
* Remove ``ListAttribute.remove_indexes`` (added in v4.3.2) and document usage of remove for list elements (:pr:`838`)
* Remove ``pynamodb.connection.util.pythonic`` (:pr:`753`) and (:pr:`865`)
* Remove ``ModelContextManager`` class (:pr:`861`)

PynamoDB 3.x to 4.x
-------------------

Requests Removal
~~~~~~~~~~~~~~~~

Given that ``botocore`` has moved to using ``urllib3`` directly for making HTTP requests, we'll be doing the same (via ``botocore``). This means the following:

* The ``session_cls`` option is no longer supported.
* The ``request_timeout_seconds`` parameter is no longer supported. ``connect_timeout_seconds`` and ``read_timeout_seconds`` are available instead.

  + Note that the timeouts for connection and read are now ``15`` and ``30`` seconds respectively. This represents a change from the previous ``60`` second combined ``requests`` timeout.
* *Wrapped* exceptions (i.e ``exc.cause``) that were from ``requests.exceptions`` will now be comparable ones from ``botocore.exceptions`` instead.

Key attribute types must match table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The previous release would call `DescribeTable` to discover table metadata
and would use the key types as defined in the DynamoDB table. This could obscure
type mismatches e.g. where a table's hash key is a number (`N`) in DynamoDB,
but defined in PynamoDB as a `UnicodeAttribute`.

With this release, we're always using the PynamoDB model's definition
of all attributes including the key attributes.

Deprecation of old APIs
~~~~~~~~~~~~~~~~~~~~~~~

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

PynamoDB 2.x to 3.x
--------------------

Changes to UnicodeSetAttribute
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :ref:`upgrading_unicodeset` for details.
