Upgrading UnicodeSetAttribute
=============================

.. warning::

    The behavior of 'UnicodeSetAttribute' has changed in backwards-incompatible ways
    as of the 1.6.0 and 3.0.1 releases of PynamoDB.

The following steps can be used to safely update PynamoDB assuming that the data stored
in the item's UnicodeSetAttribute is not JSON. If JSON is being stored, these steps will
not work and a custom migration plan is required. Be aware that values such as numeric
strings (i.e. "123") are valid JSON.

When upgrading services that use PynamoDB with tables that contain UnicodeSetAttributes
with a version < 1.6.0, first deploy version 1.5.4 to prepare the read path for the new
serialization format.

Once all services that read from the tables have been deployed, then deploy version 2.2.0
and migrate your data using the provided convenience methods on the Model.
(Note: these methods are only available in version 2.2.0)

.. code-block:: python

    def get_save_kwargs(item):
        # any conditional args needed to ensure data does not get overwritten
        # for example if your item has a `version` attribute
        {'version__eq': item.version}

    # Re-serialize all UnicodeSetAttributes in the table by scanning all items.
    # See documentation of fix_unicode_set_attributes for rate limiting options
    # to avoid exceeding provisioned capacity.
    Model.fix_unicode_set_attributes(get_save_kwargs)

    # Verify the migration is complete
    print("Migration Complete? " + Model.needs_unicode_set_fix())

Once all data has been migrated then upgrade to a version >= 3.0.1.
