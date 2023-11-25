:orphan:

.. _upgrading_binary:

Upgrading Binary(Set)Attribute
==============================

.. warning::

    The behavior of :py:class:`~pynamodb.attributes.BinaryAttribute` and
    :py:class:`~pynamodb.attributes.BinarySetAttribute` has changed in backwards-incompatible ways
    as of the 6.0 release of PynamoDB.

    To prevent data corruption, use :code:`legacy_encoding=True` for **existing** binary attributes.

Context
#######

PynamoDB version 5 (and lower) had two bugs in the way they handled binary attributes,
which were addressed in PynamoDB 6:

- Top-level binary attributes (i.e. within a :py:class:`~pynamodb.models.Model`) were being Base64-encoded
  twice. For elements in BinarySetAttribute, each element was being encoded twice.

  This behavior was an oversight and resulted in larger item sizes and non-standard semantics.

  .. admonition:: For example...

     The 4 bytes :code:`CA FE F0 0D` should've been sent over the wire as :code:`yv7wDQ==` (single round
     of Base64 encoding). Server-side, they would've been decoded and stored as 4 bytes. Instead, they were put through an extra
     round of Base64 encoding, thus sending :code:`eXY3d0RRPT0=` over the wire. Server-side, they decoded into :code:`yv7wDQ==`
     (in bytes, :code:`79 76 37 77 44 51 3D 3D`) and stored as 8 bytes.

- Nested binary attributes (i.e. within a :py:class:`~pynamodb.attributes.MapAttribute` and :py:class:`~pynamodb.attributes.ListAttribute`)
  were being wrapped in an additional layer of Base64 encoding on every serialization roundtrip.

  Not only it prevented them from being deserialized correctly, but also the model would also grow
  in size exponentially until it hit the DynamoDB item limit of 400KB. For this reason we conclude
  that :code:`BinaryAttribute` and :code:`BinarySetAttribute` were not used in practice within maps and lists
  before PynamoDB 6.0 and thus there is no practical reason you would want :code:`legacy_encoding=True` for them.


Guidance
########

Top-level binary attributes
***************************

- In models existing at the time of an upgrade from PynamoDB 5 (or lower), use :code:`legacy_encoding=True`.

  .. note::

     In PynamoDB 6 we require this new parameter to be explicitly set to prevent inadvertent data corruption
     during upgrades. By setting it to :code:`True` during an upgrade, the developer marks the attribute as pre-existing
     and thus requiring legacy handling.

  For example:

  .. code-block:: diff

      class SomeExistingModel(Model):
     -  my_binary = BinaryAttribute()
     +  my_binary = BinaryAttribute(legacy_encoding=True)

  .. code-block:: diff

      class SomeExistingModel(Model):
     -  my_binary = BinarySetAttribute()
     +  my_binary = BinarySetAttribute(legacy_encoding=True)

  After the version upgrade is complete, you can consider adding a new binary attribute
  and :ref:`migrating the data <migrating>`.

- In new models, use :code:`legacy_encoding=False`.

  .. code-block:: python

     class NewModel(Model):
       my_binary = BinaryAttribute(legacy_encoding=False)
       my_binary_set = BinarySetAttribute(legacy_encoding=False)


Nested binary attributes
************************

- In maps, use :code:`legacy_encoding=False`.

  .. code-block:: python

     class MyMap(MapAttribute):
       binary = BinaryAttribute(legacy_encoding=False)
       binary_set = BinarySetAttribute(legacy_encoding=False)

- In raw maps, normal (non-legacy) encoding will be used.

  .. code-block:: python

     class MyModel(Model):
       my_raw_map = MapAttribute()

     my_model = MyModel()
     my_model.my_raw_map = MapAttribute(binary=b'foo')

- In lists, normal (non-legacy) encoding will be used.

  This applies to both :code:`ListAttribute(of=BinaryAttribute)` and
  :code:`of=BinarySetAttribute` as well as when :code:`of=...`
  is not specified (for :code:`bytes` and :code:`Set[bytes]` elements).

  For example:

  .. code-block:: python

     class MyModel(Model):
       binary_list = ListAttribute(of=BinaryAttribute)
       binary_set_list = ListAttribute(of=BinarySetAttribute)
       mixed_list = ListAttribute()


     model = MyModel()
     model.binary_list = [b'\xCA', b'\xFE']
     model.binary_set_list = [{b'\xCA', b'\xFE'}, {b'\xF0', b'\x0D'}]
     model.mixed_list = [
        b'\xCA\xFE',
        {b'\xF0', b'\x0D'},
     ]


.. _migrating:

Migrating
#########

Since PynamoDB 6 is compatible with existing data through :code:`legacy_encoding=True`, you do not need
to migrate data during an upgrade. Whether you want to migrate data depends on your use case.
Advantages include smaller item sizes and more standardized serialization. However, for large tables,
there might be significant cost and engineering complexity involved.

 .. warning::

    Be sure to have an up-to-date backup of your data.

These are the typical steps to migrate an attribute:

1. Double-write to both the old and new attribute. Read from the new, falling back to the old.

  .. code-block:: python

     class SomeExistingModel(Model):
        _my_binary_v1 = BinaryAttribute(legacy_encoding=True, attr_name='my_binary')
        _my_binary_v2 = BinaryAttribute(legacy_encoding=False, attr_name='my_binary_v2')

        @property
        def my_binary() -> bytes:
          return self._my_binary_v1 if self._my_binary_v2 is None else self._my_binary_v2

        @my_binary.setter
        def my_binary(value: bytes) -> None:
          self._my_binary_v1 = value
          self._my_binary_v2 = value

        def save(self, *args, **kwargs):
          self.my_binary_v2 = self._my_binary_v1
          return super().save(*args, **kwargs)

2. Change the old attribute to be optional:

   .. code-block:: diff

      class SomeExistingModel(Model):
     -   _my_binary_v1 = BinaryAttribute(legacy_encoding=True, attr_name='my_binary')
     +   _my_binary_v1 = BinaryAttribute(legacy_encoding=True, attr_name='my_binary', null=True)

   and rather than double-write to it, unset it by assigning :code:`None`:

   .. code-block:: diff

       @my_binary.setter
       def my_binary(value: bytes) -> None:
      -  self._my_binary_v1 = value
      +  self._my_binary_v1 = None
         self._my_binary_v2 = value

       def save(self, *args, **kwargs):
      -  self.my_binary_v2 = self._my_binary_v1
      +  if self._my_binary_v1 is not None:
      +    self.my_binary_v2 = self._my_binary_v1
      +    self._my_binary_v1 = None
         return super().save(*args, **kwargs)


   At this point, you can either let natural migration run its course (as your online system
   re-saves models), or you can perform a one-time migration by scanning the table and
   re-saving each item.

3. Once migration is done, remove the old attribute and all migration logic.

  .. code-block:: python

     class SomeExistingModel(Model):
        my_binary = BinaryAttribute(legacy_encoding=False, attr_name='my_binary_v2')
