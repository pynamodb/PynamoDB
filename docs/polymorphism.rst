.. _polymorphism:

Polymorphism
============

PynamoDB supports polymorphism through the use of discriminators.

A discriminator is a value that is written to DynamoDB that identifies the python class being stored.

Discriminator Attributes
^^^^^^^^^^^^^^^^^^^^^^^^

The discriminator value is stored using a special attribute, the DiscriminatorAttribute.
Only a single DiscriminatorAttribute can be defined on a class.

The discriminator value can be assigned to a class as part of the definition:

.. code-block:: python

    class ParentClass(MapAttribute):
        cls = DiscriminatorAttribute()

    class ChildClass(ParentClass, discriminator='child'):
        pass

Declaring the discriminator value as part of the class definition will automatically register the class with the discriminator attribute.
A class can also be registered manually:

.. code-block:: python

    class ParentClass(MapAttribute):
        cls = DiscriminatorAttribute()

    class ChildClass(ParentClass):
        pass

    ParentClass._cls.register_class(ChildClass, 'child')

.. note::

    A class may be registered with a discriminator attribute multiple times.
    Only the first registered value is used during serialization;
    however, any registered value can be used to deserialize the class.
    This behavior is intended to facilitate migrations if discriminator values must be changed.

.. warning::

    Discriminator values are written to DynamoDB.
    Changing the value after items have been saved to the database can result in deserialization failures.
    In order to read items with an old discriminator value, the old value must be manually registered.


Model Discriminators
^^^^^^^^^^^^^^^^^^^^

Model classes also support polymorphism through the use of discriminators.
(Note: currently discriminator attributes cannot be used as the hash or range key of a table.)

.. code-block:: python

    class ParentModel(Model):
        class Meta:
            table_name = 'polymorphic_table'
        id = UnicodeAttribute(hash_key=True)
        cls = DiscriminatorAttribute()

    class FooModel(ParentModel, discriminator='Foo'):
        foo = UnicodeAttribute()

    class BarModel(ParentModel, discriminator='Bar'):
        bar = UnicodeAttribute()

    BarModel(id='Hello', bar='World!').serialize()
    # {'id': {'S': 'Hello'}, 'cls': {'S': 'Bar'}, 'bar': {'S': 'World!'}}
.. note::

    Read operations that are performed on a class that has a discriminator value are slightly modified to ensure that only instances of the class are returned.
    Query and scan operations transparently add a filter condition to ensure that only items with a matching discriminator value are returned.
    Get and batch get operations will raise a ``ValueError`` if the returned item(s) are not a subclass of the model being read.
