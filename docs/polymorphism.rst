Polymorphism
============

PynamoDB supports polymorphism through the use of discriminators.

A discriminator is a value that is written to DynamoDB that identifies the python class being stored.
(Note: currently discriminators are only supported on MapAttribute subclasses; support for model subclasses coming soon.)

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
