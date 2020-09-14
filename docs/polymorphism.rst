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
        _cls = DiscriminatorAttribute()

    class ChildClass(ParentClass, discriminator='child'):
        pass

Declaring the discriminator value as part of the class definition will automatically register the class with the discriminator attribute.
A class can also be registered manually:

.. code-block:: python

    class ParentClass(MapAttribute):
        _cls = DiscriminatorAttribute()

    class ChildClass(ParentClass):
        pass

    ParentClass._cls.register_class(ChildClass, 'child')
