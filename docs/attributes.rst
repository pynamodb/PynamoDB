Custom Attributes
==========================

Attributes in PynamoDB are classes that are serialized to and from DynamoDB attributes. PynamoDB provides attribute classes
for all of the basic DynamoDB data types, as defined in the `DynamoDB documentation <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html>`_.
Higher level attribute types (internally stored as a DynamoDB data types) can be defined with PynamoDB. Two such types
are included with PynamoDB for convenience: `JSONAttribute` and `UnicodeDatetimeAttribute`.

Attribute Methods
-----------------

All `Attribute` classes must define two methods, `serialize` and `deserialize`. The `serialize` method takes a Python
value and converts it into a format that can be stored into DynamoDB. The `deserialize` method takes a raw DynamoDB value
and converts it back into its value in Python. Additionally, a class attribute called `attr_type` is required for PynamoDB
to know which DynamoDB data type the attribute is stored as.


Writing your own attribute
--------------------------

You can write your own attribute class which defines the necessary methods like this:

.. code-block:: python

    from pynamodb.attributes import Attribute
    from pynamodb.constants import BINARY

    class CustomAttribute(Attribute):
        """
        A custom model attribute
        """

        # This tells PynamoDB that the attribute is stored in DynamoDB as a binary
        # attribute
        attr_type = BINARY

        def serialize(value):
            # convert the value to binary and return it

        def deserialize(value):
            # convert the value from binary back into whatever type you require


Custom Attribute Example
------------------------

The example below shows how to write a custom attribute that will pickle a customized class. The attribute itself is stored
in DynamoDB as a binary attribute. The `pickle` module is used to serialize and deserialize the attribute. In this example,
it is not necessary to define `attr_type` because the `PickleAttribute` class is inheriting from `BinaryAttribute` which has
already defined it.

.. code-block:: python

    import pickle
    from pynamodb.attributes import BinaryAttribute, UnicodeAttribute
    from pynamodb.models import Model


    class Color(object):
        """
        This class is used to demonstrate the PickleAttribute below
        """
        def __init__(self, name):
            self.name = name

        def __str__(self):
            return "<Color: {}>".format(self.name)


    class PickleAttribute(BinaryAttribute):
        """
        This class will serializer/deserialize any picklable Python object.
        The value will be stored as a binary attribute in DynamoDB.
        """
        def serialize(self, value):
            """
            The super class takes the binary string returned from pickle.dumps
            and encodes it for storage in DynamoDB
            """
            return super(PickleAttribute, self).serialize(pickle.dumps(value))

        def deserialize(self, value):
            return pickle.loads(super(PickleAttribute, self).deserialize(value))


    class CustomAttributeModel(Model):
        """
        A model with a custom attribute
        """
        class Meta:
            host = 'http://localhost:8000'
            table_name = 'custom_attr'
            read_capacity_units = 1
            write_capacity_units = 1

        id = UnicodeAttribute(hash_key=True)
        obj = PickleAttribute()

Now we can use our custom attribute to round trip any object that can be pickled.

.. code-block:: python

    >>>instance = CustomAttributeModel()
    >>>instance.obj = Color('red')
    >>>instance.id = 'red'
    >>>instance.save()

    >>>instance = CustomAttributeModel.get('red')
    >>>print(instance.obj)
    <Color: red>