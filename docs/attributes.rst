Custom Attributes
==========================

Attributes in PynamoDB are classes that are serialized to and from DynamoDB attributes. PynamoDB provides attribute classes
for all DynamoDB data types, as defined in the `DynamoDB documentation <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html>`_.
Higher level attribute types (internally stored as a DynamoDB data types) can be defined with PynamoDB. Two such types
are included with PynamoDB for convenience: ``JSONAttribute`` and ``UnicodeDatetimeAttribute``.

Attribute Methods
-----------------

All ``Attribute`` classes must define three methods, ``serialize``, ``deserialize`` and ``get_value``. The ``serialize`` method takes a Python
value and converts it into a format that can be stored into DynamoDB. The ``get_value`` method reads the serialized value out of the DynamoDB record.
This raw value is then passed to the ``deserialize`` method. The ``deserialize`` method then converts it back into its value in Python.
Additionally, a class attribute called ``attr_type`` is required for PynamoDB to know which DynamoDB data type the attribute is stored as.
The ``get_value`` method is provided to help when migrating from one attribute type to another, specifically with the ``BooleanAttribute`` type.
If you're writing your own attribute and the ``attr_type`` has not changed you can simply use the base ``Attribute`` implementation of ``get_value``.


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
in DynamoDB as a binary attribute. The ``pickle`` module is used to serialize and deserialize the attribute. In this example,
it is not necessary to define ``attr_type`` because the ``PickleAttribute`` class is inheriting from ``BinaryAttribute`` which has
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


List Attributes
---------------

DynamoDB list attributes are simply lists of other attributes. DynamoDB asserts no requirements about the types embedded within the list.
Creating an untyped list is done like so:

.. code-block:: python

    from pynamodb.attributes import ListAttribute, NumberAttribute, UnicodeAttribute

    class GroceryList(Model):
        class Meta:
            table_name = 'GroceryListModel'

        store_name = UnicodeAttribute(hash_key=True)
        groceries = ListAttribute()

    # Example usage:

    GroceryList(store_name='Haight Street Market',
                groceries=['bread', 1, 'butter', 6, 'milk', 1])

PynamoDB can provide type safety if it is required. Currently PynamoDB does not allow type checks on anything other than ``MapAttribute`` and subclasses of ``MapAttribute``. We're working on adding more generic type checking in a future version.
When defining your model use the ``of=`` kwarg and pass in a class. PynamoDB will check that all items in the list are of the type you require.

.. code-block:: python

    from pynamodb.attributes import ListAttribute, NumberAttribute


    class OfficeEmployeeMap(MapAttribute):
        office_employee_id = NumberAttribute()
        person = UnicodeAttribute()


    class Office(Model):
        class Meta:
            table_name = 'OfficeModel'
        office_id = NumberAttribute(hash_key=True)
        employees = ListAttribute(of=OfficeEmployeeMap)

    # Example usage:

    emp1 = OfficeEmployeeMap(
        office_employee_id=123,
        person='justin'
    )
    emp2 = OfficeEmployeeMap(
        office_employee_id=125,
        person='lita'
    )
    emp4 = OfficeEmployeeMap(
        office_employee_id=126,
        person='garrett'
    )

    Office(
        office_id=3,
        employees=[emp1, emp2, emp3]
    ).save()  # persists

    Office(
        office_id=3,
        employees=['justin', 'lita', 'garrett']
    ).save()  # raises ValueError

Map Attributes
--------------

DynamoDB map attributes are objects embedded inside of top level models. See the examples `here <https://github.com/pynamodb/PynamoDB/tree/devel/examples/office_model.py>`_.
When implementing your own MapAttribute you can simply extend ``MapAttribute`` and ignore writing serialization code.
These attributes can then be used inside of Model classes just like any other attribute.

.. code-block:: python

    from pynamodb.attributes import MapAttribute, UnicodeAttribute

    class CarInfoMap(MapAttribute):
        make = UnicodeAttribute(null=False)
        model = UnicodeAttribute(null=True)
