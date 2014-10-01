"""
A PynamoDB example using a custom attribute
"""
from __future__ import print_function
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


# Create the example table
if not CustomAttributeModel.exists():
    CustomAttributeModel.create_table(wait=True)


instance = CustomAttributeModel()
instance.obj = Color('red')
instance.id = 'red'
instance.save()

instance = CustomAttributeModel.get('red')
print(instance.obj)
