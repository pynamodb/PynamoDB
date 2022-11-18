"""
A PynamoDB example using a custom attribute
"""
import pickle
from typing import Any

from pynamodb.attributes import Attribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.constants import BINARY
from pynamodb.models import Model


class Color(object):
    """
    This class is used to demonstrate the PickleAttribute below
    """
    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return "<Color: {}>".format(self.name)


class PickleAttribute(Attribute[object]):
    """
    This class will serializer/deserialize any picklable Python object.
    The value will be stored as a binary attribute in DynamoDB.
    """
    attr_type = BINARY

    def serialize(self, value: Any) -> bytes:
        return pickle.dumps(value)

    def deserialize(self, value: Any) -> Any:
        return pickle.loads(value)


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
