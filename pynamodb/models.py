"""
DynamoDB Models for PynamoDB
"""
import six

from .attributes import Attribute
from .connection.base import MetaTable
from .connection.table import TableConnection
from .connection.util import pythonic
from .types import HASH, RANGE
from pynamodb.constants import (
    ATTR_TYPE_MAP, ATTR_DEFINITIONS, ATTR_NAME, ATTR_TYPE, KEY_SCHEMA,
    KEY_TYPE, ITEM, ITEMS, READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS,
    RANGE_KEY, ATTRIBUTES
)


class Model(object):
    """
    Defines a pynamodb model
    """
    table_name = None
    hash_key = None
    meta_table = None
    range_key = None
    attributes = None
    connection = None

    def __init__(self, hash_key=None, range_key=None, **attrs):
        self.attribute_values = {}
        self.set_defaults()
        if hash_key:
            self.attribute_values[self.meta().hash_keyname] = hash_key
        if range_key:
            self.attribute_values[self.meta().range_keyname] = range_key
        self.set_attributes(**attrs)

    def __getattribute__(self, item):
        """
        Smarter than the average attribute
        """
        values = object.__getattribute__(self, 'attribute_values')
        if item in values:
            return values[item]
        else:
            return object.__getattribute__(self, item)

    def set_defaults(self):
        """
        Sets and fields that provide a default value
        """
        for name, attr in self.get_attributes().items():
            default = attr.default
            if callable(default):
                value = default()
            else:
                value = default
            if value:
                self.attribute_values[name] = value

    def set_attributes(self, **attrs):
        """
        Sets the attributes for this object
        """
        for key, value in attrs.items():
            self.attribute_values[key] = value

    def __repr__(self):
        hash_key = self.attribute_values.get(self.meta().hash_keyname, None)
        if hash_key and self.table_name:
            if self.meta().range_keyname:
                range_key = self.attribute_values.get(self.meta().range_keyname, None)
                msg = "{0}<{1}, {2}>".format(self.table_name, hash_key, range_key)
            else:
                msg = "{0}<{1}>".format(self.table_name, hash_key)
            return six.u(msg)
        else:
            return six.u("Model")

    @classmethod
    def meta(cls):
        """
        A helper object that contains meta data about this table
        """
        if cls.meta_table is None:
            cls.meta_table = MetaTable(cls.get_connection().describe_table())
        return cls.meta_table

    @classmethod
    def get_connection(cls):
        """
        Returns a (cached) connection
        """
        if cls.connection is None:
            cls.connection = TableConnection(cls.table_name)
        return cls.connection

    def save(self):
        """
        Save this object to dynamodb
        """
        args, kwargs = self._get_save_args()
        return self.get_connection().put_item(*args, **kwargs)

    def _get_save_args(self, attributes=True):
        """
        Gets the proper *args, **kwargs for saving and retrieving this object
        """
        kwargs = {}
        serialized = self.serialize()
        hash_key = serialized.get(HASH)
        range_key = serialized.get(RANGE, None)
        args = (hash_key, )
        if range_key:
            kwargs[pythonic(range_key)] = range_key
        if attributes:
            kwargs[pythonic(ATTRIBUTES)] = serialized[pythonic(ATTRIBUTES)]
        return args, kwargs

    def update(self):
        """
        Retrieves this object's data from dynamodb and syncs this local object
        """
        args, kwargs = self._get_save_args(attributes=False)
        attrs = self.get_connection().get_item(*args, **kwargs)
        self.deserialize(attrs.get(ITEM, {}))

    def deserialize(self, attrs):
        """
        Sets attributes sent back from dynamodb on this object
        """
        for name, attr in attrs.items():
            attr_instance = self.get_attributes().get(name, None)
            if attr_instance:
                attr_type = ATTR_TYPE_MAP[attr_instance.attr_type]
                value = attr.get(attr_type, None)
                if value:
                    self.attribute_values[name] = attr_instance.deserialize(value)

    def serialize(self):
        """
        Serializes a value for use with DynamoDB
        """
        attributes = pythonic(ATTRIBUTES)
        attrs = {attributes: {}}
        for name, attr in self.get_attributes().items():
            value = self.attribute_values.get(name)
            if value is None:
                if attr.null:
                    continue
                else:
                    raise ValueError("Attribute '{0}' cannot be None".format(name))
            if attr.is_hash_key:
                attrs[HASH] = value
            elif attr.is_range_key:
                attrs[RANGE] = value
            else:
                attrs[attributes][name] = {
                    ATTR_TYPE_MAP[attr.attr_type]: attr.serialize(value)
                }
        return attrs

    @classmethod
    def get(cls,
            hash_key,
            range_key=None,
            consistent_read=False):
        """
        Returns a single object using the provided keys
        """
        data = cls.get_connection().get_item(
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read).get(ITEM)
        return cls.from_raw_data(data)

    @classmethod
    def from_raw_data(cls, data):
        """
        Returns an instance of this class
        from the raw data
        """
        if data is None:
            raise ValueError("Received no data to construct object")
        hash_keyname = cls.meta().hash_keyname
        range_keyname = cls.meta().range_keyname
        hash_key_type = cls.meta().get_attribute_type(hash_keyname)
        args = (data.pop(hash_keyname).get(hash_key_type),)
        kwargs = {}
        if range_keyname:
            range_key_type = cls.meta().get_attribute_type(range_keyname)
            kwargs['range_key'] = data.pop(range_keyname).get(range_key_type)
        for name, value in data.items():
            attr = cls.get_attributes().get(name, None)
            if attr:
                kwargs[name] = attr.deserialize(value.get(ATTR_TYPE_MAP[attr.attr_type]))
        return cls(*args, **kwargs)

    @classmethod
    def get_attributes(cls):
        """
        Returns the list of attributes for this class
        """
        if cls.attributes is None:
            cls.attributes = {}
            for item in dir(cls):
                item_cls = getattr(cls, item).__class__
                if issubclass(item_cls, (Attribute, )):
                    cls.attributes[item] = getattr(cls, item)
        return cls.attributes

    @classmethod
    def schema(cls):
        """
        Returns the schema for this table
        """
        schema = {
            pythonic(ATTR_DEFINITIONS): [],
            pythonic(KEY_SCHEMA): []
        }
        for attr_name, attr_cls in cls.get_attributes().items():
            if attr_cls.is_hash_key or attr_cls.is_range_key:
                schema[pythonic(ATTR_DEFINITIONS)].append({
                    pythonic(ATTR_NAME): attr_name,
                    pythonic(ATTR_TYPE): ATTR_TYPE_MAP[attr_cls.attr_type]
                })
            if attr_cls.is_hash_key:
                schema[pythonic(KEY_SCHEMA)].append({
                    pythonic(KEY_TYPE): HASH,
                    pythonic(ATTR_NAME): attr_name
                })
            elif attr_cls.is_range_key:
                schema[pythonic(KEY_SCHEMA)].append({
                    pythonic(KEY_TYPE): RANGE,
                    pythonic(ATTR_NAME): attr_name
                })
        return schema

    @classmethod
    def scan(cls):
        """
        Iterates through all items in the table
        """
        data = cls.get_connection().scan()
        for item in data.get(ITEMS):
            yield cls.from_raw_data(item)

    @classmethod
    def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None):
        """
        Create the table for this model
        """
        schema = cls.schema()
        schema[pythonic(READ_CAPACITY_UNITS)] = read_capacity_units
        schema[pythonic(WRITE_CAPACITY_UNITS)] = write_capacity_units
        return cls.get_connection().create_table(
            **schema
        )