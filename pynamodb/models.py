"""
DynamoDB Models for PynamoDB
"""
import six
from .connection.base import MetaTable
from .connection.table import TableConnection
from .connection.util import pythonic
from .types import HASH, RANGE
from .connection.constants import (
    STRING, NUMBER, BINARY, ATTR_TYPE_MAP, ATTR_DEFINITIONS, ATTR_NAME, ATTR_TYPE,
    KEY_SCHEMA, KEY_TYPE)


class Attribute(object):
    """
    An attribute of a model
    """
    def __init__(self,
                 attr_type=str,
                 hash_key=False,
                 range_key=False,
                 null=False
                 ):
        self.null = null
        self.attr_type = attr_type
        self.is_hash_key = hash_key
        self.is_range_key = range_key

    def serialize(self, value):
        """
        This method should return a dynamodb compatible value
        """
        return value

class BinaryAttribute(Attribute):
    """
    A binary attribute
    """
    def __init__(self, **kwargs):
        super(BinaryAttribute, self).__init__(
            attr_type=BINARY,
            **kwargs
        )

    def serialize(self, value):
        """
        Returns a utf-8 encoded binary string
        """
        return six.b(value)

class UnicodeAttribute(Attribute):
    """
    A unicode attribute
    """
    def __init__(self, **kwargs):
        super(UnicodeAttribute, self).__init__(
            attr_type=STRING,
            **kwargs
        )

    def serialize(self, value):
        """
        Returns a unicode string
        """
        return six.u(value)


class NumberAttribute(Attribute):
    """
    A number attribute
    """
    name = None
    def __init__(self, **kwargs):
        super(NumberAttribute, self).__init__(
            attr_type=NUMBER,
            **kwargs
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
        if hash_key:
            self.attribute_values[self.meta().hash_keyname] = hash_key
        if range_key:
            self.attribute_values[self.meta().range_keyname] = range_key
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
        kwargs = {}
        serialized = self.serialize()
        hash_key = serialized.get(HASH)
        range_key = serialized.get(RANGE, None)
        args = (hash_key, )
        if range_key:
            kwargs['range_key'] = range_key
        kwargs['attributes'] = serialized['attributes']
        return self.get_connection().put_item(*args, **kwargs)

    def serialize(self):
        """
        Serializes a value for use with DynamoDB
        """
        attributes = 'attributes'
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
                attrs[attributes][name] = attr.serialize(value)
        return attrs

    @classmethod
    def get(cls,
            hash_key,
            range_key=None,
            consistent_read=False):
        """
        Returns a single object using the provided keys
        """
        return cls.get_connection().get_item(
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read)

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
    def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None):
        """
        Create the table for this model
        """
        schema = cls.schema()
        schema['read_capacity_units'] = read_capacity_units
        schema['write_capacity_units'] = write_capacity_units
        return cls.get_connection().create_table(
            **schema
        )