"""
pynamodb.models
~~~~~~~~~~~~~~~~

DynamoDB Models for PynamoDB
"""

import time
import six
import copy
from .attributes import Attribute
from .connection.base import MetaTable
from .connection.table import TableConnection
from .connection.util import pythonic
from .types import HASH, RANGE
from pynamodb.constants import (
    ATTR_TYPE_MAP, ATTR_DEFINITIONS, ATTR_NAME, ATTR_TYPE, KEY_SCHEMA,
    KEY_TYPE, ITEM, ITEMS, READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS,
    RANGE_KEY, ATTRIBUTES, PUT, DELETE, RESPONSES, GLOBAL_SECONDARY_INDEX,
    LOCAL_SECONDARY_INDEX, INDEX_NAME, PROVISIONED_THROUGHPUT, PROJECTION,
    KEYS_ONLY, ALL, INCLUDE, GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES,
    PROJECTION_TYPE, NON_KEY_ATTRIBUTES, EQ, LE, LT, GT, GE, BEGINS_WITH, BETWEEN,
    COMPARISON_OPERATOR, ATTR_VALUE_LIST, TABLE_DESCRIPTION, TABLE_STATUS, ACTIVE)


class ModelContextManager(object):
    """
    A class for managing batch operations
    """

    def __init__(self, model, auto_commit=True):
        self.model = model
        self.auto_commit = auto_commit
        self.max_operations = 25
        self.pending_operations = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class BatchWrite(ModelContextManager):
    """
    A class for batch writes
    """

    def save(self, put_item):
        if len(self.pending_operations) == self.max_operations:
            if not self.auto_commit:
                raise ValueError("DynamoDB allows a maximum of 25 batch operations")
            else:
                self.commit()
        self.pending_operations.append({"action": PUT, "item": put_item})

    def delete(self, del_item):
        if len(self.pending_operations) == self.max_operations:
            if not self.auto_commit:
                raise ValueError("DynamoDB allows a maximum of 25 batch operations")
            else:
                self.commit()
        self.pending_operations.append({"action": DELETE, "item": del_item})

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.commit()

    def commit(self):
        """
        Writes all of the changes
        """
        put_items = []
        delete_items = []
        attrs_name = pythonic(ATTRIBUTES)
        for item in self.pending_operations:
            if item['action'] == PUT:
                put_items.append(item['item'].serialize(attr_map=True)[attrs_name])
            elif item['action'] == DELETE:
                delete_items.append(item['item'].get_keys())
        self.pending_operations = []
        return self.model.get_connection().batch_write_item(
            put_items=put_items,
            delete_items=delete_items
        )


class Model(object):
    """
    Defines a `PynamoDB` Model

    This model is backed by a table in DynamoDB.
    You can create the table by with the ``create_table`` method.
    """
    table_name = None
    hash_key = None
    meta_table = None
    range_key = None
    attributes = None
    indexes = None
    connection = None

    def __init__(self, hash_key=None, range_key=None, **attrs):
        """
        :param hash_key: Required. The hash key for this object.
        :param range_key: Only required if the table has a range key attribute.
        :param attrs: A dictionary of attributes to set on this object.
        """
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

    @classmethod
    def batch_get(cls, items):
        """
        BatchGetItem for this model
        """
        hash_keyname = cls.meta().hash_keyname
        range_keyname = cls.meta().range_keyname
        keys_to_get = []
        for item in items:
            if range_keyname:
                keys_to_get.append({
                    hash_keyname: item[0],
                    range_keyname: item[1]
                })
            else:
                keys_to_get.append({
                    hash_keyname: item[0]
                })

        data = cls.get_connection().batch_get_item(
            keys_to_get
        ).get(RESPONSES).get(cls.table_name)
        for item in data:
            yield cls.from_raw_data(item)

    @classmethod
    def batch_write(cls, auto_commit=True):
        """
        Returns a context manager for a batch operation
        """
        return BatchWrite(cls, auto_commit=auto_commit)

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

    def delete(self):
        """
        Deletes this object from dynamodb
        """
        args, kwargs = self._get_save_args(attributes=False)
        return self.get_connection().delete_item(*args, **kwargs)

    def save(self):
        """
        Save this object to dynamodb
        """
        args, kwargs = self._get_save_args()
        return self.get_connection().put_item(*args, **kwargs)

    def get_keys(self):
        """
        Returns the proper arguments for deleting
        """
        serialized = self.serialize()
        hash_key = serialized.get(HASH)
        range_key = serialized.get(RANGE, None)
        hash_keyname = self.meta().hash_keyname
        range_keyname = self.meta().range_keyname
        attrs = {
            hash_keyname: hash_key,
            range_keyname: range_key
        }
        return attrs

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
            kwargs[pythonic(RANGE_KEY)] = range_key
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

    def serialize(self, attr_map=False):
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
            if attr_map:
                attrs[attributes][name] = {
                    ATTR_TYPE_MAP[attr.attr_type]: attr.serialize(value)
                }
            else:
                if attr.is_hash_key:
                    attrs[HASH] = attr.serialize(value)
                elif attr.is_range_key:
                    attrs[RANGE] = attr.serialize(value)
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
            consistent_read=consistent_read
        )
        if data:
            return cls.from_raw_data(data.get(ITEM))
        else:
            return None

    @classmethod
    def from_raw_data(cls, data):
        """
        Returns an instance of this class
        from the raw data
        """
        mutable_data = copy.copy(data)
        if mutable_data is None:
            raise ValueError("Received no mutable_data to construct object")
        hash_keyname = cls.meta().hash_keyname
        range_keyname = cls.meta().range_keyname
        hash_key_type = cls.meta().get_attribute_type(hash_keyname)
        args = (mutable_data.pop(hash_keyname).get(hash_key_type),)
        kwargs = {}
        if range_keyname:
            range_key_type = cls.meta().get_attribute_type(range_keyname)
            kwargs['range_key'] = mutable_data.pop(range_keyname).get(range_key_type)
        for name, value in mutable_data.items():
            attr = cls.get_attributes().get(name, None)
            if attr:
                kwargs[name] = attr.deserialize(value.get(ATTR_TYPE_MAP[attr.attr_type]))
        return cls(*args, **kwargs)

    @classmethod
    def get_indexes(cls):
        """
        Returns a list of the secondary indexes
        """
        if cls.indexes is None:
            cls.indexes = {
                pythonic(GLOBAL_SECONDARY_INDEXES): [],
                pythonic(LOCAL_SECONDARY_INDEXES): [],
                pythonic(ATTR_DEFINITIONS): []
            }
            for item in dir(cls):
                item_cls = getattr(cls, item).__class__
                if issubclass(item_cls, (Index, )):
                    item_cls = getattr(cls, item)
                    schema = item_cls.schema()
                    idx = {
                        pythonic(INDEX_NAME): item,
                        pythonic(KEY_SCHEMA): schema.get(pythonic(KEY_SCHEMA)),
                        pythonic(PROJECTION): {
                            PROJECTION_TYPE: item_cls.projection.projection_type,
                        },
                        pythonic(PROVISIONED_THROUGHPUT): {
                            READ_CAPACITY_UNITS: item_cls.read_capacity_units,
                            WRITE_CAPACITY_UNITS: item_cls.write_capacity_units
                        }
                    }
                    cls.indexes[pythonic(ATTR_DEFINITIONS)].extend(schema.get(pythonic(ATTR_DEFINITIONS)))
                    if item_cls.projection.non_key_attributes:
                        idx[pythonic(PROJECTION)][NON_KEY_ATTRIBUTES] = item_cls.projection.non_key_attributes
                    if item_cls.index_type == GLOBAL_SECONDARY_INDEX:
                        cls.indexes[pythonic(GLOBAL_SECONDARY_INDEXES)].append(idx)
                    else:
                        cls.indexes[pythonic(LOCAL_SECONDARY_INDEXES)].append(idx)

        return cls.indexes

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
    def query(cls, hash_key, **filters):
        """
        Provides a high level query API
        """
        operators = {
            'eq': EQ,
            'le': LE,
            'lt': LT,
            'ge': GE,
            'gt': GT,
            'begins_with': BEGINS_WITH,
            'between': BETWEEN
        }
        key_conditions = {}
        for query, value in filters.items():
            attribute = None
            for token in query.split('__'):
                if not isinstance(value, list):
                    value = [value]
                if attribute is None:
                    attribute = token
                elif token in operators:
                    key_conditions[attribute] = {
                        COMPARISON_OPERATOR: operators.get(token),
                        ATTR_VALUE_LIST: value
                    }
                else:
                    raise ValueError("Could not parse filter: {0}".format(query))
        data = cls.get_connection().query(
            hash_key,
            key_conditions=key_conditions
        )
        for item in data.get(ITEMS):
            yield cls.from_raw_data(item)

    @classmethod
    def scan(cls):
        """
        Iterates through all items in the table
        """
        data = cls.get_connection().scan()
        for item in data.get(ITEMS):
            yield cls.from_raw_data(item)

    @classmethod
    def exists(cls):
        """
        Returns True if this table exists, False otherwise
        """
        return cls.get_connection().describe_table() is not None

    @classmethod
    def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None):
        """
        Create the table for this model
        """
        if not cls.exists():
            schema = cls.schema()
            schema[pythonic(READ_CAPACITY_UNITS)] = read_capacity_units
            schema[pythonic(WRITE_CAPACITY_UNITS)] = write_capacity_units
            index_data = cls.get_indexes()
            schema[pythonic(GLOBAL_SECONDARY_INDEXES)] = index_data.get(pythonic(GLOBAL_SECONDARY_INDEXES))
            schema[pythonic(LOCAL_SECONDARY_INDEXES)] = index_data.get(pythonic(LOCAL_SECONDARY_INDEXES))
            schema[pythonic(ATTR_DEFINITIONS)].extend(index_data.get(pythonic(ATTR_DEFINITIONS)))
            cls.get_connection().create_table(
                **schema
            )
        if wait:
            while True:
                status = cls.get_connection().describe_table()
                if status:
                    data = status.get(TABLE_STATUS)
                    if data == ACTIVE:
                        return
                    else:
                        time.sleep(2)
                else:
                    raise ValueError("No TableStatus returned for table")


class Index(object):
    """
    Base class for secondary indexes
    """
    projection = None
    attributes = None
    read_capacity_units = None
    write_capacity_units = None
    index_type = None

    def __init__(self):
        if not self.projection:
            raise ValueError("No projection defined, define a projection for this class")

    @classmethod
    def schema(cls):
        """
        Returns the schema for this index
        """
        attr_definitions = []
        schema = []
        for attr_name, attr_cls in cls.get_attributes().items():
            attr_definitions.append({
                pythonic(ATTR_NAME): attr_name,
                pythonic(ATTR_TYPE): ATTR_TYPE_MAP[attr_cls.attr_type]
            })
            if attr_cls.is_hash_key:
                schema.append({
                    ATTR_NAME: attr_name,
                    KEY_TYPE: HASH
                })
            elif attr_cls.is_range_key:
                schema.append({
                    ATTR_NAME: attr_name,
                    KEY_TYPE: RANGE
                })
        return {
            pythonic(KEY_SCHEMA): schema,
            pythonic(ATTR_DEFINITIONS): attr_definitions
        }

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


class GlobalSecondaryIndex(Index):
    """
    A global secondary index
    """
    index_type = GLOBAL_SECONDARY_INDEX


class LocalSecondaryIndex(Index):
    """
    A local secondary index
    """
    index_type = LOCAL_SECONDARY_INDEX


class Projection(object):
    """
    A class for presenting projections
    """
    projection_type = None
    non_key_attributes = None


class KeysOnlyProjection(Projection):
    """
    Keys only projection
    """
    projection_type = KEYS_ONLY


class IncludeProjection(Projection):
    """
    An INCLUDE projection
    """
    projection_type = INCLUDE

    def __init__(self, non_attr_keys=None):
        if not non_attr_keys:
            raise ValueError("The INCLUDE type projection requires a list of string attribute names")
        self.non_key_attributes = non_attr_keys


class AllProjection(Projection):
    """
    An ALL projection
    """
    projection_type = ALL