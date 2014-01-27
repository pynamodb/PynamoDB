"""
pynamodb.models
~~~~~~~~~~~~~~~~

DynamoDB Models for PynamoDB
"""

import time
import six
import copy
from six import with_metaclass
from .throttle import NoThrottle
from .attributes import Attribute
from .connection.base import MetaTable
from .connection.table import TableConnection
from .connection.util import pythonic
from .types import HASH, RANGE
from pynamodb.indexes import Index
from pynamodb.constants import (
    ATTR_TYPE_MAP, ATTR_DEFINITIONS, ATTR_NAME, ATTR_TYPE, KEY_SCHEMA,
    KEY_TYPE, ITEM, ITEMS, READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS,
    RANGE_KEY, ATTRIBUTES, PUT, DELETE, RESPONSES, GLOBAL_SECONDARY_INDEX,
    INDEX_NAME, PROVISIONED_THROUGHPUT, PROJECTION, ATTR_UPDATES, ALL_NEW,
    GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, ACTION, VALUE, KEYS,
    PROJECTION_TYPE, NON_KEY_ATTRIBUTES, COMPARISON_OPERATOR, ATTR_VALUE_LIST,
    TABLE_STATUS, ACTIVE, RETURN_VALUES, BATCH_GET_PAGE_LIMIT, UNPROCESSED_KEYS,
    PUT_REQUEST, DELETE_REQUEST, LAST_EVALUATED_KEY, QUERY_OPERATOR_MAP,
    SCAN_OPERATOR_MAP, CONSUMED_CAPACITY, BATCH_WRITE_PAGE_LIMIT, TABLE_NAME,
    CAPACITY_UNITS)


class ModelContextManager(object):
    """
    A class for managing batch operations
    """

    def __init__(self, model, auto_commit=True):
        self.model = model
        self.auto_commit = auto_commit
        self.max_operations = BATCH_WRITE_PAGE_LIMIT
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
        if not len(put_items) and not len(delete_items):
            return
        self.model.throttle.throttle()
        data = self.model.get_connection().batch_write_item(
            put_items=put_items,
            delete_items=delete_items
        )
        self.model.add_throttle_record(data.get(CONSUMED_CAPACITY, None))
        if not data:
            return
        unprocessed_keys = data.get(UNPROCESSED_KEYS, {}).get(self.model.table_name)
        while unprocessed_keys:
            put_items = []
            delete_items = []
            for key in unprocessed_keys:
                if PUT_REQUEST in key:
                    put_items.append(key.get(PUT_REQUEST))
                elif DELETE_REQUEST in key:
                    delete_items.append(key.get(DELETE_REQUEST))
            self.model.throttle.throttle()
            data = self.model.get_connection().batch_write_item(
                put_items=put_items,
                delete_items=delete_items
            )
            self.model.add_throttle_record(data.get(CONSUMED_CAPACITY))
            unprocessed_keys = data.get(UNPROCESSED_KEYS, {}).get(self.model.table_name)


class MetaModel(type):
    """
    Model meta class

    This class is just here so that index queries have nice syntax.
    Model.index.query()
    """
    def __init__(cls, name, bases, attrs):
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if issubclass(attr_obj.__class__, (Index, )):
                    attr_obj.__class__.model = cls
                    attr_obj.__class__.index_name = attr_name
                elif issubclass(attr_obj.__class__, (Attribute, )):
                    attr_obj.attr_name = attr_name


class Model(with_metaclass(MetaModel)):
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
    index_classes = None
    throttle = NoThrottle()

    def __init__(self, hash_key=None, range_key=None, **attrs):
        """
        :param hash_key: Required. The hash key for this object.
        :param range_key: Only required if the table has a range key attribute.
        :param attrs: A dictionary of attributes to set on this object.
        """
        self.attribute_values = {}
        self.set_defaults()
        if hash_key:
            setattr(self, self.meta().hash_keyname, hash_key)
        if range_key:
            setattr(self, self.meta().range_keyname, range_key)
        self.set_attributes(**attrs)

    @classmethod
    def add_throttle_record(cls, records):
        """
        Pulls out the table name and capacity units from `records` and
        puts it in `self.throttle`
        """
        if records:
            for record in records:
                if record.get(TABLE_NAME) == cls.table_name:
                    cls.throttle.add_record(record.get(CAPACITY_UNITS))
                    break

    @classmethod
    def batch_get(cls, items):
        """
        BatchGetItem for this model
        """
        hash_keyname = cls.meta().hash_keyname
        range_keyname = cls.meta().range_keyname
        keys_to_get = []
        while items:
            if len(keys_to_get) == BATCH_GET_PAGE_LIMIT:
                while keys_to_get:
                    page, unprocessed_keys = cls._batch_get_page(keys_to_get)
                    for batch_item in page:
                        yield cls.from_raw_data(batch_item)
                    if unprocessed_keys:
                        keys_to_get = unprocessed_keys
                    else:
                        keys_to_get = []
            item = items.pop()
            if range_keyname:
                hash_key, range_key = cls.serialize_keys(item[0], item[1])
                keys_to_get.append({
                    hash_keyname: hash_key,
                    range_keyname: range_key
                })
            else:
                hash_key = cls.serialize_keys(item[0], None)[0]
                keys_to_get.append({
                    hash_keyname: hash_key
                })

        while keys_to_get:
            page, unprocessed_keys = cls._batch_get_page(keys_to_get)
            for batch_item in page:
                yield cls.from_raw_data(batch_item)
            if unprocessed_keys:
                keys_to_get = unprocessed_keys
            else:
                keys_to_get = []

    @classmethod
    def _batch_get_page(cls, keys_to_get):
        """
        Returns a single page from BatchGetItem
        Also returns any unprocessed items
        """
        data = cls.get_connection().batch_get_item(
            keys_to_get
        )
        cls.throttle.add_record(data.get(CONSUMED_CAPACITY))
        item_data = data.get(RESPONSES).get(cls.table_name)
        unprocessed_items = data.get(UNPROCESSED_KEYS).get(cls.table_name, {}).get(KEYS, None)
        return item_data, unprocessed_items

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
            if value is not None:
                setattr(self, name, value)

    def set_attributes(self, **attrs):
        """
        Sets the attributes for this object
        """

        for key, value in attrs.items():
            setattr(self, key, value)

    def __repr__(self):
        hash_key = getattr(self, self.meta().hash_keyname, None)
        if hash_key and self.table_name:
            if self.meta().range_keyname:
                range_key = getattr(self, self.meta().range_keyname, None)
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

    def update_item(self, attribute, value, action=None):
        args, kwargs = self._get_save_args()
        for attr_name, attr_cls in self.get_attributes().items():
            if attr_name == attribute:
                value = attr_cls.serialize(value)
                break
        del(kwargs[pythonic(ATTRIBUTES)])
        kwargs[pythonic(ATTR_UPDATES)] = {
            attribute: {
                ACTION: action.upper(),
                VALUE: value
            }
        }
        kwargs[pythonic(RETURN_VALUES)] = ALL_NEW
        data = self.get_connection().update_item(
            *args,
            **kwargs
        )
        self.throttle.add_record(data.get(CONSUMED_CAPACITY))
        for name, value in data.get(ATTRIBUTES).items():
            attr = self.get_attributes().get(name, None)
            if attr:
                setattr(self, name, attr.deserialize(value.get(ATTR_TYPE_MAP[attr.attr_type])))
        return data

    def save(self):
        """
        Save this object to dynamodb
        """
        args, kwargs = self._get_save_args()
        data = self.get_connection().put_item(*args, **kwargs)
        self.throttle.add_record(data.get(CONSUMED_CAPACITY))
        return data

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

    def refresh(self, consistent_read=False):
        """
        Retrieves this object's data from dynamodb and syncs this local object
        """
        args, kwargs = self._get_save_args(attributes=False)
        kwargs.setdefault('consistent_read', consistent_read)
        attrs = self.get_connection().get_item(*args, **kwargs)
        self.throttle.add_record(attrs.get(CONSUMED_CAPACITY))
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
                    setattr(self, name, attr_instance.deserialize(value))

    def serialize(self, attr_map=False):
        """
        Serializes a value for use with DynamoDB
        """
        attributes = pythonic(ATTRIBUTES)
        attrs = {attributes: {}}
        for name, attr in self.get_attributes().items():
            value = getattr(self, name)
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
    def serialize_keys(cls, hash_key, range_key=None):
        """
        Serializes the hash and range keys
        """
        hash_key = cls.hash_key_attribute().serialize(hash_key)
        if range_key:
            range_key = cls.range_key_attribute().serialize(range_key)
        return hash_key, range_key

    @classmethod
    def range_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        attributes = cls.get_attributes()
        range_keyname = cls.meta().range_keyname
        if range_keyname:
            return attributes[range_keyname]
        else:
            return None

    @classmethod
    def hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        attributes = cls.get_attributes()
        hash_keyname = cls.meta().hash_keyname
        return attributes[hash_keyname]

    @classmethod
    def get(cls,
            hash_key,
            range_key=None,
            consistent_read=False):
        """
        Returns a single object using the provided keys
        """
        hash_key, range_key = cls.serialize_keys(hash_key, range_key)
        data = cls.get_connection().get_item(
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read
        )
        cls.throttle.add_record(data.get(CONSUMED_CAPACITY))
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
            cls.index_classes = {}
            for item in dir(cls):
                item_cls = getattr(cls, item).__class__
                if issubclass(item_cls, (Index, )):
                    item_cls = getattr(cls, item)
                    cls.index_classes[item] = item_cls
                    schema = item_cls.schema()
                    idx = {
                        pythonic(INDEX_NAME): item,
                        pythonic(KEY_SCHEMA): schema.get(pythonic(KEY_SCHEMA)),
                        pythonic(PROJECTION): {
                            PROJECTION_TYPE: item_cls.projection.projection_type,
                        },

                    }
                    if item_cls.index_type == GLOBAL_SECONDARY_INDEX:
                        idx[pythonic(PROVISIONED_THROUGHPUT)] = {
                            READ_CAPACITY_UNITS: item_cls.read_capacity_units,
                            WRITE_CAPACITY_UNITS: item_cls.write_capacity_units
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
    def _build_filters(cls, operator_map, filters):
        """
        Builds an appropriate condition map
        """
        key_conditions = {}
        attribute_classes = cls.get_attributes()
        for query, value in filters.items():
            attribute = None
            for token in query.split('__'):
                if attribute is None:
                    attribute = token
                    attribute_class = attribute_classes.get(attribute)
                    if not isinstance(value, list):
                        value = [value]
                    value = [attribute_class.serialize(val) for val in value]
                elif token in operator_map:
                    key_conditions[attribute] = {
                        COMPARISON_OPERATOR: operator_map.get(token),
                        ATTR_VALUE_LIST: value
                    }
                else:
                    raise ValueError("Could not parse filter: {0}".format(query))
        return key_conditions

    @classmethod
    def query(cls,
              hash_key,
              consistent_read=False,
              index_name=None,
              scan_index_forward=None,
              **filters):
        """
        Provides a high level query API
        """
        key_conditions = {}
        cls.get_indexes()
        if index_name:
            hash_key = cls.index_classes[index_name].hash_key_attribute().serialize(hash_key)
        else:
            hash_key = cls.serialize_keys(hash_key, None)[0]
        key_conditions = cls._build_filters(QUERY_OPERATOR_MAP, filters)
        data = cls.get_connection().query(
            hash_key,
            index_name=index_name,
            consistent_read=consistent_read,
            scan_index_forward=scan_index_forward,
            key_conditions=key_conditions
        )
        cls.throttle.add_record(data.get(CONSUMED_CAPACITY))
        last_evaluated_key = data.get(LAST_EVALUATED_KEY, None)
        for item in data.get(ITEMS):
            yield cls.from_raw_data(item)
        while last_evaluated_key:
            data = cls.get_connection().query(
                hash_key,
                exclusive_start_key=last_evaluated_key,
                index_name=index_name,
                consistent_read=consistent_read,
                scan_index_forward=scan_index_forward,
                key_conditions=key_conditions
            )
            cls.throttle.add_record(data.get(CONSUMED_CAPACITY))
            for item in data.get(ITEMS):
                yield cls.from_raw_data(item)
            last_evaluated_key = data.get(LAST_EVALUATED_KEY, None)

    @classmethod
    def scan(cls,
             segment=None,
             total_segments=None,
             limit=None,
             **filters):
        """
        Iterates through all items in the table
        """
        scan_filter = cls._build_filters(SCAN_OPERATOR_MAP, filters)
        data = cls.get_connection().scan(
            segment=segment,
            limit=limit,
            scan_filter=scan_filter,
            total_segments=total_segments
        )
        last_evaluated_key = data.get(LAST_EVALUATED_KEY, None)
        cls.throttle.add_record(data.get(CONSUMED_CAPACITY))
        for item in data.get(ITEMS):
            yield cls.from_raw_data(item)
        while last_evaluated_key:
            data = cls.get_connection().scan(
                exclusive_start_key=last_evaluated_key,
                limit=limit,
                scan_filter=scan_filter,
                segment=segment,
                total_segments=total_segments
            )
            for item in data.get(ITEMS):
                yield cls.from_raw_data(item)
            last_evaluated_key = data.get(LAST_EVALUATED_KEY, None)

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
            index_attrs = index_data.get(pythonic(ATTR_DEFINITIONS))
            attr_keys = [attr.get(pythonic(ATTR_NAME)) for attr in schema.get(pythonic(ATTR_DEFINITIONS))]
            for attr in index_attrs:
                attr_name = attr.get(pythonic(ATTR_NAME))
                if not attr_name in attr_keys:
                    schema[pythonic(ATTR_DEFINITIONS)].append(attr)
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
