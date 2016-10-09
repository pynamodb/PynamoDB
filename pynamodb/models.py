"""
DynamoDB Models for PynamoDB
"""
import json
import time
import six
import copy
import logging
import collections
from six import with_metaclass
from pynamodb.exceptions import DoesNotExist, TableDoesNotExist, TableError
from pynamodb.throttle import NoThrottle
from pynamodb.attributes import Attribute
from pynamodb.connection.base import MetaTable
from pynamodb.connection.table import TableConnection
from pynamodb.connection.util import pythonic
from pynamodb.types import HASH, RANGE
from pynamodb.compat import NullHandler
from pynamodb.indexes import Index, GlobalSecondaryIndex
from pynamodb.settings import get_settings_value
from pynamodb.constants import (
    ATTR_TYPE_MAP, ATTR_DEFINITIONS, ATTR_NAME, ATTR_TYPE, KEY_SCHEMA,
    KEY_TYPE, ITEM, ITEMS, READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS, CAMEL_COUNT,
    RANGE_KEY, ATTRIBUTES, PUT, DELETE, RESPONSES, QUERY_FILTER_OPERATOR_MAP,
    INDEX_NAME, PROVISIONED_THROUGHPUT, PROJECTION, ATTR_UPDATES, ALL_NEW,
    GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, ACTION, VALUE, KEYS,
    PROJECTION_TYPE, NON_KEY_ATTRIBUTES, COMPARISON_OPERATOR, ATTR_VALUE_LIST,
    TABLE_STATUS, ACTIVE, RETURN_VALUES, BATCH_GET_PAGE_LIMIT, UNPROCESSED_KEYS,
    PUT_REQUEST, DELETE_REQUEST, LAST_EVALUATED_KEY, QUERY_OPERATOR_MAP, NOT_NULL,
    SCAN_OPERATOR_MAP, CONSUMED_CAPACITY, BATCH_WRITE_PAGE_LIMIT, TABLE_NAME,
    CAPACITY_UNITS, META_CLASS_NAME, REGION, HOST, EXISTS, NULL,
    DELETE_FILTER_OPERATOR_MAP, UPDATE_FILTER_OPERATOR_MAP, PUT_FILTER_OPERATOR_MAP,
    COUNT, ITEM_COUNT, KEY, UNPROCESSED_ITEMS, STREAM_VIEW_TYPE, STREAM_SPECIFICATION,
    STREAM_ENABLED, EQ, NE)


log = logging.getLogger(__name__)
log.addHandler(NullHandler())


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


class BatchWrite(ModelContextManager):
    """
    A class for batch writes
    """
    def save(self, put_item):
        """
        This adds `put_item` to the list of pending writes to be performed.
        Additionally, the a BatchWriteItem will be performed if the length of items
        reaches 25.

        :param put_item: Should be an instance of a `Model` to be written
        """
        if len(self.pending_operations) == self.max_operations:
            if not self.auto_commit:
                raise ValueError("DynamoDB allows a maximum of 25 batch operations")
            else:
                self.commit()
        self.pending_operations.append({"action": PUT, "item": put_item})

    def delete(self, del_item):
        """
        This adds `del_item` to the list of pending deletes to be performed.
        If the list of items reaches 25, a BatchWriteItem will be called.

        :param del_item: Should be an instance of a `Model` to be deleted
        """
        if len(self.pending_operations) == self.max_operations:
            if not self.auto_commit:
                raise ValueError("DynamoDB allows a maximum of 25 batch operations")
            else:
                self.commit()
        self.pending_operations.append({"action": DELETE, "item": del_item})

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This ensures that all pending operations are committed when
        the context is exited
        """
        return self.commit()

    def commit(self):
        """
        Writes all of the changes that are pending
        """
        log.debug("%s committing batch operation", self.model)
        put_items = []
        delete_items = []
        attrs_name = pythonic(ATTRIBUTES)
        for item in self.pending_operations:
            if item['action'] == PUT:
                put_items.append(item['item']._serialize(attr_map=True)[attrs_name])
            elif item['action'] == DELETE:
                delete_items.append(item['item']._get_keys())
        self.pending_operations = []
        if not len(put_items) and not len(delete_items):
            return
        self.model.get_throttle().throttle()
        data = self.model._get_connection().batch_write_item(
            put_items=put_items,
            delete_items=delete_items
        )
        self.model.add_throttle_record(data.get(CONSUMED_CAPACITY, None))
        if data is None:
            return
        unprocessed_items = data.get(UNPROCESSED_ITEMS, {}).get(self.model.Meta.table_name)
        while unprocessed_items:
            put_items = []
            delete_items = []
            for item in unprocessed_items:
                if PUT_REQUEST in item:
                    put_items.append(item.get(PUT_REQUEST).get(ITEM))
                elif DELETE_REQUEST in item:
                    delete_items.append(item.get(DELETE_REQUEST).get(KEY))
            self.model.get_throttle().throttle()
            log.debug("Resending %s unprocessed keys for batch operation", len(unprocessed_items))
            data = self.model._get_connection().batch_write_item(
                put_items=put_items,
                delete_items=delete_items
            )
            self.model.add_throttle_record(data.get(CONSUMED_CAPACITY))
            unprocessed_items = data.get(UNPROCESSED_ITEMS, {}).get(self.model.Meta.table_name)


class DefaultMeta(object):
    pass


class ResultSet(object):

    def __init__(self, results, operation, arguments):
        self.results = results
        self.operation = operation
        self.arguments = arguments

    def __iter__(self):
        return iter(self.results)


class MetaModel(type):
    """
    Model meta class

    This class is just here so that index queries have nice syntax.
    Model.index.query()
    """
    def __init__(cls, name, bases, attrs):
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == META_CLASS_NAME:
                    if not hasattr(attr_obj, REGION):
                        setattr(attr_obj, REGION, get_settings_value('region'))
                    if not hasattr(attr_obj, HOST):
                        setattr(attr_obj, HOST, get_settings_value('host'))
                    if not hasattr(attr_obj, 'session_cls'):
                        setattr(attr_obj, 'session_cls', get_settings_value('session_cls'))
                    if not hasattr(attr_obj, 'request_timeout_seconds'):
                        setattr(attr_obj, 'request_timeout_seconds', get_settings_value('request_timeout_seconds'))
                    if not hasattr(attr_obj, 'base_backoff_ms'):
                        setattr(attr_obj, 'base_backoff_ms', get_settings_value('base_backoff_ms'))
                    if not hasattr(attr_obj, 'max_retry_attempts'):
                        setattr(attr_obj, 'max_retry_attempts', get_settings_value('max_retry_attempts'))
                elif issubclass(attr_obj.__class__, (Index, )):
                    attr_obj.Meta.model = cls
                    if not hasattr(attr_obj.Meta, "index_name"):
                        attr_obj.Meta.index_name = attr_name
                elif issubclass(attr_obj.__class__, (Attribute, )):
                    if attr_obj.attr_name is None:
                        attr_obj.attr_name = attr_name

            if META_CLASS_NAME not in attrs:
                setattr(cls, META_CLASS_NAME, DefaultMeta)


class AttributeDict(collections.MutableMapping):
    """
    A dictionary that stores attributes by two keys
    """
    def __init__(self, *args, **kwargs):
        self._values = {}
        self._alt_values = {}
        self.update(dict(*args, **kwargs))

    def __getitem__(self, key):
        if key in self._alt_values:
            return self._alt_values[key]
        return self._values[key]

    def __setitem__(self, key, value):
        if value.attr_name is not None:
            self._values[value.attr_name] = value
        self._alt_values[key] = value

    def __delitem__(self, key):
        del self._values[key]

    def __iter__(self):
        return iter(self._alt_values)

    def __len__(self):
        return len(self._values)

    def aliased_attrs(self):
        return self._alt_values.items()


class Model(with_metaclass(MetaModel)):
    """
    Defines a `PynamoDB` Model

    This model is backed by a table in DynamoDB.
    You can create the table by with the ``create_table`` method.
    """

    # These attributes are named to avoid colliding with user defined
    # DynamoDB attributes
    _meta_table = None
    _attributes = None
    _indexes = None
    _connection = None
    _index_classes = None
    _throttle = NoThrottle()
    DoesNotExist = DoesNotExist

    def __init__(self, hash_key=None, range_key=None, **attrs):
        """
        :param hash_key: Required. The hash key for this object.
        :param range_key: Only required if the table has a range key attribute.
        :param attrs: A dictionary of attributes to set on this object.
        """
        self.attribute_values = {}
        self._set_defaults()
        if hash_key is not None:
            attrs[self._get_meta_data().hash_keyname] = hash_key
        if range_key is not None:
            range_keyname = self._get_meta_data().range_keyname
            if range_keyname is None:
                raise ValueError(
                    "This table has no range key, but a range key value was provided: {0}".format(range_key)
                )
            attrs[range_keyname] = range_key
        self._set_attributes(**attrs)

    @classmethod
    def batch_get(cls, items, consistent_read=None, attributes_to_get=None):
        """
        BatchGetItem for this model

        :param items: Should be a list of hash keys to retrieve, or a list of
            tuples if range keys are used.
        """
        items = list(items)
        hash_keyname = cls._get_meta_data().hash_keyname
        range_keyname = cls._get_meta_data().range_keyname
        keys_to_get = []
        while items:
            if len(keys_to_get) == BATCH_GET_PAGE_LIMIT:
                while keys_to_get:
                    page, unprocessed_keys = cls._batch_get_page(keys_to_get, consistent_read=None,
                                                                 attributes_to_get=None)
                    for batch_item in page:
                        yield cls.from_raw_data(batch_item)
                    if unprocessed_keys:
                        keys_to_get = unprocessed_keys
                    else:
                        keys_to_get = []
            item = items.pop()
            if range_keyname:
                hash_key, range_key = cls._serialize_keys(item[0], item[1])
                keys_to_get.append({
                    hash_keyname: hash_key,
                    range_keyname: range_key
                })
            else:
                hash_key = cls._serialize_keys(item)[0]
                keys_to_get.append({
                    hash_keyname: hash_key
                })

        while keys_to_get:
            page, unprocessed_keys = cls._batch_get_page(keys_to_get, consistent_read=None, attributes_to_get=None)
            for batch_item in page:
                yield cls.from_raw_data(batch_item)
            if unprocessed_keys:
                keys_to_get = unprocessed_keys
            else:
                keys_to_get = []

    @classmethod
    def batch_write(cls, auto_commit=True):
        """
        Returns a context manager for a batch operation'

        :param auto_commit: Commits writes automatically if `True`
        """
        return BatchWrite(cls, auto_commit=auto_commit)

    def __repr__(self):
        if self.Meta.table_name:
            serialized = self._serialize(null_check=False)
            if self._get_meta_data().range_keyname:
                msg = "{0}<{1}, {2}>".format(self.Meta.table_name, serialized.get(HASH), serialized.get(RANGE))
            else:
                msg = "{0}<{1}>".format(self.Meta.table_name, serialized.get(HASH))
            return six.u(msg)

    def delete(self, conditional_operator=None, **expected_values):
        """
        Deletes this object from dynamodb
        """
        args, kwargs = self._get_save_args(attributes=False, null_check=False)
        if len(expected_values):
            kwargs.update(expected=self._build_expected_values(expected_values, DELETE_FILTER_OPERATOR_MAP))
        kwargs.update(conditional_operator=conditional_operator)
        return self._get_connection().delete_item(*args, **kwargs)

    def update_item(self, attribute, value=None, action=None, conditional_operator=None, **expected_values):
        """
        Updates an item using the UpdateItem operation.

        This should be used for updating a single attribute of an item.

        :param attribute: The name of the attribute to be updated
        :param value: The new value for the attribute.
        :param action: The action to take if this item already exists.
            See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-AttributeUpdate
        """
        args, save_kwargs = self._get_save_args(null_check=False)
        attribute_cls = None
        for attr_name, attr_cls in self._get_attributes().items():
            if attr_name == attribute:
                value = attr_cls.serialize(value)
                attribute_cls = attr_cls
                break
        if save_kwargs.get(pythonic(RANGE_KEY)):
            kwargs = {pythonic(RANGE_KEY): save_kwargs.get(pythonic(RANGE_KEY))}
        else:
            kwargs = {}
        if len(expected_values):
            kwargs.update(expected=self._build_expected_values(expected_values, UPDATE_FILTER_OPERATOR_MAP))
        kwargs[pythonic(ATTR_UPDATES)] = {
            attribute_cls.attr_name: {
                ACTION: action.upper() if action else None,
            }
        }
        if action is not None and action.upper() != DELETE:
            kwargs[pythonic(ATTR_UPDATES)][attribute_cls.attr_name][VALUE] = {ATTR_TYPE_MAP[attribute_cls.attr_type]: value}
        kwargs[pythonic(RETURN_VALUES)] = ALL_NEW
        kwargs.update(conditional_operator=conditional_operator)
        data = self._get_connection().update_item(
            *args,
            **kwargs
        )
        self._throttle.add_record(data.get(CONSUMED_CAPACITY))
        for name, value in data.get(ATTRIBUTES).items():
            attr = self._get_attributes().get(name, None)
            if attr:
                setattr(self, name, attr.deserialize(value.get(ATTR_TYPE_MAP[attr.attr_type])))
        return data

    def save(self, conditional_operator=None, **expected_values):
        """
        Save this object to dynamodb
        """
        args, kwargs = self._get_save_args()
        if len(expected_values):
            kwargs.update(expected=self._build_expected_values(expected_values, PUT_FILTER_OPERATOR_MAP))
        kwargs.update(conditional_operator=conditional_operator)
        data = self._get_connection().put_item(*args, **kwargs)
        if isinstance(data, dict):
            self._throttle.add_record(data.get(CONSUMED_CAPACITY))
        return data

    def refresh(self, consistent_read=False):
        """
        Retrieves this object's data from dynamodb and syncs this local object

        :param consistent_read: If True, then a consistent read is performed.
        """
        args, kwargs = self._get_save_args(attributes=False)
        kwargs.setdefault('consistent_read', consistent_read)
        attrs = self._get_connection().get_item(*args, **kwargs)
        self._throttle.add_record(attrs.get(CONSUMED_CAPACITY))
        item_data = attrs.get(ITEM, None)
        if item_data is None:
            raise self.DoesNotExist("This item does not exist in the table.")
        self._deserialize(item_data)

    @classmethod
    def get(cls,
            hash_key,
            range_key=None,
            consistent_read=False):
        """
        Returns a single object using the provided keys

        :param hash_key: The hash key of the desired item
        :param range_key: The range key of the desired item, only used when appropriate.
        """
        hash_key, range_key = cls._serialize_keys(hash_key, range_key)
        data = cls._get_connection().get_item(
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read
        )
        if data:
            item_data = data.get(ITEM)
            if item_data:
                cls._throttle.add_record(data.get(CONSUMED_CAPACITY))
                return cls.from_raw_data(item_data)
        raise cls.DoesNotExist()

    @classmethod
    def from_raw_data(cls, data):
        """
        Returns an instance of this class
        from the raw data

        :param data: A serialized DynamoDB object
        """
        mutable_data = copy.copy(data)
        if mutable_data is None:
            raise ValueError("Received no mutable_data to construct object")
        hash_keyname = cls._get_meta_data().hash_keyname
        range_keyname = cls._get_meta_data().range_keyname
        hash_key_type = cls._get_meta_data().get_attribute_type(hash_keyname)
        hash_key = mutable_data.pop(hash_keyname).get(hash_key_type)
        hash_key_attr = cls._get_attributes().get(hash_keyname)
        hash_key = hash_key_attr.deserialize(hash_key)
        args = (hash_key,)
        kwargs = {}
        if range_keyname:
            range_key_attr = cls._get_attributes().get(range_keyname)
            range_key_type = cls._get_meta_data().get_attribute_type(range_keyname)
            range_key = mutable_data.pop(range_keyname).get(range_key_type)
            kwargs['range_key'] = range_key_attr.deserialize(range_key)
        for name, value in mutable_data.items():
            attr = cls._get_attributes().get(name, None)
            if attr:
                kwargs[name] = attr.deserialize(attr.get_value(value))
        return cls(*args, **kwargs)

    @classmethod
    def count(cls,
              hash_key=None,
              consistent_read=False,
              index_name=None,
              limit=None,
              **filters):
        """
        Provides a filtered count

        :param hash_key: The hash key to query. Can be None.
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param filters: A dictionary of filters to be used in the query
        """
        if hash_key is None:
            return cls.describe_table().get(ITEM_COUNT)

        cls._get_indexes()
        if index_name:
            hash_key = cls._index_classes[index_name]._hash_key_attribute().serialize(hash_key)
            key_attribute_classes = cls._index_classes[index_name]._get_attributes()
            non_key_attribute_classes = cls._get_attributes()
        else:
            hash_key = cls._serialize_keys(hash_key)[0]
            non_key_attribute_classes = AttributeDict()
            key_attribute_classes = AttributeDict()
            for name, attr in cls._get_attributes().items():
                if attr.is_range_key or attr.is_hash_key:
                    key_attribute_classes[name] = attr
                else:
                    non_key_attribute_classes[name] = attr
        key_conditions, query_filters = cls._build_filters(
            QUERY_OPERATOR_MAP,
            non_key_operator_map=QUERY_FILTER_OPERATOR_MAP,
            key_attribute_classes=key_attribute_classes,
            non_key_attribute_classes=non_key_attribute_classes,
            filters=filters)

        data = cls._get_connection().query(
            hash_key,
            index_name=index_name,
            consistent_read=consistent_read,
            key_conditions=key_conditions,
            query_filters=query_filters,
            limit=limit,
            select=COUNT
        )
        return data.get(CAMEL_COUNT)

    @classmethod
    def query(cls,
              hash_key,
              consistent_read=False,
              index_name=None,
              scan_index_forward=None,
              conditional_operator=None,
              limit=None,
              last_evaluated_key=None,
              attributes_to_get=None,
              page_size=None,
              **filters):
        """
        Provides a high level query API

        :param hash_key: The hash key to query
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param limit: Used to limit the number of results returned
        :param scan_index_forward: If set, then used to specify the same parameter to the DynamoDB API.
            Controls descending or ascending results
        :param last_evaluated_key: If set, provides the starting point for query.
        :param attributes_to_get: If set, only returns these elements
        :param page_size: Page size of the query to DynamoDB
        :param filters: A dictionary of filters to be used in the query
        """
        cls._get_indexes()
        if index_name:
            hash_key = cls._index_classes[index_name]._hash_key_attribute().serialize(hash_key)
            key_attribute_classes = cls._index_classes[index_name]._get_attributes()
            non_key_attribute_classes = cls._get_attributes()
        else:
            hash_key = cls._serialize_keys(hash_key)[0]
            non_key_attribute_classes = AttributeDict()
            key_attribute_classes = AttributeDict()
            for name, attr in cls._get_attributes().items():
                if attr.is_range_key or attr.is_hash_key:
                    key_attribute_classes[name] = attr
                else:
                    non_key_attribute_classes[name] = attr

        if page_size is None:
            page_size = limit

        key_conditions, query_filters = cls._build_filters(
            QUERY_OPERATOR_MAP,
            non_key_operator_map=QUERY_FILTER_OPERATOR_MAP,
            key_attribute_classes=key_attribute_classes,
            non_key_attribute_classes=non_key_attribute_classes,
            filters=filters)
        log.debug("Fetching first query page")

        query_kwargs = dict(
            index_name=index_name,
            exclusive_start_key=last_evaluated_key,
            consistent_read=consistent_read,
            scan_index_forward=scan_index_forward,
            limit=page_size,
            key_conditions=key_conditions,
            attributes_to_get=attributes_to_get,
            query_filters=query_filters,
            conditional_operator=conditional_operator
        )

        data = cls._get_connection().query(hash_key, **query_kwargs)
        cls._throttle.add_record(data.get(CONSUMED_CAPACITY))

        last_evaluated_key = data.get(LAST_EVALUATED_KEY, None)

        for item in data.get(ITEMS):
            if limit is not None:
                if limit == 0:
                    return
                limit -= 1
            yield cls.from_raw_data(item)

        while last_evaluated_key:
            query_kwargs['exclusive_start_key'] = last_evaluated_key
            log.debug("Fetching query page with exclusive start key: %s", last_evaluated_key)
            data = cls._get_connection().query(hash_key, **query_kwargs)
            cls._throttle.add_record(data.get(CONSUMED_CAPACITY))
            for item in data.get(ITEMS):
                if limit is not None:
                    if limit == 0:
                        return
                    limit -= 1
                yield cls.from_raw_data(item)
            last_evaluated_key = data.get(LAST_EVALUATED_KEY, None)

    @classmethod
    def scan(cls,
             segment=None,
             total_segments=None,
             limit=None,
             conditional_operator=None,
             last_evaluated_key=None,
             page_size=None,
             **filters):
        """
        Iterates through all items in the table

        :param segment: If set, then scans the segment
        :param total_segments: If set, then specifies total segments
        :param limit: Used to limit the number of results returned
        :param last_evaluated_key: If set, provides the starting point for scan.
        :param page_size: Page size of the scan to DynamoDB
        :param filters: A list of item filters
        """
        key_filter, scan_filter = cls._build_filters(
            SCAN_OPERATOR_MAP,
            non_key_operator_map=SCAN_OPERATOR_MAP,
            key_attribute_classes=cls._get_attributes(),
            filters=filters
        )
        key_filter.update(scan_filter)
        if page_size is None:
            page_size = limit

        data = cls._get_connection().scan(
            exclusive_start_key=last_evaluated_key,
            segment=segment,
            limit=page_size,
            scan_filter=key_filter,
            total_segments=total_segments,
            conditional_operator=conditional_operator
        )
        log.debug("Fetching first scan page")
        last_evaluated_key = data.get(LAST_EVALUATED_KEY, None)
        cls._throttle.add_record(data.get(CONSUMED_CAPACITY))
        for item in data.get(ITEMS):
            yield cls.from_raw_data(item)
            if limit is not None:
                limit -= 1
                if not limit:
                    return
        while last_evaluated_key:
            log.debug("Fetching scan page with exclusive start key: %s", last_evaluated_key)
            data = cls._get_connection().scan(
                exclusive_start_key=last_evaluated_key,
                limit=page_size,
                scan_filter=key_filter,
                segment=segment,
                total_segments=total_segments
            )
            for item in data.get(ITEMS):
                yield cls.from_raw_data(item)
                if limit is not None:
                    limit -= 1
                    if not limit:
                        return

            last_evaluated_key = data.get(LAST_EVALUATED_KEY, None)

    @classmethod
    def exists(cls):
        """
        Returns True if this table exists, False otherwise
        """
        try:
            cls._get_connection().describe_table()
            return True
        except TableDoesNotExist:
            return False

    @classmethod
    def delete_table(cls):
        """
        Delete the table for this model
        """
        return cls._get_connection().delete_table()

    @classmethod
    def describe_table(cls):
        """
        Returns the result of a DescribeTable operation on this model's table
        """
        return cls._get_connection().describe_table()

    @classmethod
    def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None):
        """
        Create the table for this model

        :param wait: If set, then this call will block until the table is ready for use
        :param read_capacity_units: Sets the read capacity units for this table
        :param write_capacity_units: Sets the write capacity units for this table
        """
        if not cls.exists():
            schema = cls._get_schema()
            if hasattr(cls.Meta, pythonic(READ_CAPACITY_UNITS)):
                schema[pythonic(READ_CAPACITY_UNITS)] = cls.Meta.read_capacity_units
            if hasattr(cls.Meta, pythonic(WRITE_CAPACITY_UNITS)):
                schema[pythonic(WRITE_CAPACITY_UNITS)] = cls.Meta.write_capacity_units
            if hasattr(cls.Meta, pythonic(STREAM_VIEW_TYPE)):
                schema[pythonic(STREAM_SPECIFICATION)] = {
                    pythonic(STREAM_ENABLED): True,
                    pythonic(STREAM_VIEW_TYPE): cls.Meta.stream_view_type
                }
            if read_capacity_units is not None:
                schema[pythonic(READ_CAPACITY_UNITS)] = read_capacity_units
            if write_capacity_units is not None:
                schema[pythonic(WRITE_CAPACITY_UNITS)] = write_capacity_units
            index_data = cls._get_indexes()
            schema[pythonic(GLOBAL_SECONDARY_INDEXES)] = index_data.get(pythonic(GLOBAL_SECONDARY_INDEXES))
            schema[pythonic(LOCAL_SECONDARY_INDEXES)] = index_data.get(pythonic(LOCAL_SECONDARY_INDEXES))
            index_attrs = index_data.get(pythonic(ATTR_DEFINITIONS))
            attr_keys = [attr.get(pythonic(ATTR_NAME)) for attr in schema.get(pythonic(ATTR_DEFINITIONS))]
            for attr in index_attrs:
                attr_name = attr.get(pythonic(ATTR_NAME))
                if attr_name not in attr_keys:
                    schema[pythonic(ATTR_DEFINITIONS)].append(attr)
                    attr_keys.append(attr_name)
            cls._get_connection().create_table(
                **schema
            )
        if wait:
            while True:
                status = cls._get_connection().describe_table()
                if status:
                    data = status.get(TABLE_STATUS)
                    if data == ACTIVE:
                        return
                    else:
                        time.sleep(2)
                else:
                    raise TableError("No TableStatus returned for table")

    @classmethod
    def dumps(cls):
        """
        Returns a JSON representation of this model's table
        """
        return json.dumps([item._get_json() for item in cls.scan()])

    @classmethod
    def dump(cls, filename):
        """
        Writes the contents of this model's table as JSON to the given filename
        """
        with open(filename, 'w') as out:
            out.write(cls.dumps())

    @classmethod
    def loads(cls, data):
        content = json.loads(data)
        with cls.batch_write() as batch:
            for item_data in content:
                item = cls._from_data(item_data)
                batch.save(item)

    @classmethod
    def load(cls, filename):
        with open(filename, 'r') as inf:
            cls.loads(inf.read())

    # Private API below
    @classmethod
    def _from_data(cls, data):
        """
        Reconstructs a model object from JSON.
        """
        hash_key, attrs = data
        range_key = attrs.pop('range_key', None)
        attributes = attrs.pop(pythonic(ATTRIBUTES))
        if range_key is not None:
            range_keyname = cls._get_meta_data().range_keyname
            range_keytype = cls._get_meta_data().get_attribute_type(range_keyname)
            attributes[range_keyname] = {
                range_keytype: range_key
            }
        item = cls(hash_key)
        item._deserialize(attributes)
        return item

    @classmethod
    def _build_expected_values(cls, expected_values, operator_map=None):
        """
        Builds an appropriate expected value map

        :param expected_values: A list of expected values
        """
        expected_values_result = {}
        attributes = cls._get_attributes()
        filters = {}
        for attr_name, attr_value in expected_values.items():
            attr_cond = VALUE
            if attr_name.endswith("__exists"):
                attr_cond = EXISTS
                attr_name = attr_name[:-8]
            attr_cls = attributes.get(attr_name, None)
            if attr_cls is None:
                filters[attr_name] = attr_value
            else:
                if attr_cond == VALUE:
                    attr_value = attr_cls.serialize(attr_value)
                expected_values_result[attr_cls.attr_name] = {
                    attr_cond: attr_value
                }
        for cond, value in filters.items():
            attribute = None
            attribute_class = None
            for token in cond.split('__'):
                if attribute is None:
                    attribute = token
                    attribute_class = attributes.get(attribute)
                    if attribute_class is None:
                        raise ValueError("Attribute {0} specified for expected value does not exist".format(attribute))
                elif token in operator_map:
                    operator = operator_map.get(token)
                    if operator == NULL:
                        if value:
                            value = NULL
                        else:
                            value = NOT_NULL
                        condition = {
                            COMPARISON_OPERATOR: value,
                        }
                    elif operator == EQ or operator == NE:
                        condition = {
                            COMPARISON_OPERATOR: operator,
                            ATTR_VALUE_LIST: [{
                                ATTR_TYPE_MAP[attribute_class.attr_type]:
                                attribute_class.serialize(value)
                            }]
                        }
                    else:
                        if not isinstance(value, list):
                            value = [value]
                        condition = {
                            COMPARISON_OPERATOR: operator,
                            ATTR_VALUE_LIST: [
                                {
                                    ATTR_TYPE_MAP[attribute_class.attr_type]:
                                    attribute_class.serialize(val)
                                } for val in value
                            ]
                        }
                    expected_values_result[attributes.get(attribute).attr_name] = condition
                else:
                    raise ValueError("Could not parse expected condition: {0}".format(cond))
        return expected_values_result

    @classmethod
    def _tokenize_filters(cls, filters):
        """
        Tokenizes filters in the attribute name, operator, and value
        """
        filters = filters or {}
        for query, value in filters.items():
            if '__' in query:
                attribute, operator = query.split('__')
                yield attribute, operator, value
            else:
                yield query, None, value

    @classmethod
    def _build_filters(cls,
                       key_operator_map,
                       non_key_operator_map=None,
                       key_attribute_classes=None,
                       non_key_attribute_classes=None,
                       filters=None):
        """
        Builds an appropriate condition map

        :param operator_map: The mapping of operators used for key attributes
        :param non_key_operator_map: The mapping of operators used for non key attributes
        :param filters: A list of item filters
        """
        key_conditions = {}
        query_conditions = {}
        non_key_operator_map = non_key_operator_map or {}
        key_attribute_classes = key_attribute_classes or {}
        non_key_attribute_classes = non_key_attribute_classes or {}
        for attr_name, operator, value in cls._tokenize_filters(filters):
            attribute_class = key_attribute_classes.get(attr_name, None)
            if attribute_class is None:
                attribute_class = non_key_attribute_classes.get(attr_name, None)
            if attribute_class is None:
                raise ValueError("Attribute {0} specified for filter does not exist.".format(attr_name))
            attribute_name = attribute_class.attr_name
            if operator not in key_operator_map and operator not in non_key_operator_map:
                raise ValueError(
                    "{0} is not a valid filter. Must be one of {1} {2}".format(
                        operator,
                        key_operator_map.keys(), non_key_operator_map.keys()
                    )
                )
            if attribute_name in key_conditions or attribute_name in query_conditions:
                # Before this validation logic, PynamoDB would stomp on multiple values and use only the last provided.
                # This leads to unexpected behavior. In some cases, the DynamoDB API does not allow multiple values
                # even when using the newer API (e.g. KeyConditions and KeyConditionExpression only allow a single
                # value for each member of the primary key). In other cases, moving PynamoDB to the newer API
                # (e.g. FilterExpression over ScanFilter) would allow support for multiple conditions.
                raise ValueError(
                    "Multiple values not supported for attributes in KeyConditions, QueryFilter, or ScanFilter, "
                    "multiple values provided for attribute {0}".format(attribute_name)
                )

            if key_operator_map.get(operator, '') == NULL or non_key_operator_map.get(operator, '') == NULL:
                if value:
                    operator = pythonic(NULL)
                else:
                    operator = pythonic(NOT_NULL)
                condition = {}
            else:
                if not isinstance(value, list):
                    value = [value]
                value = [
                    {ATTR_TYPE_MAP[attribute_class.attr_type]: attribute_class.serialize(val)} for val in value
                ]
                condition = {
                    ATTR_VALUE_LIST: value
                }
            if operator in key_operator_map and (attribute_class.is_hash_key or attribute_class.is_range_key):
                condition.update({COMPARISON_OPERATOR: key_operator_map.get(operator)})
                key_conditions[attribute_name] = condition
            elif operator in non_key_operator_map and not (attribute_class.is_hash_key or attribute_class.is_range_key):
                condition.update({COMPARISON_OPERATOR: non_key_operator_map.get(operator)})
                query_conditions[attribute_name] = condition
            else:
                raise ValueError("Invalid filter specified: {0} {1} {2}".format(attribute_name, operator, value))
        return key_conditions, query_conditions

    @classmethod
    def _get_schema(cls):
        """
        Returns the schema for this table
        """
        schema = {
            pythonic(ATTR_DEFINITIONS): [],
            pythonic(KEY_SCHEMA): []
        }
        for attr_name, attr_cls in cls._get_attributes().items():
            if attr_cls.is_hash_key or attr_cls.is_range_key:
                schema[pythonic(ATTR_DEFINITIONS)].append({
                    pythonic(ATTR_NAME): attr_cls.attr_name,
                    pythonic(ATTR_TYPE): ATTR_TYPE_MAP[attr_cls.attr_type]
                })
            if attr_cls.is_hash_key:
                schema[pythonic(KEY_SCHEMA)].append({
                    pythonic(KEY_TYPE): HASH,
                    pythonic(ATTR_NAME): attr_cls.attr_name
                })
            elif attr_cls.is_range_key:
                schema[pythonic(KEY_SCHEMA)].append({
                    pythonic(KEY_TYPE): RANGE,
                    pythonic(ATTR_NAME): attr_cls.attr_name
                })
        return schema

    @classmethod
    def _get_indexes(cls):
        """
        Returns a list of the secondary indexes
        """
        if cls._indexes is None:
            cls._indexes = {
                pythonic(GLOBAL_SECONDARY_INDEXES): [],
                pythonic(LOCAL_SECONDARY_INDEXES): [],
                pythonic(ATTR_DEFINITIONS): []
            }
            cls._index_classes = {}
            for item in dir(cls):
                item_cls = getattr(getattr(cls, item), "__class__", None)
                if item_cls is None:
                    continue
                if issubclass(item_cls, (Index, )):
                    item_cls = getattr(cls, item)
                    cls._index_classes[item_cls.Meta.index_name] = item_cls
                    schema = item_cls._get_schema()
                    idx = {
                        pythonic(INDEX_NAME): item_cls.Meta.index_name,
                        pythonic(KEY_SCHEMA): schema.get(pythonic(KEY_SCHEMA)),
                        pythonic(PROJECTION): {
                            PROJECTION_TYPE: item_cls.Meta.projection.projection_type,
                        },

                    }
                    if issubclass(item_cls.__class__, GlobalSecondaryIndex):
                        idx[pythonic(PROVISIONED_THROUGHPUT)] = {
                            READ_CAPACITY_UNITS: item_cls.Meta.read_capacity_units,
                            WRITE_CAPACITY_UNITS: item_cls.Meta.write_capacity_units
                        }
                    cls._indexes[pythonic(ATTR_DEFINITIONS)].extend(schema.get(pythonic(ATTR_DEFINITIONS)))
                    if item_cls.Meta.projection.non_key_attributes:
                        idx[pythonic(PROJECTION)][NON_KEY_ATTRIBUTES] = item_cls.Meta.projection.non_key_attributes
                    if issubclass(item_cls.__class__, GlobalSecondaryIndex):
                        cls._indexes[pythonic(GLOBAL_SECONDARY_INDEXES)].append(idx)
                    else:
                        cls._indexes[pythonic(LOCAL_SECONDARY_INDEXES)].append(idx)
        return cls._indexes

    @classmethod
    def _get_attributes(cls):
        """
        Returns the list of attributes for this class
        """
        if cls._attributes is None:
            cls._attributes = AttributeDict()
            for item in dir(cls):
                try:
                    item_cls = getattr(getattr(cls, item), "__class__", None)
                except AttributeError:
                    continue
                if item_cls is None:
                    continue
                if issubclass(item_cls, (Attribute, )):
                    instance = getattr(cls, item)
                    cls._attributes[item] = instance
        return cls._attributes

    def _get_json(self):
        """
        Returns a Python object suitable for serialization
        """
        kwargs = {}
        serialized = self._serialize(null_check=False)
        hash_key = serialized.get(HASH)
        range_key = serialized.get(RANGE, None)
        if range_key is not None:
            kwargs[pythonic(RANGE_KEY)] = range_key
        kwargs[pythonic(ATTRIBUTES)] = serialized[pythonic(ATTRIBUTES)]
        return hash_key, kwargs

    def _get_save_args(self, attributes=True, null_check=True):
        """
        Gets the proper *args, **kwargs for saving and retrieving this object

        This is used for serializing items to be saved, or for serializing just the keys.

        :param attributes: If True, then attributes are included.
        :param null_check: If True, then attributes are checked for null.
        """
        kwargs = {}
        serialized = self._serialize(null_check=null_check)
        hash_key = serialized.get(HASH)
        range_key = serialized.get(RANGE, None)
        args = (hash_key, )
        if range_key is not None:
            kwargs[pythonic(RANGE_KEY)] = range_key
        if attributes:
            kwargs[pythonic(ATTRIBUTES)] = serialized[pythonic(ATTRIBUTES)]
        return args, kwargs

    @classmethod
    def _range_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        attributes = cls._get_attributes()
        range_keyname = cls._get_meta_data().range_keyname
        if range_keyname:
            attr = attributes[range_keyname]
        else:
            attr = None
        return attr

    @classmethod
    def _hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        attributes = cls._get_attributes()
        hash_keyname = cls._get_meta_data().hash_keyname
        return attributes[hash_keyname]

    def _get_keys(self):
        """
        Returns the proper arguments for deleting
        """
        serialized = self._serialize()
        hash_key = serialized.get(HASH)
        range_key = serialized.get(RANGE, None)
        hash_keyname = self._get_meta_data().hash_keyname
        range_keyname = self._get_meta_data().range_keyname
        attrs = {
            hash_keyname: hash_key,
        }
        if range_keyname is not None:
            attrs[range_keyname] = range_key
        return attrs

    @classmethod
    def _batch_get_page(cls, keys_to_get, consistent_read, attributes_to_get):
        """
        Returns a single page from BatchGetItem
        Also returns any unprocessed items

        :param keys_to_get: A list of keys
        :param consistent_read: Whether or not this needs to be consistent
        :param attributes_to_get: A list of attributes to return
        """
        log.debug("Fetching a BatchGetItem page")
        data = cls._get_connection().batch_get_item(
            keys_to_get, consistent_read, attributes_to_get
        )
        cls._throttle.add_record(data.get(CONSUMED_CAPACITY))
        item_data = data.get(RESPONSES).get(cls.Meta.table_name)
        unprocessed_items = data.get(UNPROCESSED_KEYS).get(cls.Meta.table_name, {}).get(KEYS, None)
        return item_data, unprocessed_items

    def _set_defaults(self):
        """
        Sets and fields that provide a default value
        """
        for name, attr in self._get_attributes().aliased_attrs():
            default = attr.default
            if callable(default):
                value = default()
            else:
                value = default
            if value is not None:
                setattr(self, name, value)

    def _set_attributes(self, **attrs):
        """
        Sets the attributes for this object
        """
        for attr_name, attr in self._get_attributes().aliased_attrs():
            if attr.attr_name in attrs:
                setattr(self, attr_name, attrs.get(attr.attr_name))
            elif attr_name in attrs:
                setattr(self, attr_name, attrs.get(attr_name))

    @classmethod
    def add_throttle_record(cls, records):
        """
        (Experimental)
        Pulls out the table name and capacity units from `records` and
        puts it in `self.throttle`

        :param records: A list of usage records
        """
        if records:
            for record in records:
                if record.get(TABLE_NAME) == cls.Meta.table_name:
                    cls._throttle.add_record(record.get(CAPACITY_UNITS))
                    break

    @classmethod
    def get_throttle(cls):
        """
        Returns the throttle implementation for this Model
        """
        return cls._throttle

    @classmethod
    def _get_meta_data(cls):
        """
        A helper object that contains meta data about this table
        """
        if cls._meta_table is None:
            cls._meta_table = MetaTable(cls._get_connection().describe_table())
        return cls._meta_table

    @classmethod
    def _get_connection(cls):
        """
        Returns a (cached) connection
        """
        if not hasattr(cls, "Meta") or cls.Meta.table_name is None:
            raise AttributeError(
                """As of v1.0 PynamoDB Models require a `Meta` class.
                See https://pynamodb.readthedocs.io/en/latest/release_notes.html"""
            )
        if cls._connection is None:
            cls._connection = TableConnection(cls.Meta.table_name,
                                              region=cls.Meta.region,
                                              host=cls.Meta.host,
                                              session_cls=cls.Meta.session_cls,
                                              request_timeout_seconds=cls.Meta.request_timeout_seconds,
                                              max_retry_attempts=cls.Meta.max_retry_attempts,
                                              base_backoff_ms=cls.Meta.base_backoff_ms)
        return cls._connection

    def _deserialize(self, attrs):
        """
        Sets attributes sent back from DynamoDB on this object

        :param attrs: A dictionary of attributes to update this item with.
        """
        for name, attr in attrs.items():
            attr_instance = self._get_attributes().get(name, None)
            if attr_instance:
                attr_type = ATTR_TYPE_MAP[attr_instance.attr_type]
                value = attr.get(attr_type, None)
                if value is not None:
                    setattr(self, name, attr_instance.deserialize(value))

    def _serialize(self, attr_map=False, null_check=True):
        """
        Serializes a value for use with DynamoDB

        :param attr_map: If True, then attributes are returned
        :param null_check: If True, then attributes are checked for null
        """
        attributes = pythonic(ATTRIBUTES)
        attrs = {attributes: {}}
        for name, attr in self._get_attributes().aliased_attrs():
            value = getattr(self, name)
            if value is None:
                if attr.null:
                    continue
                elif null_check:
                    raise ValueError("Attribute '{0}' cannot be None".format(attr.attr_name))
            serialized = attr.serialize(value)
            if serialized is None:
                continue
            if attr_map:
                attrs[attributes][attr.attr_name] = {
                    ATTR_TYPE_MAP[attr.attr_type]: serialized
                }
            else:
                if attr.is_hash_key:
                    attrs[HASH] = serialized
                elif attr.is_range_key:
                    attrs[RANGE] = serialized
                else:
                    attrs[attributes][attr.attr_name] = {
                        ATTR_TYPE_MAP[attr.attr_type]: serialized
                    }
        return attrs

    @classmethod
    def _serialize_keys(cls, hash_key, range_key=None):
        """
        Serializes the hash and range keys

        :param hash_key: The hash key value
        :param range_key: The range key value
        """
        hash_key = cls._hash_key_attribute().serialize(hash_key)
        if range_key is not None:
            range_key = cls._range_key_attribute().serialize(range_key)
        return hash_key, range_key
