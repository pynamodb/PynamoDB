"""
DynamoDB Models for PynamoDB
"""
import json
import time
import six
import logging
import warnings
from inspect import getmembers

from six import add_metaclass
from pynamodb.exceptions import DoesNotExist, TableDoesNotExist, TableError
from pynamodb.attributes import Attribute, AttributeContainer, AttributeContainerMeta, MapAttribute
from pynamodb.connection.table import TableConnection
from pynamodb.connection.util import pythonic
from pynamodb.types import HASH, RANGE
from pynamodb.indexes import Index, GlobalSecondaryIndex
from pynamodb.pagination import ResultIterator
from pynamodb.settings import get_settings_value
from pynamodb.constants import (
    ATTR_TYPE_MAP, ATTR_DEFINITIONS, ATTR_NAME, ATTR_TYPE, KEY_SCHEMA,
    KEY_TYPE, ITEM, READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS,
    RANGE_KEY, ATTRIBUTES, PUT, DELETE, RESPONSES,
    INDEX_NAME, PROVISIONED_THROUGHPUT, PROJECTION, ALL_NEW,
    GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, KEYS,
    PROJECTION_TYPE, NON_KEY_ATTRIBUTES,
    TABLE_STATUS, ACTIVE, RETURN_VALUES, BATCH_GET_PAGE_LIMIT,
    UNPROCESSED_KEYS, PUT_REQUEST, DELETE_REQUEST,
    BATCH_WRITE_PAGE_LIMIT,
    META_CLASS_NAME, REGION, HOST, NULL,
    COUNT, ITEM_COUNT, KEY, UNPROCESSED_ITEMS, STREAM_VIEW_TYPE,
    STREAM_SPECIFICATION, STREAM_ENABLED, BILLING_MODE)


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


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
        This adds `put_item` to the list of pending operations to be performed.

        If the list currently contains 25 items, which is the DynamoDB imposed
        limit on a BatchWriteItem call, one of two things will happen. If auto_commit
        is True, a BatchWriteItem operation will be sent with the already pending
        writes after which put_item is appended to the (now empty) list. If auto_commit
        is False, ValueError is raised to indicate additional items cannot be accepted
        due to the DynamoDB imposed limit.

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
        This adds `del_item` to the list of pending operations to be performed.

        If the list currently contains 25 items, which is the DynamoDB imposed
        limit on a BatchWriteItem call, one of two things will happen. If auto_commit
        is True, a BatchWriteItem operation will be sent with the already pending
        operations after which put_item is appended to the (now empty) list. If auto_commit
        is False, ValueError is raised to indicate additional items cannot be accepted
        due to the DynamoDB imposed limit.

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
        data = self.model._get_connection().batch_write_item(
            put_items=put_items,
            delete_items=delete_items
        )
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
            log.info("Resending %s unprocessed keys for batch operation", len(unprocessed_items))
            data = self.model._get_connection().batch_write_item(
                put_items=put_items,
                delete_items=delete_items
            )
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


class MetaModel(AttributeContainerMeta):
    """
    Model meta class

    This class is just here so that index queries have nice syntax.
    Model.index.query()
    """
    def __init__(cls, name, bases, attrs):
        super(MetaModel, cls).__init__(name, bases, attrs)
        for attr_name, attribute in cls.get_attributes().items():
            if attribute.is_hash_key:
                cls._hash_keyname = attr_name
            if attribute.is_range_key:
                cls._range_keyname = attr_name
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == META_CLASS_NAME:
                    if not hasattr(attr_obj, REGION):
                        setattr(attr_obj, REGION, get_settings_value('region'))
                    if not hasattr(attr_obj, HOST):
                        setattr(attr_obj, HOST, get_settings_value('host'))
                    if hasattr(attr_obj, 'session_cls') or hasattr(attr_obj, 'request_timeout_seconds'):
                        warnings.warn("The `session_cls` and `request_timeout_second` options are no longer supported")
                    if not hasattr(attr_obj, 'connect_timeout_seconds'):
                        setattr(attr_obj, 'connect_timeout_seconds', get_settings_value('connect_timeout_seconds'))
                    if not hasattr(attr_obj, 'read_timeout_seconds'):
                        setattr(attr_obj, 'read_timeout_seconds', get_settings_value('read_timeout_seconds'))
                    if not hasattr(attr_obj, 'base_backoff_ms'):
                        setattr(attr_obj, 'base_backoff_ms', get_settings_value('base_backoff_ms'))
                    if not hasattr(attr_obj, 'max_retry_attempts'):
                        setattr(attr_obj, 'max_retry_attempts', get_settings_value('max_retry_attempts'))
                    if not hasattr(attr_obj, 'max_pool_connections'):
                        setattr(attr_obj, 'max_pool_connections', get_settings_value('max_pool_connections'))
                    if not hasattr(attr_obj, 'extra_headers'):
                        setattr(attr_obj, 'extra_headers', get_settings_value('extra_headers'))
                    if not hasattr(attr_obj, 'aws_access_key_id'):
                        setattr(attr_obj, 'aws_access_key_id', None)
                    if not hasattr(attr_obj, 'aws_secret_access_key'):
                        setattr(attr_obj, 'aws_secret_access_key', None)
                elif isinstance(attr_obj, Index):
                    attr_obj.Meta.model = cls
                    if not hasattr(attr_obj.Meta, "index_name"):
                        attr_obj.Meta.index_name = attr_name
                elif isinstance(attr_obj, Attribute):
                    if attr_obj.attr_name is None:
                        attr_obj.attr_name = attr_name

            if META_CLASS_NAME not in attrs:
                setattr(cls, META_CLASS_NAME, DefaultMeta)

            # create a custom Model.DoesNotExist derived from pynamodb.exceptions.DoesNotExist,
            # so that "except Model.DoesNotExist:" would not catch other models' exceptions
            if 'DoesNotExist' not in attrs:
                exception_attrs = {'__module__': attrs.get('__module__')}
                if hasattr(cls, '__qualname__'):  # On Python 3, Model.DoesNotExist
                    exception_attrs['__qualname__'] = '{}.{}'.format(cls.__qualname__, 'DoesNotExist')
                cls.DoesNotExist = type('DoesNotExist', (DoesNotExist, ), exception_attrs)


@add_metaclass(MetaModel)
class Model(AttributeContainer):
    """
    Defines a `PynamoDB` Model

    This model is backed by a table in DynamoDB.
    You can create the table by with the ``create_table`` method.
    """

    # These attributes are named to avoid colliding with user defined
    # DynamoDB attributes
    _hash_keyname = None
    _range_keyname = None
    _indexes = None
    _connection = None
    _index_classes = None
    DoesNotExist = DoesNotExist

    def __init__(self, hash_key=None, range_key=None, **attributes):
        """
        :param hash_key: Required. The hash key for this object.
        :param range_key: Only required if the table has a range key attribute.
        :param attrs: A dictionary of attributes to set on this object.
        """
        if hash_key is not None:
            attributes[self._hash_keyname] = hash_key
        if range_key is not None:
            if self._range_keyname is None:
                raise ValueError(
                    "This table has no range key, but a range key value was provided: {}".format(range_key)
                )
            attributes[self._range_keyname] = range_key
        super(Model, self).__init__(**attributes)

    @property
    def _hash_key(self):
        return getattr(self, self._hash_keyname)

    @property
    def _range_key(self):
        return getattr(self, self._range_keyname) if self._range_keyname else None

    @classmethod
    def batch_get(cls, items, consistent_read=None, attributes_to_get=None):
        """
        BatchGetItem for this model

        :param items: Should be a list of hash keys to retrieve, or a list of
            tuples if range keys are used.
        """
        items = list(items)
        hash_key_attribute = cls._hash_key_attribute()
        range_key_attribute = cls._range_key_attribute()
        keys_to_get = []
        while items:
            if len(keys_to_get) == BATCH_GET_PAGE_LIMIT:
                while keys_to_get:
                    page, unprocessed_keys = cls._batch_get_page(
                        keys_to_get,
                        consistent_read=consistent_read,
                        attributes_to_get=attributes_to_get
                    )
                    for batch_item in page:
                        yield cls.from_raw_data(batch_item)
                    if unprocessed_keys:
                        keys_to_get = unprocessed_keys
                    else:
                        keys_to_get = []
            item = items.pop()
            if range_key_attribute:
                hash_key, range_key = cls._serialize_keys(item[0], item[1])
                keys_to_get.append({
                    hash_key_attribute.attr_name: hash_key,
                    range_key_attribute.attr_name: range_key
                })
            else:
                hash_key = cls._serialize_keys(item)[0]
                keys_to_get.append({
                    hash_key_attribute.attr_name: hash_key
                })

        while keys_to_get:
            page, unprocessed_keys = cls._batch_get_page(
                keys_to_get,
                consistent_read=consistent_read,
                attributes_to_get=attributes_to_get
            )
            for batch_item in page:
                yield cls.from_raw_data(batch_item)
            if unprocessed_keys:
                keys_to_get = unprocessed_keys
            else:
                keys_to_get = []

    @classmethod
    def batch_write(cls, auto_commit=True):
        """
        Returns a BatchWrite context manager for a batch operation.

        :param auto_commit: If true, the context manager will commit writes incrementally
                            as items are written to as necessary to honor item count limits
                            in the DynamoDB API (see BatchWrite). Regardless of the value
                            passed here, changes automatically commit on context exit
                            (whether successful or not).
        """
        return BatchWrite(cls, auto_commit=auto_commit)

    def __repr__(self):
        if self.Meta.table_name:
            if self._range_keyname:
                msg = "{}<{}, {}>".format(self.Meta.table_name, self._hash_key, self._range_key)
            else:
                msg = "{}<{}>".format(self.Meta.table_name, self._hash_key)
            return six.u(msg)

    def delete(self, condition=None):
        """
        Deletes this object from dynamodb
        """
        hash_key, range_key = self._serialize_keys(self._hash_key, self._range_key)
        args = (hash_key,)
        kwargs = dict(
            range_key=range_key,
            condition=condition,
        )
        return self._get_connection().delete_item(*args, **kwargs)

    def update(self, actions, condition=None):
        """
        Updates an item using the UpdateItem operation.

        :param actions: a list of Action updates to apply
        :param condition: an optional Condition on which to update
        """
        if not isinstance(actions, list) or len(actions) == 0:
            raise TypeError("the value of `actions` is expected to be a non-empty list")

        hash_key, range_key = self._serialize_keys(self._hash_key, self._range_key)
        args = (hash_key, )
        kwargs = dict(
            range_key=range_key,
            actions=actions,
            condition=condition,
            return_values=ALL_NEW,
        )
        data = self._get_connection().update_item(*args, **kwargs)
        for name, value in data[ATTRIBUTES].items():
            attr_name = self._dynamo_to_python_attr(name)
            attr = self.get_attributes().get(attr_name)
            if attr:
                setattr(self, attr_name, attr.deserialize(attr.get_value(value)))
        return data

    def save(self, condition=None):
        """
        Save this object to dynamodb
        """
        args, kwargs = self._get_save_args()
        kwargs.update(condition=condition)
        return self._get_connection().put_item(*args, **kwargs)

    def refresh(self, consistent_read=False):
        """
        Retrieves this object's data from dynamodb and syncs this local object

        :param consistent_read: If True, then a consistent read is performed.
        """
        hash_key, range_key = self._serialize_keys(self._hash_key, self._range_key)
        args = (hash_key, )
        kwargs = dict(
            range_key=range_key,
            consistent_read=consistent_read,
        )
        attrs = self._get_connection().get_item(*args, **kwargs)
        item_data = attrs.get(ITEM, None)
        if item_data is None:
            raise self.DoesNotExist("This item does not exist in the table.")
        self._deserialize(item_data)

    @classmethod
    def get(cls,
            hash_key,
            range_key=None,
            consistent_read=False,
            attributes_to_get=None):
        """
        Returns a single object using the provided keys

        :param hash_key: The hash key of the desired item
        :param range_key: The range key of the desired item, only used when appropriate.
        """
        hash_key, range_key = cls._serialize_keys(hash_key, range_key)
        data = cls._get_connection().get_item(
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get
        )
        if data:
            item_data = data.get(ITEM)
            if item_data:
                return cls.from_raw_data(item_data)
        raise cls.DoesNotExist()

    @classmethod
    def from_raw_data(cls, data):
        """
        Returns an instance of this class
        from the raw data

        :param data: A serialized DynamoDB object
        """
        if data is None:
            raise ValueError("Received no data to construct object")

        item = cls()
        item._deserialize(data)
        return item

    @classmethod
    def count(cls,
              hash_key=None,
              range_key_condition=None,
              filter_condition=None,
              consistent_read=False,
              index_name=None,
              limit=None,
              rate_limit=None):
        """
        Provides a filtered count

        :param hash_key: The hash key to query. Can be None.
        :param range_key_condition: Condition for range key
        :param filter_condition: Condition used to restrict the query results
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param rate_limit: If set then consumed capacity will be limited to this amount per second
        """
        if hash_key is None:
            if filter_condition is not None:
                raise ValueError('A hash_key must be given to use filters')
            return cls.describe_table().get(ITEM_COUNT)

        cls._get_indexes()
        if index_name:
            hash_key = cls._index_classes[index_name]._hash_key_attribute().serialize(hash_key)
        else:
            hash_key = cls._serialize_keys(hash_key)[0]

        query_args = (hash_key,)
        query_kwargs = dict(
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=index_name,
            consistent_read=consistent_read,
            limit=limit,
            select=COUNT
        )

        result_iterator = ResultIterator(
            cls._get_connection().query,
            query_args,
            query_kwargs,
            limit=limit,
            rate_limit=rate_limit
        )

        # iterate through results
        list(result_iterator)

        return result_iterator.total_count

    @classmethod
    def query(cls,
              hash_key,
              range_key_condition=None,
              filter_condition=None,
              consistent_read=False,
              index_name=None,
              scan_index_forward=None,
              limit=None,
              last_evaluated_key=None,
              attributes_to_get=None,
              page_size=None,
              rate_limit=None):
        """
        Provides a high level query API

        :param hash_key: The hash key to query
        :param range_key_condition: Condition for range key
        :param filter_condition: Condition used to restrict the query results
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param limit: Used to limit the number of results returned
        :param scan_index_forward: If set, then used to specify the same parameter to the DynamoDB API.
            Controls descending or ascending results
        :param last_evaluated_key: If set, provides the starting point for query.
        :param attributes_to_get: If set, only returns these elements
        :param page_size: Page size of the query to DynamoDB
        :param rate_limit: If set then consumed capacity will be limited to this amount per second
        """
        cls._get_indexes()
        if index_name:
            hash_key = cls._index_classes[index_name]._hash_key_attribute().serialize(hash_key)
        else:
            hash_key = cls._serialize_keys(hash_key)[0]

        if page_size is None:
            page_size = limit

        query_args = (hash_key,)
        query_kwargs = dict(
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=index_name,
            exclusive_start_key=last_evaluated_key,
            consistent_read=consistent_read,
            scan_index_forward=scan_index_forward,
            limit=page_size,
            attributes_to_get=attributes_to_get,
        )

        return ResultIterator(
            cls._get_connection().query,
            query_args,
            query_kwargs,
            map_fn=cls.from_raw_data,
            limit=limit,
            rate_limit=rate_limit
        )

    @classmethod
    def scan(cls,
             filter_condition=None,
             segment=None,
             total_segments=None,
             limit=None,
             last_evaluated_key=None,
             page_size=None,
             consistent_read=None,
             index_name=None,
             rate_limit=None):
        """
        Iterates through all items in the table

        :param filter_condition: Condition used to restrict the scan results
        :param segment: If set, then scans the segment
        :param total_segments: If set, then specifies total segments
        :param limit: Used to limit the number of results returned
        :param last_evaluated_key: If set, provides the starting point for scan.
        :param page_size: Page size of the scan to DynamoDB
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param rate_limit: If set then consumed capacity will be limited to this amount per second
        """
        if page_size is None:
            page_size = limit

        scan_args = ()
        scan_kwargs = dict(
            filter_condition=filter_condition,
            exclusive_start_key=last_evaluated_key,
            segment=segment,
            limit=page_size,
            total_segments=total_segments,
            consistent_read=consistent_read,
            index_name=index_name
        )

        return ResultIterator(
            cls._get_connection().scan,
            scan_args,
            scan_kwargs,
            map_fn=cls.from_raw_data,
            limit=limit,
            rate_limit=rate_limit,
        )

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
    def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None, billing_mode=None):
        """
        Create the table for this model

        :param wait: If set, then this call will block until the table is ready for use
        :param read_capacity_units: Sets the read capacity units for this table
        :param write_capacity_units: Sets the write capacity units for this table
        :param billing_mode: Sets the billing mode provisioned (default) or on_demand for this table
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
            if hasattr(cls.Meta, pythonic(BILLING_MODE)):
                schema[pythonic(BILLING_MODE)] = cls.Meta.billing_mode
            if read_capacity_units is not None:
                schema[pythonic(READ_CAPACITY_UNITS)] = read_capacity_units
            if write_capacity_units is not None:
                schema[pythonic(WRITE_CAPACITY_UNITS)] = write_capacity_units
            if billing_mode is not None:
                schema[pythonic(BILLING_MODE)] = billing_mode
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
        hash_key_attribute = cls._hash_key_attribute()
        hash_keyname = hash_key_attribute.attr_name
        hash_keytype = ATTR_TYPE_MAP[hash_key_attribute.attr_type]
        attributes[hash_keyname] = {
            hash_keytype: hash_key
        }
        if range_key is not None:
            range_key_attribute = cls._range_key_attribute()
            range_keyname = range_key_attribute.attr_name
            range_keytype = ATTR_TYPE_MAP[range_key_attribute.attr_type]
            attributes[range_keyname] = {
                range_keytype: range_key
            }
        return cls.from_raw_data(attributes)

    @classmethod
    def _get_schema(cls):
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
            for name, index in getmembers(cls, lambda o: isinstance(o, Index)):
                cls._index_classes[index.Meta.index_name] = index
                schema = index._get_schema()
                idx = {
                    pythonic(INDEX_NAME): index.Meta.index_name,
                    pythonic(KEY_SCHEMA): schema.get(pythonic(KEY_SCHEMA)),
                    pythonic(PROJECTION): {
                        PROJECTION_TYPE: index.Meta.projection.projection_type,
                    },

                }
                if isinstance(index, GlobalSecondaryIndex):
                    idx[pythonic(PROVISIONED_THROUGHPUT)] = {
                        READ_CAPACITY_UNITS: index.Meta.read_capacity_units,
                        WRITE_CAPACITY_UNITS: index.Meta.write_capacity_units
                    }
                cls._indexes[pythonic(ATTR_DEFINITIONS)].extend(schema.get(pythonic(ATTR_DEFINITIONS)))
                if index.Meta.projection.non_key_attributes:
                    idx[pythonic(PROJECTION)][NON_KEY_ATTRIBUTES] = index.Meta.projection.non_key_attributes
                if isinstance(index, GlobalSecondaryIndex):
                    cls._indexes[pythonic(GLOBAL_SECONDARY_INDEXES)].append(idx)
                else:
                    cls._indexes[pythonic(LOCAL_SECONDARY_INDEXES)].append(idx)
        return cls._indexes

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

    def _get_save_args(self):
        """
        Gets the proper *args, **kwargs for saving and retrieving this object

        This is used for serializing items to be saved, or for serializing just the keys.
        """
        kwargs = {}
        serialized = self._serialize(null_check=True)
        hash_key = serialized.get(HASH)
        range_key = serialized.get(RANGE, None)
        args = (hash_key, )
        if range_key is not None:
            kwargs[pythonic(RANGE_KEY)] = range_key
        kwargs[pythonic(ATTRIBUTES)] = serialized[pythonic(ATTRIBUTES)]
        return args, kwargs

    @classmethod
    def _hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        return cls.get_attributes()[cls._hash_keyname]

    @classmethod
    def _range_key_attribute(cls):
        """
        Returns the attribute class for the range key
        """
        return cls.get_attributes()[cls._range_keyname] if cls._range_keyname else None

    def _get_keys(self):
        """
        Returns the proper arguments for deleting
        """
        hash_key, range_key = self._serialize_keys(self._hash_key, self._range_key)
        attrs = {
            self._hash_key_attribute().attr_name: hash_key,
        }
        if self._range_keyname is not None:
            range_keyname = self._range_key_attribute().attr_name
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
            keys_to_get, consistent_read=consistent_read, attributes_to_get=attributes_to_get
        )
        item_data = data.get(RESPONSES).get(cls.Meta.table_name)
        unprocessed_items = data.get(UNPROCESSED_KEYS).get(cls.Meta.table_name, {}).get(KEYS, None)
        return item_data, unprocessed_items

    @classmethod
    def _get_connection(cls):
        """
        Returns a (cached) connection
        """
        if not hasattr(cls, "Meta"):
            raise AttributeError(
                'As of v1.0 PynamoDB Models require a `Meta` class.\n'
                'Model: {}.{}\n'
                'See https://pynamodb.readthedocs.io/en/latest/release_notes.html\n'.format(
                    cls.__module__, cls.__name__,
                ),
            )
        elif not hasattr(cls.Meta, "table_name") or cls.Meta.table_name is None:
            raise AttributeError(
                'As of v1.0 PyanmoDB Models must have a table_name\n'
                'Model: {}.{}\n'
                'See https://pynamodb.readthedocs.io/en/latest/release_notes.html'.format(
                    cls.__module__, cls.__name__,
                ),
            )
        if cls._connection is None:
            cls._connection = TableConnection(cls.Meta.table_name,
                                              region=cls.Meta.region,
                                              host=cls.Meta.host,
                                              connect_timeout_seconds=cls.Meta.connect_timeout_seconds,
                                              read_timeout_seconds=cls.Meta.read_timeout_seconds,
                                              max_retry_attempts=cls.Meta.max_retry_attempts,
                                              base_backoff_ms=cls.Meta.base_backoff_ms,
                                              max_pool_connections=cls.Meta.max_pool_connections,
                                              extra_headers=cls.Meta.extra_headers,
                                              aws_access_key_id=cls.Meta.aws_access_key_id,
                                              aws_secret_access_key=cls.Meta.aws_secret_access_key)
        return cls._connection

    def _deserialize(self, data):
        """
        Sets attributes sent back from DynamoDB on this object

        :param data: A dictionary of attributes to update this item with.
        """
        for name, attr in self.get_attributes().items():
            value = data.get(attr.attr_name)
            if value is not None:
                value = attr.get_value(value)
            if value is not None:
                value = attr.deserialize(value)
            setattr(self, name, value)

    def _serialize(self, attr_map=False, null_check=True):
        """
        Serializes all model attributes for use with DynamoDB

        :param attr_map: If True, then attributes are returned
        :param null_check: If True, then attributes are checked for null
        """
        attributes = pythonic(ATTRIBUTES)
        attrs = {attributes: {}}
        for name, attr in self.get_attributes().items():
            value = getattr(self, name)
            if isinstance(value, MapAttribute):
                if not value.validate():
                    raise ValueError("Attribute '{}' is not correctly typed".format(attr.attr_name))

            serialized = self._serialize_value(attr, value, null_check)
            if NULL in serialized:
                continue

            if attr_map:
                attrs[attributes][attr.attr_name] = serialized
            else:
                if attr.is_hash_key:
                    attrs[HASH] = serialized[ATTR_TYPE_MAP[attr.attr_type]]
                elif attr.is_range_key:
                    attrs[RANGE] = serialized[ATTR_TYPE_MAP[attr.attr_type]]
                else:
                    attrs[attributes][attr.attr_name] = serialized

        return attrs

    @classmethod
    def _serialize_value(cls, attr, value, null_check=True):
        """
        Serializes a value for use with DynamoDB

        :param attr: an instance of `Attribute` for serialization
        :param value: a value to be serialized
        :param null_check: If True, then attributes are checked for null
        """
        if value is None:
            serialized = None
        else:
            serialized = attr.serialize(value)

        if serialized is None:
            if not attr.null and null_check:
                raise ValueError("Attribute '{}' cannot be None".format(attr.attr_name))
            return {NULL: True}

        return {ATTR_TYPE_MAP[attr.attr_type]: serialized}

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
