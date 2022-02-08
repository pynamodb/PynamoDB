"""
DynamoDB Models for PynamoDB
"""
import json
import random
import time
import logging
import warnings
import sys
from inspect import getmembers
from typing import Any
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Text
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
from typing import cast

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

from pynamodb.expressions.update import Action
from pynamodb.exceptions import DoesNotExist, TableDoesNotExist, TableError, InvalidStateError, PutError, \
    AttributeNullError
from pynamodb.attributes import (
    AttributeContainer, AttributeContainerMeta, TTLAttribute, VersionAttribute
)
from pynamodb.connection.table import TableConnection
from pynamodb.expressions.condition import Condition
from pynamodb.types import HASH, RANGE
from pynamodb.indexes import Index, GlobalSecondaryIndex
from pynamodb.pagination import ResultIterator
from pynamodb.settings import get_settings_value, OperationSettings
from pynamodb.constants import (
    ATTR_DEFINITIONS, ATTR_NAME, ATTR_TYPE, KEY_SCHEMA,
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
    STREAM_SPECIFICATION, STREAM_ENABLED, BILLING_MODE, PAY_PER_REQUEST_BILLING_MODE, TAGS
)
from pynamodb.util import attribute_value_to_json
from pynamodb.util import json_to_attribute_value

_T = TypeVar('_T', bound='Model')
_KeyType = Any


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class BatchWrite(Generic[_T]):
    """
    A class for batch writes
    """
    def __init__(self, model: Type[_T], auto_commit: bool = True, settings: OperationSettings = OperationSettings.default):
        self.model = model
        self.auto_commit = auto_commit
        self.max_operations = BATCH_WRITE_PAGE_LIMIT
        self.pending_operations: List[Dict[str, Any]] = []
        self.failed_operations: List[Any] = []
        self.settings = settings

    def save(self, put_item: _T) -> None:
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

    def delete(self, del_item: _T) -> None:
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This ensures that all pending operations are committed when
        the context is exited
        """
        return self.commit()

    def commit(self) -> None:
        """
        Writes all of the changes that are pending
        """
        log.debug("%s committing batch operation", self.model)
        put_items = []
        delete_items = []
        for item in self.pending_operations:
            if item['action'] == PUT:
                put_items.append(item['item'].serialize())
            elif item['action'] == DELETE:
                delete_items.append(item['item']._get_keys())
        self.pending_operations = []
        if not len(put_items) and not len(delete_items):
            return
        data = self.model._get_connection().batch_write_item(
            put_items=put_items,
            delete_items=delete_items,
            settings=self.settings,
        )
        if data is None:
            return
        retries = 0
        unprocessed_items = data.get(UNPROCESSED_ITEMS, {}).get(self.model.Meta.table_name)
        while unprocessed_items:
            sleep_time = random.randint(0, self.model.Meta.base_backoff_ms * (2 ** retries)) / 1000
            time.sleep(sleep_time)
            retries += 1
            if retries >= self.model.Meta.max_retry_attempts:
                self.failed_operations = unprocessed_items
                raise PutError("Failed to batch write items: max_retry_attempts exceeded")
            put_items = []
            delete_items = []
            for item in unprocessed_items:
                if PUT_REQUEST in item:
                    put_items.append(item.get(PUT_REQUEST).get(ITEM))  # type: ignore
                elif DELETE_REQUEST in item:
                    delete_items.append(item.get(DELETE_REQUEST).get(KEY))  # type: ignore
            log.info("Resending %d unprocessed keys for batch operation after %d seconds sleep",
                     len(unprocessed_items), sleep_time)
            data = self.model._get_connection().batch_write_item(
                put_items=put_items,
                delete_items=delete_items,
                settings=self.settings,
            )
            unprocessed_items = data.get(UNPROCESSED_ITEMS, {}).get(self.model.Meta.table_name)


class MetaProtocol(Protocol):
    table_name: str
    read_capacity_units: Optional[int]
    write_capacity_units: Optional[int]
    region: Optional[str]
    host: Optional[str]
    connect_timeout_seconds: int
    read_timeout_seconds: int
    base_backoff_ms: int
    max_retry_attempts: int
    max_pool_connections: int
    extra_headers: Mapping[str, str]
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    aws_session_token: Optional[str]
    billing_mode: Optional[str]
    tags: Optional[Dict[str, str]]
    stream_view_type: Optional[str]


class MetaModel(AttributeContainerMeta):
    """
    Model meta class
    """
    def __new__(cls, name, bases, namespace, discriminator=None):
        # Defined so that the discriminator can be set in the class definition.
        return super().__new__(cls, name, bases, namespace)

    def __init__(self, name, bases, namespace, discriminator=None) -> None:
        super().__init__(name, bases, namespace, discriminator)
        MetaModel._initialize_indexes(self)
        cls = cast(Type['Model'], self)
        for attr_name, attribute in cls.get_attributes().items():
            if attribute.is_hash_key:
                if cls._hash_keyname and cls._hash_keyname != attr_name:
                    raise ValueError(f"{cls.__name__} has more than one hash key: {cls._hash_keyname}, {attr_name}")
                cls._hash_keyname = attr_name
            if attribute.is_range_key:
                if cls._range_keyname and cls._range_keyname != attr_name:
                    raise ValueError(f"{cls.__name__} has more than one range key: {cls._range_keyname}, {attr_name}")
                cls._range_keyname = attr_name
            if isinstance(attribute, VersionAttribute):
                if cls._version_attribute_name and cls._version_attribute_name != attr_name:
                    raise ValueError(
                        "The model has more than one Version attribute: {}, {}"
                        .format(cls._version_attribute_name, attr_name)
                    )
                cls._version_attribute_name = attr_name

        ttl_attr_names = [name for name, attr in cls.get_attributes().items() if isinstance(attr, TTLAttribute)]
        if len(ttl_attr_names) > 1:
            raise ValueError("{} has more than one TTL attribute: {}".format(
                cls.__name__, ", ".join(ttl_attr_names)))

        if isinstance(namespace, dict):
            for attr_name, attr_obj in namespace.items():
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
                    if not hasattr(attr_obj, 'aws_session_token'):
                        setattr(attr_obj, 'aws_session_token', None)

            # create a custom Model.DoesNotExist derived from pynamodb.exceptions.DoesNotExist,
            # so that "except Model.DoesNotExist:" would not catch other models' exceptions
            if 'DoesNotExist' not in namespace:
                exception_attrs = {
                    '__module__': namespace.get('__module__'),
                    '__qualname__': f'{cls.__qualname__}.{"DoesNotExist"}',
                }
                cls.DoesNotExist = type('DoesNotExist', (DoesNotExist, ), exception_attrs)

    @staticmethod
    def _initialize_indexes(cls):
        """
        Initialize indexes on the class.
        """
        cls._indexes = {}
        for name, index in getmembers(cls, lambda o: isinstance(o, Index)):
            cls._indexes[index.Meta.index_name] = index


class Model(AttributeContainer, metaclass=MetaModel):
    """
    Defines a `PynamoDB` Model

    This model is backed by a table in DynamoDB.
    You can create the table by with the ``create_table`` method.
    """

    # These attributes are named to avoid colliding with user defined
    # DynamoDB attributes
    _hash_keyname: Optional[str] = None
    _range_keyname: Optional[str] = None
    _connection: Optional[TableConnection] = None
    DoesNotExist: Type[DoesNotExist] = DoesNotExist
    _version_attribute_name: Optional[str] = None

    Meta: MetaProtocol
    _indexes: Dict[str, Index]

    def __init__(
        self,
        hash_key: Optional[_KeyType] = None,
        range_key: Optional[_KeyType] = None,
        _user_instantiated: bool = True,
        **attributes: Any,
    ) -> None:
        """
        :param hash_key: Required. The hash key for this object.
        :param range_key: Only required if the table has a range key attribute.
        :param attrs: A dictionary of attributes to set on this object.
        """
        if hash_key is not None:
            if self._hash_keyname is None:
                raise ValueError(f"This model has no hash key, but a hash key value was provided: {hash_key}")
            attributes[self._hash_keyname] = hash_key
        if range_key is not None:
            if self._range_keyname is None:
                raise ValueError(f"This model has no range key, but a range key value was provided: {range_key}")
            attributes[self._range_keyname] = range_key
        super(Model, self).__init__(_user_instantiated=_user_instantiated, **attributes)

    @classmethod
    def batch_get(
        cls: Type[_T],
        items: Iterable[Union[_KeyType, Iterable[_KeyType]]],
        consistent_read: Optional[bool] = None,
        attributes_to_get: Optional[Sequence[str]] = None,
        settings: OperationSettings = OperationSettings.default
    ) -> Iterator[_T]:
        """
        BatchGetItem for this model

        :param items: Should be a list of hash keys to retrieve, or a list of
            tuples if range keys are used.
        """
        items = set(items)
        hash_key_attribute = cls._hash_key_attribute()
        range_key_attribute = cls._range_key_attribute()
        keys_to_get: List[Any] = []
        while items:
            if len(keys_to_get) == BATCH_GET_PAGE_LIMIT:
                while keys_to_get:
                    page, unprocessed_keys = cls._batch_get_page(
                        keys_to_get,
                        consistent_read=consistent_read,
                        attributes_to_get=attributes_to_get,
                        settings=settings,
                    )
                    for batch_item in page:
                        yield cls.from_raw_data(batch_item)
                    if unprocessed_keys:
                        keys_to_get = unprocessed_keys
                    else:
                        keys_to_get = []
            item = items.pop()
            if range_key_attribute:
                hash_key, range_key = cls._serialize_keys(item[0], item[1])  # type: ignore
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
                attributes_to_get=attributes_to_get,
                settings=settings,
            )
            for batch_item in page:
                yield cls.from_raw_data(batch_item)
            if unprocessed_keys:
                keys_to_get = unprocessed_keys
            else:
                keys_to_get = []

    @classmethod
    def batch_write(cls: Type[_T], auto_commit: bool = True, settings: OperationSettings = OperationSettings.default) -> BatchWrite[_T]:
        """
        Returns a BatchWrite context manager for a batch operation.

        :param auto_commit: If true, the context manager will commit writes incrementally
                            as items are written to as necessary to honor item count limits
                            in the DynamoDB API (see BatchWrite). Regardless of the value
                            passed here, changes automatically commit on context exit
                            (whether successful or not).
        """
        return BatchWrite(cls, auto_commit=auto_commit, settings=settings)

    def __repr__(self) -> str:
        hash_key, range_key = self._get_serialized_keys()
        if self._range_keyname:
            msg = "{}<{}, {}>".format(self.Meta.table_name, hash_key, range_key)
        else:
            msg = "{}<{}>".format(self.Meta.table_name, hash_key)
        return msg

    def delete(self, condition: Optional[Condition] = None, settings: OperationSettings = OperationSettings.default) -> Any:
        """
        Deletes this object from dynamodb

        :raises pynamodb.exceptions.DeleteError: If the record can not be deleted
        """
        hk_value, rk_value = self._get_hash_range_key_serialized_values()
        version_condition = self._handle_version_attribute()
        if version_condition is not None:
            condition &= version_condition

        return self._get_connection().delete_item(hk_value, range_key=rk_value, condition=condition, settings=settings)

    def update(self, actions: List[Action], condition: Optional[Condition] = None, settings: OperationSettings = OperationSettings.default) -> Any:
        """
        Updates an item using the UpdateItem operation.

        :param actions: a list of Action updates to apply
        :param condition: an optional Condition on which to update
        :param settings: per-operation settings
        :raises ModelInstance.DoesNotExist: if the object to be updated does not exist
        :raises pynamodb.exceptions.UpdateError: if the `condition` is not met
        """
        if not isinstance(actions, list) or len(actions) == 0:
            raise TypeError("the value of `actions` is expected to be a non-empty list")

        hk_value, rk_value = self._get_hash_range_key_serialized_values()
        version_condition = self._handle_version_attribute(actions=actions)
        if version_condition is not None:
            condition &= version_condition

        data = self._get_connection().update_item(hk_value, range_key=rk_value, return_values=ALL_NEW, condition=condition, actions=actions, settings=settings)
        item_data = data[ATTRIBUTES]
        stored_cls = self._get_discriminator_class(item_data)
        if stored_cls and stored_cls != type(self):
            raise ValueError("Cannot update this item from the returned class: {}".format(stored_cls.__name__))
        self.deserialize(item_data)
        return data

    def save(self, condition: Optional[Condition] = None, settings: OperationSettings = OperationSettings.default) -> Dict[str, Any]:
        """
        Save this object to dynamodb
        """
        args, kwargs = self._get_save_args(condition=condition)
        kwargs['settings'] = settings
        data = self._get_connection().put_item(*args, **kwargs)
        self.update_local_version_attribute()
        return data

    def refresh(self, consistent_read: bool = False, settings: OperationSettings = OperationSettings.default) -> None:
        """
        Retrieves this object's data from dynamodb and syncs this local object

        :param consistent_read: If True, then a consistent read is performed.
        :param settings: per-operation settings
        :raises ModelInstance.DoesNotExist: if the object to be updated does not exist
        """
        hk_value, rk_value = self._get_hash_range_key_serialized_values()
        attrs = self._get_connection().get_item(hk_value, range_key=rk_value, consistent_read=consistent_read, settings=settings)
        item_data = attrs.get(ITEM, None)
        if item_data is None:
            raise self.DoesNotExist("This item does not exist in the table.")
        stored_cls = self._get_discriminator_class(item_data)
        if stored_cls and stored_cls != type(self):
            raise ValueError("Cannot refresh this item from the returned class: {}".format(stored_cls.__name__))
        self.deserialize(item_data)

    def get_update_kwargs_from_instance(
        self,
        actions: List[Action],
        condition: Optional[Condition] = None,
        return_values_on_condition_failure: Optional[str] = None,
    ) -> Dict[str, Any]:
        hk_value, rk_value = self._get_hash_range_key_serialized_values()

        version_condition = self._handle_version_attribute(actions=actions)
        if version_condition is not None:
            condition &= version_condition

        return self._get_connection().get_operation_kwargs(hk_value, range_key=rk_value, key=KEY, actions=actions, condition=condition, return_values_on_condition_failure=return_values_on_condition_failure)

    def get_delete_kwargs_from_instance(
        self,
        condition: Optional[Condition] = None,
        return_values_on_condition_failure: Optional[str] = None,
    ) -> Dict[str, Any]:
        hk_value, rk_value = self._get_hash_range_key_serialized_values()

        version_condition = self._handle_version_attribute()
        if version_condition is not None:
            condition &= version_condition

        return self._get_connection().get_operation_kwargs(hk_value, range_key=rk_value, key=KEY, condition=condition, return_values_on_condition_failure=return_values_on_condition_failure)

    def get_save_kwargs_from_instance(
        self,
        condition: Optional[Condition] = None,
        return_values_on_condition_failure: Optional[str] = None,
    ) -> Dict[str, Any]:
        args, save_kwargs = self._get_save_args(null_check=True, condition=condition)
        save_kwargs['key'] = ITEM
        save_kwargs['return_values_on_condition_failure'] = return_values_on_condition_failure
        return self._get_connection().get_operation_kwargs(*args, **save_kwargs)

    @classmethod
    def get_operation_kwargs_from_class(
        cls,
        hash_key: str,
        range_key: Optional[_KeyType] = None,
        condition: Optional[Condition] = None,
    ) -> Dict[str, Any]:
        hash_key, range_key = cls._serialize_keys(hash_key, range_key)
        return cls._get_connection().get_operation_kwargs(
            hash_key=hash_key,
            range_key=range_key,
            condition=condition
        )

    @classmethod
    def get(
        cls: Type[_T],
        hash_key: _KeyType,
        range_key: Optional[_KeyType] = None,
        consistent_read: bool = False,
        attributes_to_get: Optional[Sequence[Text]] = None,
        settings: OperationSettings = OperationSettings.default
    ) -> _T:
        """
        Returns a single object using the provided keys

        :param hash_key: The hash key of the desired item
        :param range_key: The range key of the desired item, only used when appropriate.
        :param consistent_read:
        :param attributes_to_get:
        :raises ModelInstance.DoesNotExist: if the object to be updated does not exist
        """
        hash_key, range_key = cls._serialize_keys(hash_key, range_key)

        data = cls._get_connection().get_item(
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get,
            settings=settings,
        )
        if data:
            item_data = data.get(ITEM)
            if item_data:
                return cls.from_raw_data(item_data)
        raise cls.DoesNotExist()

    @classmethod
    def from_raw_data(cls: Type[_T], data: Dict[str, Any]) -> _T:
        """
        Returns an instance of this class
        from the raw data

        :param data: A serialized DynamoDB object
        """
        if data is None:
            raise ValueError("Received no data to construct object")

        return cls._instantiate(data)

    @classmethod
    def count(
        cls: Type[_T],
        hash_key: Optional[_KeyType] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> int:
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

        if index_name:
            hash_key = cls._indexes[index_name]._hash_key_attribute().serialize(hash_key)
        else:
            hash_key = cls._serialize_keys(hash_key)[0]

        # If this class has a discriminator attribute, filter the query to only return instances of this class.
        discriminator_attr = cls._get_discriminator_attribute()
        if discriminator_attr:
            filter_condition &= discriminator_attr.is_in(*discriminator_attr.get_registered_subclasses(cls))

        query_args = (hash_key,)
        query_kwargs = dict(
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=index_name,
            consistent_read=consistent_read,
            limit=limit,
            select=COUNT
        )

        result_iterator: ResultIterator[_T] = ResultIterator(
            cls._get_connection().query,
            query_args,
            query_kwargs,
            limit=limit,
            rate_limit=rate_limit,
            settings=settings,
        )

        # iterate through results
        list(result_iterator)

        return result_iterator.total_count

    @classmethod
    def query(
        cls: Type[_T],
        hash_key: _KeyType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        scan_index_forward: Optional[bool] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        attributes_to_get: Optional[Iterable[str]] = None,
        page_size: Optional[int] = None,
        rate_limit: Optional[float] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> ResultIterator[_T]:
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
        if index_name:
            hash_key = cls._indexes[index_name]._hash_key_attribute().serialize(hash_key)
        else:
            hash_key = cls._serialize_keys(hash_key)[0]

        # If this class has a discriminator attribute, filter the query to only return instances of this class.
        discriminator_attr = cls._get_discriminator_attribute()
        if discriminator_attr:
            filter_condition &= discriminator_attr.is_in(*discriminator_attr.get_registered_subclasses(cls))

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
            rate_limit=rate_limit,
            settings=settings,
        )

    @classmethod
    def scan(
        cls: Type[_T],
        filter_condition: Optional[Condition] = None,
        segment: Optional[int] = None,
        total_segments: Optional[int] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        page_size: Optional[int] = None,
        consistent_read: Optional[bool] = None,
        index_name: Optional[str] = None,
        rate_limit: Optional[float] = None,
        attributes_to_get: Optional[Sequence[str]] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> ResultIterator[_T]:
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
        :param attributes_to_get: If set, specifies the properties to include in the projection expression
        """
        # If this class has a discriminator attribute, filter the scan to only return instances of this class.
        discriminator_attr = cls._get_discriminator_attribute()
        if discriminator_attr:
            filter_condition &= discriminator_attr.is_in(*discriminator_attr.get_registered_subclasses(cls))

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
            index_name=index_name,
            attributes_to_get=attributes_to_get
        )

        return ResultIterator(
            cls._get_connection().scan,
            scan_args,
            scan_kwargs,
            map_fn=cls.from_raw_data,
            limit=limit,
            rate_limit=rate_limit,
            settings=settings,
        )

    @classmethod
    def exists(cls: Type[_T]) -> bool:
        """
        Returns True if this table exists, False otherwise
        """
        try:
            cls._get_connection().describe_table()
            return True
        except TableDoesNotExist:
            return False

    @classmethod
    def delete_table(cls) -> Any:
        """
        Delete the table for this model
        """
        return cls._get_connection().delete_table()

    @classmethod
    def describe_table(cls) -> Any:
        """
        Returns the result of a DescribeTable operation on this model's table
        """
        return cls._get_connection().describe_table()

    @classmethod
    def create_table(
        cls,
        wait: bool = False,
        read_capacity_units: Optional[int] = None,
        write_capacity_units: Optional[int] = None,
        billing_mode: Optional[str] = None,
        ignore_update_ttl_errors: bool = False,
    ) -> Any:
        """
        Create the table for this model

        :param wait: If set, then this call will block until the table is ready for use
        :param read_capacity_units: Sets the read capacity units for this table
        :param write_capacity_units: Sets the write capacity units for this table
        :param billing_mode: Sets the billing mode 'PROVISIONED' (default) or 'PAY_PER_REQUEST' for this table
        """
        if not cls.exists():
            schema = cls._get_schema()
            if hasattr(cls.Meta, 'read_capacity_units'):
                schema['read_capacity_units'] = cls.Meta.read_capacity_units
            if hasattr(cls.Meta, 'write_capacity_units'):
                schema['write_capacity_units'] = cls.Meta.write_capacity_units
            if hasattr(cls.Meta, 'stream_view_type'):
                schema['stream_specification'] = {
                    'stream_enabled': True,
                    'stream_view_type': cls.Meta.stream_view_type
                }
            if hasattr(cls.Meta, 'billing_mode'):
                schema['billing_mode'] = cls.Meta.billing_mode
            if hasattr(cls.Meta, 'tags'):
                schema['tags'] = cls.Meta.tags
            if read_capacity_units is not None:
                schema['read_capacity_units'] = read_capacity_units
            if write_capacity_units is not None:
                schema['write_capacity_units'] = write_capacity_units
            if billing_mode is not None:
                schema['billing_mode'] = billing_mode
            cls._get_connection().create_table(
                **schema
            )
        if wait:
            while True:
                status = cls._get_connection().describe_table()
                if status:
                    data = status.get(TABLE_STATUS)
                    if data == ACTIVE:
                        break
                    else:
                        time.sleep(2)
                else:
                    raise TableError("No TableStatus returned for table")

        cls.update_ttl(ignore_update_ttl_errors)

    @classmethod
    def update_ttl(cls, ignore_update_ttl_errors: bool) -> None:
        """
        Attempt to update the TTL on the table.
        Certain implementations (eg: dynalite) do not support updating TTLs and will fail.
        """
        ttl_attribute = cls._ttl_attribute()
        if ttl_attribute:
            # Some dynamoDB implementations (eg: dynalite) do not support updating TTLs so
            # this will fail.  It's fine for this to fail in those cases.
            try:
                cls._get_connection().update_time_to_live(ttl_attribute.attr_name)
            except Exception:
                if ignore_update_ttl_errors:
                    log.info("Unable to update the TTL for {}".format(cls.Meta.table_name))
                else:
                    raise

    # Private API below
    @classmethod
    def _get_schema(cls) -> Dict[str, Any]:
        """
        Returns the schema for this table
        """
        schema: Dict[str, List] = {
            'attribute_definitions': [],
            'key_schema': [],
            'global_secondary_indexes': [],
            'local_secondary_indexes': [],
        }
        for attr_name, attr_cls in cls.get_attributes().items():
            if attr_cls.is_hash_key or attr_cls.is_range_key:
                schema['attribute_definitions'].append({
                    'attribute_name': attr_cls.attr_name,
                    'attribute_type': attr_cls.attr_type
                })
            if attr_cls.is_hash_key:
                schema['key_schema'].append({
                    'key_type': HASH,
                    'attribute_name': attr_cls.attr_name
                })
            elif attr_cls.is_range_key:
                schema['key_schema'].append({
                    'key_type': RANGE,
                    'attribute_name': attr_cls.attr_name
                })
        for index in cls._indexes.values():
            index_schema = index._get_schema()
            if isinstance(index, GlobalSecondaryIndex):
                if getattr(cls.Meta, 'billing_mode', None) == PAY_PER_REQUEST_BILLING_MODE:
                    index_schema.pop('provisioned_throughput', None)
                schema['global_secondary_indexes'].append(index_schema)
            else:
                schema['local_secondary_indexes'].append(index_schema)
        attr_names = {key_schema[ATTR_NAME]
                      for index_schema in (*schema['global_secondary_indexes'], *schema['local_secondary_indexes'])
                      for key_schema in index_schema['key_schema']}
        attr_keys = {attr.get('attribute_name') for attr in schema['attribute_definitions']}
        for attr_name in attr_names:
            if attr_name not in attr_keys:
                attr_cls = cls.get_attributes()[cls._dynamo_to_python_attr(attr_name)]
                schema['attribute_definitions'].append({
                    'attribute_name': attr_cls.attr_name,
                    'attribute_type': attr_cls.attr_type
                })
        return schema

    def _get_save_args(self, null_check: bool = True, condition: Optional[Condition] = None) -> Tuple[Iterable[Any], Dict[str, Any]]:
        """
        Gets the proper *args, **kwargs for saving and retrieving this object

        This is used for serializing items to be saved, or for serializing just the keys.

        :param null_check: If True, then attributes are checked for null.
        :param condition: If set, a condition
        """
        attribute_values = self.serialize(null_check)
        hash_key_attribute = self._hash_key_attribute()
        hash_key = attribute_values.pop(hash_key_attribute.attr_name, {}).get(hash_key_attribute.attr_type)
        range_key = None
        range_key_attribute = self._range_key_attribute()
        if range_key_attribute:
            range_key = attribute_values.pop(range_key_attribute.attr_name, {}).get(range_key_attribute.attr_type)
        args = (hash_key, )
        kwargs = {}
        if range_key is not None:
            kwargs['range_key'] = range_key
        version_condition = self._handle_version_attribute(attributes=attribute_values)
        if version_condition is not None:
            condition &= version_condition
        kwargs['attributes'] = attribute_values
        kwargs['condition'] = condition
        return args, kwargs

    def _get_hash_range_key_serialized_values(self) -> Tuple[Any, Optional[Any]]:
        if self._hash_keyname is None:
            raise Exception("The model has no hash key")

        attrs = self.get_attributes()

        hk_value = getattr(self, self._hash_keyname)
        hk_serialized_value = attrs[self._hash_keyname].serialize(hk_value)

        rk_serialized_value = None
        if self._range_keyname:
            rk_value = getattr(self, self._range_keyname)
            if rk_value is not None:
                rk_serialized_value = attrs[self._range_keyname].serialize(rk_value)

        return hk_serialized_value, rk_serialized_value

    def _handle_version_attribute(self, *, attributes: Optional[Dict[str, Any]] = None, actions: Optional[List[Action]] = None) -> Optional[Condition]:
        """
        Handles modifying the request to set or increment the version attribute.
        """
        if self._version_attribute_name is None:
            return None

        version_attribute = self.get_attributes()[self._version_attribute_name]
        value = getattr(self, self._version_attribute_name)

        if value is not None:
            condition = version_attribute == value
            if attributes is not None:
                attributes[version_attribute.attr_name] = self._serialize_value(version_attribute, value + 1)
            if actions is not None:
                actions.append(version_attribute.add(1))
        else:
            condition = version_attribute.does_not_exist()
            if attributes is not None:
                attributes[version_attribute.attr_name] = self._serialize_value(version_attribute, 1)
            if actions is not None:
                actions.append(version_attribute.set(1))

        return condition

    def update_local_version_attribute(self):
        if self._version_attribute_name is not None:
            value = getattr(self, self._version_attribute_name, None) or 0
            setattr(self, self._version_attribute_name, value + 1)

    @classmethod
    def _hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        return cls.get_attributes()[cls._hash_keyname] if cls._hash_keyname else None

    @classmethod
    def _range_key_attribute(cls):
        """
        Returns the attribute class for the range key
        """
        return cls.get_attributes()[cls._range_keyname] if cls._range_keyname else None

    @classmethod
    def _ttl_attribute(cls):
        """
        Returns the ttl attribute for this table
        """
        attributes = cls.get_attributes()
        for attr_obj in attributes.values():
            if isinstance(attr_obj, TTLAttribute):
                return attr_obj
        return None

    def _get_keys(self):
        """
        Returns the proper arguments for deleting
        """
        hash_key, range_key = self._get_serialized_keys()
        hash_key_attribute = self._hash_key_attribute()
        range_key_attribute = self._range_key_attribute()
        attrs = {}
        if hash_key_attribute:
            attrs[hash_key_attribute.attr_name] = hash_key
        if range_key_attribute:
            attrs[range_key_attribute.attr_name] = range_key
        return attrs

    def _get_serialized_keys(self) -> Tuple[_KeyType, _KeyType]:
        hash_key = getattr(self, self._hash_keyname) if self._hash_keyname else None
        range_key = getattr(self, self._range_keyname) if self._range_keyname else None
        return self._serialize_keys(hash_key, range_key)

    @classmethod
    def _batch_get_page(cls, keys_to_get, consistent_read, attributes_to_get, settings: OperationSettings):
        """
        Returns a single page from BatchGetItem
        Also returns any unprocessed items

        :param keys_to_get: A list of keys
        :param consistent_read: Whether or not this needs to be consistent
        :param attributes_to_get: A list of attributes to return
        """
        log.debug("Fetching a BatchGetItem page")
        data = cls._get_connection().batch_get_item(
            keys_to_get, consistent_read=consistent_read, attributes_to_get=attributes_to_get, settings=settings,
        )
        item_data = data.get(RESPONSES).get(cls.Meta.table_name)  # type: ignore
        unprocessed_items = data.get(UNPROCESSED_KEYS).get(cls.Meta.table_name, {}).get(KEYS, None)  # type: ignore
        return item_data, unprocessed_items

    @classmethod
    def _get_connection(cls) -> TableConnection:
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
                'As of v1.0 PynamoDB Models must have a table_name\n'
                'Model: {}.{}\n'
                'See https://pynamodb.readthedocs.io/en/latest/release_notes.html'.format(
                    cls.__module__, cls.__name__,
                ),
            )
        # For now we just check that the connection exists and (in the case of model inheritance)
        # points to the same table. In the future we should update the connection if any of the attributes differ.
        if cls._connection is None or cls._connection.table_name != cls.Meta.table_name:
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
                                              aws_secret_access_key=cls.Meta.aws_secret_access_key,
                                              aws_session_token=cls.Meta.aws_session_token)
        return cls._connection

    @classmethod
    def _serialize_value(cls, attr, value):
        """
        Serializes a value for use with DynamoDB

        :param attr: an instance of `Attribute` for serialization
        :param value: a value to be serialized
        """
        serialized = attr.serialize(value)

        if serialized is None:
            if not attr.null:
                raise AttributeNullError(attr.attr_name)
            return {NULL: True}

        return {attr.attr_type: serialized}

    @classmethod
    def _serialize_keys(cls, hash_key, range_key=None) -> Tuple[_KeyType, _KeyType]:
        """
        Serializes the hash and range keys

        :param hash_key: The hash key value
        :param range_key: The range key value
        """
        if hash_key is not None:
            hash_key = cls._hash_key_attribute().serialize(hash_key)
        if range_key is not None:
            range_key = cls._range_key_attribute().serialize(range_key)
        return hash_key, range_key

    def serialize(self, null_check: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        Serialize attribute values for DynamoDB
        """
        return self._container_serialize(null_check=null_check)

    def deserialize(self, attribute_values: Dict[str, Dict[str, Any]]) -> None:
        """
        Sets attributes sent back from DynamoDB on this object
        """
        return self._container_deserialize(attribute_values=attribute_values)

    def to_json(self) -> str:
        return json.dumps({k: attribute_value_to_json(v) for k, v in self.serialize().items()})

    def from_json(self, s: str) -> None:
        attribute_values = {k: json_to_attribute_value(v) for k, v in json.loads(s).items()}
        self._update_attribute_types(attribute_values)
        self.deserialize(attribute_values)


class _ModelFuture(Generic[_T]):
    """
    A placeholder object for a model that does not exist yet

    For example: when performing a TransactGet request, this is a stand-in for a model that will be returned
    when the operation is complete
    """
    def __init__(self, model_cls: Type[_T]) -> None:
        self._model_cls = model_cls
        self._model: Optional[_T] = None
        self._resolved = False

    def update_with_raw_data(self, data: Dict[str, Any]) -> None:
        if data is not None and data != {}:
            self._model = self._model_cls.from_raw_data(data=data)
        self._resolved = True

    def get(self) -> _T:
        if not self._resolved:
            raise InvalidStateError()
        if self._model:
            return self._model
        raise self._model_cls.DoesNotExist()
