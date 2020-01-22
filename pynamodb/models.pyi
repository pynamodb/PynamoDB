
from .attributes import Attribute
from .exceptions import DoesNotExist as DoesNotExist
from typing import Any, Dict, Generic, Iterable, Iterator, List, Optional, Sequence, Tuple, Type, TypeVar, Text, Union

from pynamodb.connection.table import TableConnection
from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator

log: Any

class DefaultMeta: ...

class ResultSet(Iterable):
    results: Any
    operation: Any
    arguments: Any
    def __init__(self, results, operation, arguments) -> None: ...
    def __iter__(self): ...

class MetaModel(type):
    def __init__(self, name: Text, bases: Tuple[type, ...], attrs: Dict[Any, Any]) -> None: ...

_T = TypeVar('_T', bound='Model')
KeyType = Union[Text, bytes, float, int, Tuple]

class Model(metaclass=MetaModel):
    DoesNotExist = DoesNotExist
    attribute_values: Dict[Text, Any]
    _hash_keyname: Optional[str]
    _range_keyname: Optional[str]
    _connection: Optional[TableConnection]
    def __init__(self, hash_key: Optional[KeyType] = ..., range_key: Optional[Any] = ..., **attrs) -> None: ...
    @classmethod
    def has_map_or_list_attributes(cls: Type[_T]) -> bool: ...
    @classmethod
    def batch_get(cls: Type[_T], items: Iterable[Union[KeyType, Iterable[KeyType]]], consistent_read: Optional[bool] = ..., attributes_to_get: Optional[Sequence[Text]] = ...) -> Iterator[_T]: ...
    @classmethod
    def batch_write(cls: Type[_T], auto_commit: bool = ...) -> BatchWrite[_T]: ...
    def delete(self, condition: Optional[Any] = ...) -> Any: ...
    def update(self, actions: List[Any], condition: Optional[Condition] = ...) -> Any: ...
    def save(self, condition: Optional[Condition] = ...) -> Dict[str, Any]: ...
    def refresh(self, consistent_read: bool = ...): ...
    @classmethod
    def get(cls: Type[_T], hash_key: KeyType, range_key: Optional[KeyType] = ..., consistent_read: bool = ..., attributes_to_get: Optional[Sequence[Text]] = ...) -> _T: ...
    @classmethod
    def from_raw_data(cls: Type[_T], data) -> _T: ...

    @classmethod
    def count(
        cls: Type[_T],
        hash_key: Optional[KeyType] = ...,
        range_key_condition: Optional[Condition] = ...,
        filter_condition: Optional[Condition] = ...,
        consistent_read: bool = ...,
        index_name: Optional[Text] = ...,
        limit: Optional[int] = ...,
        rate_limit: Optional[float] = ...,
    ) -> int: ...

    @classmethod
    def query(
        cls: Type[_T],
        hash_key: KeyType,
        range_key_condition: Optional[Condition] = ...,
        filter_condition: Optional[Condition] = ...,
        consistent_read: bool = ...,
        index_name: Optional[Text] = ...,
        scan_index_forward: Optional[Any] = ...,
        limit: Optional[int] = ...,
        last_evaluated_key: Optional[Dict[Text, Dict[Text, Any]]] = ...,
        attributes_to_get: Optional[Iterable[Text]] = ...,
        page_size: Optional[int] = ...,
        rate_limit: Optional[float] = ...,
    ) -> ResultIterator[_T]: ...

    @classmethod
    def scan(
        cls: Type[_T],
        filter_condition: Optional[Condition] = ...,
        segment: Optional[int] = ...,
        total_segments: Optional[int] = ...,
        limit: Optional[int] = ...,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = ...,
        page_size: Optional[int] = ...,
        rate_limit: Optional[float] = ...,
        attributes_to_get: Optional[List[str]] = ...,
    ) -> ResultIterator[_T]: ...

    @classmethod
    def exists(cls: Type[_T]) -> bool: ...
    @classmethod
    def delete_table(cls): ...
    @classmethod
    def describe_table(cls): ...
    @classmethod
    def create_table(
        cls: Type[_T],
        wait: bool = ...,
        read_capacity_units: Optional[Any] = ...,
        write_capacity_units: Optional[Any] = ...,
        billing_mode: Optional[Any] = ...,
        ignore_update_ttl_errors: bool = ...
    ): ...
    @classmethod
    def update_ttl(cls, ignore_update_ttl_errors: bool): ...
    @classmethod
    def dumps(cls): ...
    @classmethod
    def dump(cls, filename): ...
    @classmethod
    def loads(cls, data): ...
    @classmethod
    def load(cls, filename): ...
    @classmethod
    def add_throttle_record(cls, records): ...
    @classmethod
    def get_throttle(cls): ...
    @classmethod
    def _ttl_attribute(cls): ...
    @classmethod
    def get_attributes(cls) -> Dict[str, Attribute]: ...
    @classmethod
    def _get_attributes(cls) -> Dict[str, Attribute]: ...
    @classmethod
    def _get_connection(cls) -> TableConnection: ...

class ModelContextManager(Generic[_T]):
    model: Type[_T]
    auto_commit: bool
    max_operations: int
    pending_operations: List[Dict[Text, Any]]
    def __init__(self, model: Type[_T], auto_commit: bool = ...) -> None: ...
    def __enter__(self) -> ModelContextManager[_T]: ...

class BatchWrite(Generic[_T], ModelContextManager[_T]):
    def save(self, put_item: _T) -> None: ...
    def delete(self, del_item: _T) -> None: ...
    def __enter__(self) -> BatchWrite[_T]: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
    pending_operations: Any
    def commit(self) -> None: ...

class _ModelFuture(Generic[_T]):
    _model_cls: Type[_T]
    _model: Optional[_T]
    _resolved: bool
    def __init__(self, model_cls: Type[_T]) -> None: ...
    def update_with_raw_data(self, data: Dict) -> None: ...
    def get(self) -> _T: ...
