from typing import Any, Dict, List, Optional, Text, TypeVar, Generic, Union

from pynamodb.expressions.condition import Condition
from pynamodb.models import Model
from pynamodb.pagination import ResultIterator

_M = TypeVar('_M', bound=Model)
ConditionType = Union[Optional[Condition], Optional[bool]]


class IndexMeta(type):
    def __init__(cls, name, bases, attrs) -> None: ...


class Index(Generic[_M], metaclass=IndexMeta):
    Meta: Any
    def __init__(self) -> None: ...
    @classmethod
    def count(
        cls,
        hash_key,
        range_key_condition: ConditionType = ...,
        filter_condition: ConditionType = ...,
        consistent_read: bool = ...,
        limit: Optional[int] = ...,
        rate_limit: Optional[float] = ...,
    ) -> int: ...
    @classmethod
    def query(
        cls,
        hash_key,
        range_key_condition: ConditionType = ...,
        filter_condition: ConditionType = ...,
        scan_index_forward: Optional[Any] = ...,
        consistent_read: Optional[bool] = ...,
        limit: Optional[int] = ...,
        last_evaluated_key: Optional[Dict[Text, Dict[Text, Any]]] = ...,
        attributes_to_get: Optional[Any] = ...,
        page_size: Optional[int] = ...,
        rate_limit: Optional[float] = ...,
    ) -> ResultIterator[_M]: ...
    @classmethod
    def scan(
        cls,
        filter_condition: ConditionType = ...,
        segment: Optional[int] = ...,
        total_segments: Optional[int] = ...,
        limit: Optional[int] = ...,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = ...,
        page_size: Optional[int] = ...,
        consistent_read: Optional[bool] = ...,
        rate_limit: Optional[float] = ...,
        attributes_to_get: Optional[List[str]] = ...,
    ) -> ResultIterator[_M]: ...

class GlobalSecondaryIndex(Index[_M]): ...
class LocalSecondaryIndex(Index[_M]): ...

class Projection(object):
    projection_type: Any
    non_key_attributes: Any

class KeysOnlyProjection(Projection):
    projection_type: Any

class IncludeProjection(Projection):
    projection_type: Any
    non_key_attributes: Any
    def __init__(self, non_attr_keys: Optional[Any] = ...) -> None: ...

class AllProjection(Projection):
    projection_type: Any
