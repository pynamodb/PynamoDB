"""
PynamoDB Indexes
"""
from inspect import getmembers
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar
from typing import TYPE_CHECKING

from pynamodb.constants import (
    INCLUDE, ALL, KEYS_ONLY, ATTR_NAME, ATTR_TYPE, KEY_TYPE, KEY_SCHEMA,
    ATTR_DEFINITIONS, PROJECTION_TYPE, NON_KEY_ATTRIBUTES,
    READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS,
)
from pynamodb.attributes import Attribute
from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator
from pynamodb.types import HASH, RANGE

if TYPE_CHECKING:
    from pynamodb.models import Model

_KeyType = Any
_M = TypeVar('_M', bound='Model')


class Index(Generic[_M]):
    """
    Base class for secondary indexes
    """
    Meta: Any = None

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)  # type: ignore  # see https://github.com/python/mypy/issues/4660
        if cls.Meta is not None:
            cls.Meta.attributes = {}
            for name, attribute in getmembers(cls, lambda o: isinstance(o, Attribute)):
                cls.Meta.attributes[name] = attribute

    def __init__(self) -> None:
        if self.Meta is None:
            raise ValueError("Indexes require a Meta class for settings")
        if not hasattr(self.Meta, "projection"):
            raise ValueError("No projection defined, define a projection for this class")

    def __set_name__(self, owner: Type[_M], name: str):
        if not hasattr(self.Meta, "model"):
            self.Meta.model = owner
        if not hasattr(self.Meta, "index_name"):
            self.Meta.index_name = name

    @classmethod
    def count(
        cls,
        hash_key: _KeyType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
    ) -> int:
        """
        Count on an index
        """
        return cls.Meta.model.count(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=cls.Meta.index_name,
            consistent_read=consistent_read,
            limit=limit,
            rate_limit=rate_limit,
        )

    @classmethod
    def query(
        cls,
        hash_key: _KeyType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: Optional[bool] = False,
        scan_index_forward: Optional[bool] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        attributes_to_get: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        rate_limit: Optional[float] = None,
    ) -> ResultIterator[_M]:
        """
        Queries an index
        """
        return cls.Meta.model.query(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            consistent_read=consistent_read,
            index_name=cls.Meta.index_name,
            scan_index_forward=scan_index_forward,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            page_size=page_size,
            rate_limit=rate_limit,
        )

    @classmethod
    def scan(
        cls,
        filter_condition: Optional[Condition] = None,
        segment: Optional[int] = None,
        total_segments: Optional[int] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        page_size: Optional[int] = None,
        consistent_read: Optional[bool] = None,
        rate_limit: Optional[float] = None,
        attributes_to_get: Optional[List[str]] = None,
    ) -> ResultIterator[_M]:
        """
        Scans an index
        """
        return cls.Meta.model.scan(
            filter_condition=filter_condition,
            segment=segment,
            total_segments=total_segments,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            page_size=page_size,
            consistent_read=consistent_read,
            index_name=cls.Meta.index_name,
            rate_limit=rate_limit,
            attributes_to_get=attributes_to_get,
        )

    @classmethod
    def _hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        for attr_cls in cls.Meta.attributes.values():
            if attr_cls.is_hash_key:
                return attr_cls

    @classmethod
    def _get_schema(cls) -> Dict:
        """
        Returns the schema for this index
        """
        schema = {
            'index_name': cls.Meta.index_name,
            'key_schema': [],
            'projection': {
                PROJECTION_TYPE: cls.Meta.projection.projection_type,
            },
        }
        for attr_cls in cls.Meta.attributes.values():
            if attr_cls.is_hash_key:
                schema['key_schema'].append({
                    ATTR_NAME: attr_cls.attr_name,
                    KEY_TYPE: HASH
                })
            elif attr_cls.is_range_key:
                schema['key_schema'].append({
                    ATTR_NAME: attr_cls.attr_name,
                    KEY_TYPE: RANGE
                })
        if cls.Meta.projection.non_key_attributes:
            schema['projection'][NON_KEY_ATTRIBUTES] = cls.Meta.projection.non_key_attributes
        return schema


class GlobalSecondaryIndex(Index[_M]):
    """
    A global secondary index
    """

    @classmethod
    def _get_schema(cls) -> Dict:
        schema = super()._get_schema()
        provisioned_throughput = {}
        if hasattr(cls.Meta, 'read_capacity_units'):
            provisioned_throughput[READ_CAPACITY_UNITS] = cls.Meta.read_capacity_units
        if hasattr(cls.Meta, 'write_capacity_units'):
            provisioned_throughput[WRITE_CAPACITY_UNITS] = cls.Meta.write_capacity_units
        schema['provisioned_throughput'] = provisioned_throughput
        return schema


class LocalSecondaryIndex(Index[_M]):
    """
    A local secondary index
    """
    pass


class Projection(object):
    """
    A class for presenting projections
    """
    projection_type: Any = None
    non_key_attributes: Any = None


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

    def __init__(self, non_attr_keys: Optional[List[str]] = None) -> None:
        if not non_attr_keys:
            raise ValueError("The INCLUDE type projection requires a list of string attribute names")
        self.non_key_attributes = non_attr_keys


class AllProjection(Projection):
    """
    An ALL projection
    """
    projection_type = ALL
