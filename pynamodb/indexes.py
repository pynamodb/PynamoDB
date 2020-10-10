"""
PynamoDB Indexes
"""
from inspect import getmembers
from typing import Any, Dict, Generic, List, Optional, TypeVar
from typing import TYPE_CHECKING

from pynamodb._compat import GenericMeta
from pynamodb.constants import (
    INCLUDE, ALL, KEYS_ONLY, ATTR_NAME, ATTR_TYPE, KEY_TYPE, KEY_SCHEMA,
    ATTR_DEFINITIONS, META_CLASS_NAME
)
from pynamodb.attributes import Attribute
from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator
from pynamodb.types import HASH, RANGE

if TYPE_CHECKING:
    from pynamodb.models import Model

_KeyType = Any
_M = TypeVar('_M', bound='Model')


class IndexMeta(GenericMeta):
    """
    Index meta class

    This class is here to allow for an index `Meta` class
    that contains the index settings
    """
    def __init__(self, name, bases, attrs, *args, **kwargs):
        super().__init__(name, bases, attrs, *args, **kwargs)  # type: ignore
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == META_CLASS_NAME:
                    meta_cls = attrs.get(META_CLASS_NAME)
                    if meta_cls is not None:
                        meta_cls.attributes = None
                elif isinstance(attr_obj, Attribute):
                    if attr_obj.attr_name is None:
                        attr_obj.attr_name = attr_name


class Index(Generic[_M], metaclass=IndexMeta):
    """
    Base class for secondary indexes
    """
    Meta: Any = None

    def __init__(self) -> None:
        if self.Meta is None:
            raise ValueError("Indexes require a Meta class for settings")
        if not hasattr(self.Meta, "projection"):
            raise ValueError("No projection defined, define a projection for this class")

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
    ):
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
        for attr_cls in cls._get_attributes().values():
            if attr_cls.is_hash_key:
                return attr_cls

    @classmethod
    def _get_schema(cls) -> Dict:
        """
        Returns the schema for this index
        """
        attr_definitions = []
        schema = []
        for attr_name, attr_cls in cls._get_attributes().items():
            attr_definitions.append({
                'attribute_name': attr_cls.attr_name,
                'attribute_type': attr_cls.attr_type
            })
            if attr_cls.is_hash_key:
                schema.append({
                    ATTR_NAME: attr_cls.attr_name,
                    KEY_TYPE: HASH
                })
            elif attr_cls.is_range_key:
                schema.append({
                    ATTR_NAME: attr_cls.attr_name,
                    KEY_TYPE: RANGE
                })
        return {
            'key_schema': schema,
            'attribute_definitions': attr_definitions
        }

    @classmethod
    def _get_attributes(cls):
        """
        Returns the list of attributes for this class
        """
        if cls.Meta.attributes is None:
            cls.Meta.attributes = {}
            for name, attribute in getmembers(cls, lambda o: isinstance(o, Attribute)):
                cls.Meta.attributes[name] = attribute
        return cls.Meta.attributes


class GlobalSecondaryIndex(Index[_M]):
    """
    A global secondary index
    """
    pass


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
