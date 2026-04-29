"""
PynamoDB Indexes
"""
from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union
from typing import TYPE_CHECKING

from pynamodb._schema import IndexSchema, GlobalSecondaryIndexSchema
from pynamodb._schema import ModelSchema
from pynamodb.constants import (
    INCLUDE, ALL, KEYS_ONLY, ATTR_NAME, ATTR_TYPE, KEY_TYPE,
    PROJECTION_TYPE, NON_KEY_ATTRIBUTES,
    READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS,
)
from pynamodb.attributes import Attribute
from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator
from pynamodb.types import HASH, RANGE
if TYPE_CHECKING:
    from pynamodb.models import Model

_KeyType = object
_HashKeyInputType = Union[_KeyType, Tuple[_KeyType, ...], List[_KeyType]]
_SerializedHashKeyType = Union[_KeyType, Tuple[_KeyType, ...]]
_M = TypeVar('_M', bound='Model')


class Index(Generic[_M]):
    """
    Base class for secondary indexes
    """
    Meta: Any = None
    _model: _M

    @staticmethod
    def _get_attributes_in_declaration_order(index_cls: Type['Index']) -> Dict[str, Attribute]:
        """
        Returns attributes in declaration order, respecting overrides.
        """
        attributes: Dict[str, Attribute] = {}
        for base in reversed(index_cls.__mro__):
            for name, attribute in getattr(base, "__dict__", {}).items():
                if isinstance(attribute, Attribute):
                    # If a subclass overrides an attribute, preserve the subclass declaration order.
                    if name in attributes:
                        del attributes[name]
                    attributes[name] = attribute
        return attributes

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.Meta is not None:
            cls.Meta.attributes = cls._get_attributes_in_declaration_order(cls)

    def __init__(self) -> None:
        if self.Meta is None:
            raise ValueError("Indexes require a Meta class for settings")
        if not hasattr(self.Meta, "projection"):
            raise ValueError("No projection defined, define a projection for this class")

    def __set_name__(self, owner: Type[_M], name: str):
        if not hasattr(self.Meta, "index_name"):
            self.Meta.index_name = name

    def count(
        self,
        hash_key: _HashKeyInputType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
    ) -> int:
        """
        Count on an index
        """
        return self._model.count(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=self.Meta.index_name,
            consistent_read=consistent_read,
            limit=limit,
            rate_limit=rate_limit,
        )

    def query(
        self,
        hash_key: _HashKeyInputType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
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
        return self._model.query(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            consistent_read=consistent_read,
            index_name=self.Meta.index_name,
            scan_index_forward=scan_index_forward,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            page_size=page_size,
            rate_limit=rate_limit,
        )

    def scan(
        self,
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
        return self._model.scan(
            filter_condition=filter_condition,
            segment=segment,
            total_segments=total_segments,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            page_size=page_size,
            consistent_read=consistent_read,
            index_name=self.Meta.index_name,
            rate_limit=rate_limit,
            attributes_to_get=attributes_to_get,
        )

    @classmethod
    def _hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        hash_key_attributes = cls._hash_key_attributes()
        if hash_key_attributes:
            return hash_key_attributes[0]

    @classmethod
    def _hash_key_attributes(cls) -> List[Attribute]:
        return [attr for attr in cls.Meta.attributes.values() if attr.is_hash_key]

    @classmethod
    def _range_key_attributes(cls) -> List[Attribute]:
        return [attr for attr in cls.Meta.attributes.values() if attr.is_range_key]

    @classmethod
    def _serialize_hash_key_values(cls, hash_key: _HashKeyInputType) -> _SerializedHashKeyType:
        hash_key_attributes = cls._hash_key_attributes()
        if len(hash_key_attributes) <= 1:
            if len(hash_key_attributes) == 0:
                raise ValueError(f"{cls.__name__} has no hash key attributes")
            return hash_key_attributes[0].serialize(hash_key)

        if not isinstance(hash_key, (tuple, list)):
            raise ValueError(
                f"{cls.__name__} expects {len(hash_key_attributes)} hash key values as a tuple/list"
            )
        if len(hash_key) != len(hash_key_attributes):
            raise ValueError(
                f"{cls.__name__} expects {len(hash_key_attributes)} hash key values, got {len(hash_key)}"
            )
        return tuple(
            attr.serialize(value)
            for attr, value in zip(hash_key_attributes, hash_key)
        )

    @classmethod
    def _validate_key_attributes(cls) -> None:
        """
        Hook for subclasses to validate key constraints.
        """
        return None

    def _update_model_schema(self, schema: ModelSchema) -> None:
        raise NotImplementedError

    @classmethod
    def _get_schema(cls) -> IndexSchema:
        """
        Returns the schema for this index
        """
        schema: IndexSchema = {
            'index_name': cls.Meta.index_name,
            'key_schema': [],
            'projection': {
                PROJECTION_TYPE: cls.Meta.projection.projection_type,
            },
            'attribute_definitions': [],
        }

        cls._validate_key_attributes()

        hash_key_attributes = cls._hash_key_attributes()
        range_key_attributes = cls._range_key_attributes()

        for attr_cls in hash_key_attributes:
            schema['attribute_definitions'].append({
                ATTR_NAME: attr_cls.attr_name,
                ATTR_TYPE: attr_cls.attr_type,
            })
            schema['key_schema'].append({
                ATTR_NAME: attr_cls.attr_name,
                KEY_TYPE: HASH,
            })
        for attr_cls in range_key_attributes:
            schema['attribute_definitions'].append({
                ATTR_NAME: attr_cls.attr_name,
                ATTR_TYPE: attr_cls.attr_type,
            })
            schema['key_schema'].append({
                ATTR_NAME: attr_cls.attr_name,
                KEY_TYPE: RANGE,
            })
        if cls.Meta.projection.non_key_attributes:
            schema['projection'][NON_KEY_ATTRIBUTES] = cls.Meta.projection.non_key_attributes
        return schema


class GlobalSecondaryIndex(Index[_M]):
    """
    A global secondary index
    """
    @classmethod
    def _validate_key_attributes(cls) -> None:
        hash_keys = cls._hash_key_attributes()
        range_keys = cls._range_key_attributes()
        if len(hash_keys) == 0:
            raise ValueError(f"{cls.__name__} must have at least one hash key attribute")
        if len(hash_keys) > 4:
            raise ValueError(f"{cls.__name__} supports at most 4 hash key attributes")
        if len(range_keys) > 4:
            raise ValueError(f"{cls.__name__} supports at most 4 range key attributes")

    @classmethod
    def _update_model_schema(cls, schema: ModelSchema) -> None:
        index_schema: GlobalSecondaryIndexSchema = {
            **cls._get_schema(),  # type:ignore[misc]  # https://github.com/python/mypy/pull/13353
            'provisioned_throughput': {},
        }

        if hasattr(cls.Meta, 'read_capacity_units'):
            index_schema['provisioned_throughput'][READ_CAPACITY_UNITS] = cls.Meta.read_capacity_units
        if hasattr(cls.Meta, 'write_capacity_units'):
            index_schema['provisioned_throughput'][WRITE_CAPACITY_UNITS] = cls.Meta.write_capacity_units

        schema['global_secondary_indexes'].append(index_schema)
        # With polymorphism, indexes can use the same attribute, e.g. index1 on (thread_id, created_at)
        # and index2 on (thread_id, updated_at). We need to deduplicate.
        for attr_def in index_schema['attribute_definitions']:
            if attr_def not in schema['attribute_definitions']:
                schema['attribute_definitions'].append(attr_def)


class LocalSecondaryIndex(Index[_M]):
    """
    A local secondary index
    """
    @classmethod
    def _validate_key_attributes(cls) -> None:
        hash_keys = cls._hash_key_attributes()
        range_keys = cls._range_key_attributes()
        if len(hash_keys) > 1:
            raise ValueError(f"{cls.__name__} supports at most one hash key attribute")
        if len(range_keys) > 1:
            raise ValueError(f"{cls.__name__} supports at most one range key attribute")

    @classmethod
    def _update_model_schema(cls, schema: ModelSchema) -> None:
        index_schema = cls._get_schema()
        schema['local_secondary_indexes'].append(index_schema)
        # With polymorphism, indexes can use the same attribute, e.g. index1 on (thread_id, created_at)
        # and index2 on (thread_id, updated_at). We need to deduplicate.
        for attr_def in index_schema['attribute_definitions']:
            if attr_def not in schema['attribute_definitions']:
                schema['attribute_definitions'].append(attr_def)



class Projection:
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
