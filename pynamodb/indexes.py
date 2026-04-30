"""
PynamoDB Indexes
"""
from inspect import getmembers
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Mapping, Optional, Type, TypeVar, Union

from pynamodb._schema import IndexSchema, GlobalSecondaryIndexSchema
from pynamodb._schema import ModelSchema
from pynamodb.attributes import Attribute
from pynamodb.constants import (
    INCLUDE, ALL, KEYS_ONLY, ATTR_NAME, ATTR_TYPE, KEY_TYPE,
    PROJECTION_TYPE, NON_KEY_ATTRIBUTES,
    READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS,
)
from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator
from pynamodb.types import HASH, RANGE

if TYPE_CHECKING:
    from pynamodb.models import Model

_KeyType = Any
_HashKeysInputType = Mapping[str, _KeyType]
_SerializedHashKeyType = Union[_KeyType, Dict[str, _KeyType]]
_M = TypeVar('_M', bound='Model')


class Index(Generic[_M]):
    """
    Base class for secondary indexes
    """

    Meta: Any = None
    _model: _M

    @staticmethod
    def _get_attributes_in_declaration_order(
        index_cls: Type['Index'],
    ) -> Dict[str, Attribute]:
        """
        Returns attributes in declaration order, respecting overrides.
        """
        attributes: Dict[str, Attribute] = {}
        for base in reversed(index_cls.__mro__):
            for name, attribute in getattr(base, '__dict__', {}).items():
                if name in attributes:
                    del attributes[name]
                if isinstance(attribute, Attribute):
                    # If a subclass overrides an attribute, preserve the subclass declaration order.
                    attributes[name] = attribute
        return attributes

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.Meta is not None:
            cls.Meta.attributes = cls._get_attributes_in_declaration_order(cls)

    def __init__(self) -> None:
        if self.Meta is None:
            raise ValueError('Indexes require a Meta class for settings')
        if not hasattr(self.Meta, 'projection'):
            raise ValueError('No projection defined, define a projection for this class')

    def __set_name__(self, owner: Type[_M], name: str):
        if not hasattr(self.Meta, 'index_name'):
            self.Meta.index_name = name

    def count(
        self,
        hash_key: Optional[_KeyType] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
        hash_keys: Optional[_HashKeysInputType] = None,
    ) -> int:
        """
        Count on an index

        :param hash_key: The hash key to query. Can be None when ``hash_keys`` is provided.
        :param hash_keys: Named hash key values for indexes with multiple hash key attributes.
        """
        return self._model.count(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=self.Meta.index_name,
            consistent_read=consistent_read,
            limit=limit,
            rate_limit=rate_limit,
            hash_keys=hash_keys,
        )

    def query(
        self,
        hash_key: Optional[_KeyType] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        scan_index_forward: Optional[bool] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        attributes_to_get: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        rate_limit: Optional[float] = None,
        hash_keys: Optional[_HashKeysInputType] = None,
    ) -> ResultIterator[_M]:
        """
        Queries an index

        :param hash_key: The hash key to query. Can be None when ``hash_keys`` is provided.
        :param hash_keys: Named hash key values for indexes with multiple hash key attributes.
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
            hash_keys=hash_keys,
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
    def _hash_key_aliases(
        cls, hash_key_attributes: List[Attribute]
    ) -> Dict[str, Attribute]:
        aliases: Dict[str, Attribute] = {}
        hash_key_attribute_ids = {id(attr) for attr in hash_key_attributes}
        for attr_name, attr in cls.Meta.attributes.items():
            if id(attr) in hash_key_attribute_ids:
                aliases[attr_name] = attr
                aliases[attr.attr_name] = attr
        return aliases

    @staticmethod
    def _flatten_and_conditions(condition: Condition) -> List[Condition]:
        if condition.operator == 'AND':
            conditions: List[Condition] = []
            for value in condition.values:
                conditions.extend(Index._flatten_and_conditions(value))
            return conditions
        return [condition]

    @staticmethod
    def _condition_key_name(condition: Condition) -> Optional[str]:
        path = getattr(condition.values[0], 'path', None) if condition.values else None
        if not isinstance(path, list) or len(path) != 1:
            return None
        return path[0]

    @staticmethod
    def _combine_conditions(conditions: List[Condition]) -> Condition:
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition &= condition
        return combined_condition

    @staticmethod
    def _normalize_multi_key_condition(
        range_key_condition: Condition,
        range_keynames: List[str],
        context: str,
    ) -> Condition:
        valid_operators = {'=', '<', '<=', '>', '>=', 'BETWEEN', 'begins_with'}
        conditions_by_key: Dict[str, Condition] = {}
        for condition in Index._flatten_and_conditions(range_key_condition):
            if condition.operator not in valid_operators:
                raise ValueError(
                    f'{context} range_key_condition uses unsupported range key operator: {condition.operator}'
                )
            key_name = Index._condition_key_name(condition)
            if key_name is None or key_name not in range_keynames:
                raise ValueError(
                    f'{context} range_key_condition must only use range keys: ' + ', '.join(range_keynames)
                )
            if key_name in conditions_by_key:
                raise ValueError(
                    f'{context} range_key_condition has multiple conditions for range key: {key_name}'
                )
            conditions_by_key[key_name] = condition

        if not conditions_by_key:
            return range_key_condition

        highest_position = max(
            range_keynames.index(key_name) for key_name in conditions_by_key
        )
        missing_prefix_keys = [
            key_name
            for key_name in range_keynames[:highest_position]
            if key_name not in conditions_by_key
        ]
        if missing_prefix_keys:
            raise ValueError(
                f'{context} range_key_condition must include equality conditions for preceding range keys: '
                + ', '.join(missing_prefix_keys)
            )

        non_equal_prefix_keys = [
            key_name
            for key_name in range_keynames[:highest_position]
            if conditions_by_key[key_name].operator != '='
        ]
        if non_equal_prefix_keys:
            raise ValueError(
                f'{context} range_key_condition must use equality for preceding range keys: '
                + ', '.join(non_equal_prefix_keys)
            )

        ordered_conditions = [
            conditions_by_key[key_name]
            for key_name in range_keynames
            if key_name in conditions_by_key
        ]
        return Index._combine_conditions(ordered_conditions)

    @classmethod
    def _serialize_hash_key_values(
        cls,
        hash_key: Optional[_KeyType] = None,
        hash_keys: Optional[_HashKeysInputType] = None,
    ) -> _SerializedHashKeyType:
        hash_key_attributes = cls._hash_key_attributes()
        if not hash_key_attributes:
            raise ValueError(f'{cls.__name__} has no hash key attributes')

        if hash_key is not None and hash_keys is not None:
            raise ValueError(f'{cls.__name__} received both hash_key and hash_keys')

        if len(hash_key_attributes) == 1:
            if hash_keys is None:
                if hash_key is None:
                    raise ValueError(f'{cls.__name__} requires a hash_key')
                if isinstance(hash_key, (tuple, list)):
                    raise ValueError(f'{cls.__name__} expects a single hash_key value')
                if isinstance(hash_key, Mapping):
                    raise ValueError(
                        f'{cls.__name__} expects hash_keys=... for named hash key values'
                    )
                return hash_key_attributes[0].serialize(hash_key)

            hash_key_values = cls._get_ordered_hash_key_values(
                hash_keys, hash_key_attributes
            )
            return hash_key_attributes[0].serialize(hash_key_values[0])

        if hash_keys is None:
            if hash_key is None:
                raise ValueError(f'{cls.__name__} requires hash_keys')
            raise ValueError(
                f'{cls.__name__} has multiple hash key attributes; use hash_keys=...'
            )

        hash_key_values = cls._get_ordered_hash_key_values(
            hash_keys, hash_key_attributes
        )
        return {
            attr.attr_name: attr.serialize(value)
            for attr, value in zip(hash_key_attributes, hash_key_values)
        }

    @classmethod
    def serialize_hash_key_values(
        cls,
        hash_key: Optional[_KeyType] = None,
        hash_keys: Optional[_HashKeysInputType] = None,
    ) -> _SerializedHashKeyType:
        return cls._serialize_hash_key_values(hash_key, hash_keys=hash_keys)

    @classmethod
    def _get_ordered_hash_key_values(
        cls,
        hash_keys: _HashKeysInputType,
        hash_key_attributes: List[Attribute],
    ) -> List[_KeyType]:
        if not isinstance(hash_keys, Mapping):
            raise ValueError(f'{cls.__name__} expects hash_keys to be a mapping')

        expected_aliases = cls._hash_key_aliases(hash_key_attributes)

        values_by_attr_name: Dict[str, _KeyType] = {}
        unknown_keys = []
        for key, value in hash_keys.items():
            key_name = key
            attr = expected_aliases.get(key_name)
            if attr is None:
                unknown_keys.append(str(key_name))
                continue
            if attr.attr_name in values_by_attr_name:
                raise ValueError(
                    f'{cls.__name__} received duplicate value for hash key: {attr.attr_name}'
                )
            values_by_attr_name[attr.attr_name] = value

        if unknown_keys:
            raise ValueError(
                f'{cls.__name__} received unknown hash keys: ' + ', '.join(unknown_keys)
            )

        missing_keys = [
            attr.attr_name
            for attr in hash_key_attributes
            if attr.attr_name not in values_by_attr_name
        ]
        if missing_keys:
            raise ValueError(
                f'{cls.__name__} requires values for hash keys: ' + ', '.join(missing_keys)
            )

        return [values_by_attr_name[attr.attr_name] for attr in hash_key_attributes]

    @classmethod
    def _normalize_range_key_condition(
        cls, range_key_condition: Optional[Condition]
    ) -> Optional[Condition]:
        range_key_attributes = cls._range_key_attributes()
        if range_key_condition is None or len(range_key_attributes) <= 1:
            return range_key_condition
        return cls._normalize_multi_key_condition(
            range_key_condition,
            [attr.attr_name for attr in range_key_attributes],
            cls.__name__,
        )

    @classmethod
    def _validate_range_key_condition(
        cls, range_key_condition: Optional[Condition]
    ) -> None:
        cls._normalize_range_key_condition(range_key_condition)

    @classmethod
    def validate_range_key_condition(
        cls, range_key_condition: Optional[Condition]
    ) -> None:
        cls._validate_range_key_condition(range_key_condition)

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

        for attr_cls in range_key_attributes:
            schema['attribute_definitions'].append(
                {
                    ATTR_NAME: attr_cls.attr_name,
                    ATTR_TYPE: attr_cls.attr_type,
                }
            )
        for attr_cls in hash_key_attributes:
            schema['attribute_definitions'].append(
                {
                    ATTR_NAME: attr_cls.attr_name,
                    ATTR_TYPE: attr_cls.attr_type,
                }
            )
        for attr_cls in hash_key_attributes:
            schema['key_schema'].append(
                {
                    ATTR_NAME: attr_cls.attr_name,
                    KEY_TYPE: HASH,
                }
            )
        for attr_cls in range_key_attributes:
            schema['key_schema'].append(
                {
                    ATTR_NAME: attr_cls.attr_name,
                    KEY_TYPE: RANGE,
                }
            )
        if cls.Meta.projection.non_key_attributes:
            schema['projection'][NON_KEY_ATTRIBUTES] = (
                cls.Meta.projection.non_key_attributes
            )
        return schema


class GlobalSecondaryIndex(Index[_M]):
    """
    A global secondary index
    """

    @classmethod
    def _validate_key_attributes(cls) -> None:
        hash_keys = cls._hash_key_attributes()
        range_keys = cls._range_key_attributes()
        if len(hash_keys) > 4:
            raise ValueError(f'{cls.__name__} supports at most 4 hash key attributes')
        if len(range_keys) > 4:
            raise ValueError(f'{cls.__name__} supports at most 4 range key attributes')

    @classmethod
    def _update_model_schema(cls, schema: ModelSchema) -> None:
        index_schema: GlobalSecondaryIndexSchema = {
            **cls._get_schema(),  # type:ignore[misc]  # https://github.com/python/mypy/pull/13353
            'provisioned_throughput': {},
        }

        if hasattr(cls.Meta, 'read_capacity_units'):
            index_schema['provisioned_throughput'][READ_CAPACITY_UNITS] = (
                cls.Meta.read_capacity_units
            )
        if hasattr(cls.Meta, 'write_capacity_units'):
            index_schema['provisioned_throughput'][WRITE_CAPACITY_UNITS] = (
                cls.Meta.write_capacity_units
            )

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
            raise ValueError(f'{cls.__name__} supports at most one hash key attribute')
        if len(range_keys) > 1:
            raise ValueError(f'{cls.__name__} supports at most one range key attribute')

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
            raise ValueError(
                'The INCLUDE type projection requires a list of string attribute names'
            )
        self.non_key_attributes = non_attr_keys


class AllProjection(Projection):
    """
    An ALL projection
    """

    projection_type = ALL
