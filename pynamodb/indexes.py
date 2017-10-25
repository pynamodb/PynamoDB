"""
PynamoDB Indexes
"""
from pynamodb.constants import (
    INCLUDE, ALL, KEYS_ONLY, ATTR_NAME, ATTR_TYPE, KEY_TYPE, ATTR_TYPE_MAP, KEY_SCHEMA,
    ATTR_DEFINITIONS, META_CLASS_NAME
)
from pynamodb.attributes import Attribute
from pynamodb.types import HASH, RANGE
from pynamodb.compat import getmembers_issubclass
from pynamodb.connection.util import pythonic
from six import with_metaclass


class IndexMeta(type):
    """
    Index meta class

    This class is here to allow for an index `Meta` class
    that contains the index settings
    """
    def __init__(cls, name, bases, attrs):
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == META_CLASS_NAME:
                    meta_cls = attrs.get(META_CLASS_NAME)
                    if meta_cls is not None:
                        meta_cls.attributes = None
                elif issubclass(attr_obj.__class__, (Attribute, )):
                    if attr_obj.attr_name is None:
                        attr_obj.attr_name = attr_name


class Index(with_metaclass(IndexMeta)):
    """
    Base class for secondary indexes
    """
    Meta = None

    def __init__(self):
        if self.Meta is None:
            raise ValueError("Indexes require a Meta class for settings")
        if not hasattr(self.Meta, "projection"):
            raise ValueError("No projection defined, define a projection for this class")

    @classmethod
    def count(cls,
              hash_key,
              range_key_condition=None,
              filter_condition=None,
              consistent_read=False,
              **filters):
        """
        Count on an index
        """
        return cls.Meta.model.count(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=cls.Meta.index_name,
            consistent_read=consistent_read,
            **filters
        )

    @classmethod
    def query(self,
              hash_key,
              range_key_condition=None,
              filter_condition=None,
              scan_index_forward=None,
              consistent_read=False,
              limit=None,
              last_evaluated_key=None,
              attributes_to_get=None,
              **filters):
        """
        Queries an index
        """
        return self.Meta.model.query(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=self.Meta.index_name,
            scan_index_forward=scan_index_forward,
            consistent_read=consistent_read,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            **filters
        )

    @classmethod
    def scan(self,
             filter_condition=None,
             segment=None,
             total_segments=None,
             limit=None,
             conditional_operator=None,
             last_evaluated_key=None,
             page_size=None,
             consistent_read=None,
             **filters):
        """
        Scans an index
        """
        return self.Meta.model.scan(
            filter_condition=filter_condition,
            segment=segment,
            total_segments=total_segments,
            limit=limit,
            conditional_operator=conditional_operator,
            last_evaluated_key=last_evaluated_key,
            page_size=page_size,
            consistent_read=consistent_read,
            index_name=self.Meta.index_name,
            **filters
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
    def _get_schema(cls):
        """
        Returns the schema for this index
        """
        attr_definitions = []
        schema = []
        for attr_name, attr_cls in cls._get_attributes().items():
            attr_definitions.append({
                pythonic(ATTR_NAME): attr_cls.attr_name,
                pythonic(ATTR_TYPE): ATTR_TYPE_MAP[attr_cls.attr_type]
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
            pythonic(KEY_SCHEMA): schema,
            pythonic(ATTR_DEFINITIONS): attr_definitions
        }

    @classmethod
    def _get_attributes(cls):
        """
        Returns the list of attributes for this class
        """
        if cls.Meta.attributes is None:
            cls.Meta.attributes = {}
            for name, attribute in getmembers_issubclass(cls, Attribute):
                cls.Meta.attributes[name] = attribute
        return cls.Meta.attributes


class GlobalSecondaryIndex(Index):
    """
    A global secondary index
    """
    pass


class LocalSecondaryIndex(Index):
    """
    A local secondary index
    """
    pass


class Projection(object):
    """
    A class for presenting projections
    """
    projection_type = None
    non_key_attributes = None


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

    def __init__(self, non_attr_keys=None):
        if not non_attr_keys:
            raise ValueError("The INCLUDE type projection requires a list of string attribute names")
        self.non_key_attributes = non_attr_keys


class AllProjection(Projection):
    """
    An ALL projection
    """
    projection_type = ALL
