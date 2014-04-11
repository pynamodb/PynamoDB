"""
PynamoDB Indexes
"""
from pynamodb.constants import (
    INCLUDE, ALL, KEYS_ONLY, ATTR_NAME, ATTR_TYPE, KEY_TYPE, ATTR_TYPE_MAP, KEY_SCHEMA,
    ATTR_DEFINITIONS, META_CLASS_NAME
)
from pynamodb.attributes import Attribute
from pynamodb.types import HASH, RANGE
from pynamodb.connection.util import pythonic
from six import with_metaclass


class IndexMeta(type):
    """
    Index meta class

    This class is here to allow for an index `Meta` class
    that contains the index settings
    """
    def __init__(cls, name, bases, attrs):
        if META_CLASS_NAME in attrs:
            meta_cls = attrs.get(META_CLASS_NAME)
            if meta_cls is not None:
                meta_cls.attributes = None


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
    def query(cls, *args, **kwargs):
        """
        Queries an index
        """
        pass

    @classmethod
    def _hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        for attr_cls in cls.get_attributes().values():
            if attr_cls.is_hash_key:
                return attr_cls

    @classmethod
    def get_schema(cls):
        """
        Returns the schema for this index
        """
        attr_definitions = []
        schema = []
        for attr_name, attr_cls in cls.get_attributes().items():
            attr_definitions.append({
                pythonic(ATTR_NAME): attr_name,
                pythonic(ATTR_TYPE): ATTR_TYPE_MAP[attr_cls.attr_type]
            })
            if attr_cls.is_hash_key:
                schema.append({
                    ATTR_NAME: attr_name,
                    KEY_TYPE: HASH
                })
            elif attr_cls.is_range_key:
                schema.append({
                    ATTR_NAME: attr_name,
                    KEY_TYPE: RANGE
                })
        return {
            pythonic(KEY_SCHEMA): schema,
            pythonic(ATTR_DEFINITIONS): attr_definitions
        }

    @classmethod
    def get_attributes(cls):
        """
        Returns the list of attributes for this class
        """
        if cls.Meta.attributes is None:
            cls.Meta.attributes = {}
            for item in dir(cls):
                item_cls = getattr(getattr(cls, item), "__class__", None)
                if item_cls and issubclass(item_cls, (Attribute, )):
                    cls.Meta.attributes[item] = getattr(cls, item)
        return cls.Meta.attributes


class GlobalSecondaryIndex(Index):
    """
    A global secondary index
    """
    def query(self,
              hash_key,
              scan_index_forward=None,
              consistent_read=False,
              limit=None,
              **filters):
        """
        Queries an index
        """
        return self.Meta.model.query(
            hash_key,
            index_name=self.Meta.index_name,
            scan_index_forward=scan_index_forward,
            consistent_read=consistent_read,
            limit=limit,
            **filters
        )


class LocalSecondaryIndex(Index):
    """
    A local secondary index
    """
    @classmethod
    def query(cls,
              hash_key,
              scan_index_forward=None,
              consistent_read=False,
              limit=None,
              **filters):
        """
        Queries an index
        """
        return cls.Meta.model.query(
            hash_key,
            index_name=cls.Meta.index_name,
            scan_index_forward=scan_index_forward,
            consistent_read=consistent_read,
            limit=limit,
            **filters
        )


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
