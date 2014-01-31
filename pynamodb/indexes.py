"""
PynamoDB Indexes
"""
from pynamodb.constants import (
    INCLUDE, ALL, KEYS_ONLY, ATTR_NAME, ATTR_TYPE, KEY_TYPE, ATTR_TYPE_MAP, KEY_SCHEMA,
    ATTR_DEFINITIONS, GLOBAL_SECONDARY_INDEX, LOCAL_SECONDARY_INDEX
)
from pynamodb.attributes import Attribute
from pynamodb.types import HASH, RANGE
from pynamodb.connection.util import pythonic


class Index(object):
    """
    Base class for secondary indexes
    """
    projection = None
    attributes = None
    read_capacity_units = None
    write_capacity_units = None
    index_type = None
    model = None
    index_name = None

    def __init__(self):
        if not self.projection:
            raise ValueError("No projection defined, define a projection for this class")

    @classmethod
    def query(cls, *args, **kwargs):
        """
        Queries an index
        """
        pass

    @classmethod
    def hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        for attr_cls in cls.get_attributes().values():
            if attr_cls.is_hash_key:
                return attr_cls

    @classmethod
    def schema(cls):
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
        if cls.attributes is None:
            cls.attributes = {}
            for item in dir(cls):
                item_cls = getattr(cls, item).__class__
                if issubclass(item_cls, (Attribute, )):
                    cls.attributes[item] = getattr(cls, item)
        return cls.attributes


class GlobalSecondaryIndex(Index):
    """
    A global secondary index
    """
    index_type = GLOBAL_SECONDARY_INDEX

    def query(self,
              hash_key,
              scan_index_forward=None,
              consistent_read=False,
              **filters):
        """
        Queries an index
        """
        return self.model.query(
            hash_key,
            index_name=self.index_name,
            scan_index_forward=scan_index_forward,
            consistent_read=consistent_read,
            **filters
        )


class LocalSecondaryIndex(Index):
    """
    A local secondary index
    """
    index_type = LOCAL_SECONDARY_INDEX

    @classmethod
    def query(cls,
              hash_key,
              scan_index_forward=None,
              consistent_read=False,
              **filters):
        """
        Queries an index
        """
        return cls.model.query(
            hash_key,
            index_name=cls.index_name,
            scan_index_forward=scan_index_forward,
            consistent_read=consistent_read,
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
