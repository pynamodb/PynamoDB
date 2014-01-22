"""
DynamoDB Models for PynamoDB
"""
import json
from delorean import Delorean
from datetime import datetime
from .connection.constants import UTC, DATETIME_FORMAT
from .connection.table import TableConnection
from .connection.util import pythonic
from .types import HASH, RANGE
from .connection.constants import (
    STRING, NUMBER, BINARY, ATTR_TYPE_MAP, ATTR_DEFINITIONS, ATTR_NAME, ATTR_TYPE,
    KEY_SCHEMA, KEY_TYPE)


class Attribute(object):
    """
    An attribute of a model
    """
    def __init__(self, attr_type=str, hash_key=False, range_key=False):
        self.attr_type = attr_type
        self.is_hash_key = hash_key
        self.is_range_key = range_key


class BinaryAttribute(Attribute):
    """
    A binary attribute
    """
    def __init__(self, hash_key=False, range_key=False):
        super(BinaryAttribute, self).__init__(
            hash_key=hash_key,
            range_key=range_key,
            attr_type=BINARY
        )

class UnicodeAttribute(Attribute):
    """
    A unicode attribute
    """
    def __init__(self, hash_key=False, range_key=False):
        super(UnicodeAttribute, self).__init__(
            hash_key=hash_key,
            range_key=range_key,
            attr_type=STRING
        )


class NumberAttribute(Attribute):
    """
    A number attribute
    """
    name = None
    def __init__(self, hash_key=False, range_key=False):
        super(NumberAttribute, self).__init__(
            hash_key=hash_key,
            range_key=range_key,
            attr_type=NUMBER
        )


class Model(object):
    """
    Defines a pynamodb model
    """
    table_name = None
    hash_key = None
    range_key = None
    attributes = None
    connection = None

    @classmethod
    def get_connection(cls):
        """
        Returns a (cached) connection
        """
        if cls.connection is None:
            cls.connection = TableConnection(cls.table_name)
        return cls.connection

    @classmethod
    def get(cls,
            hash_key,
            range_key=None,
            consistent_read=False):
        """
        Returns a single object using the provided keys
        """
        return cls.get_connection().get_item(
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read)

    def serialize(self, value):
        """
        Serializes a value for use with DynamoDB
        """
        if isinstance(value, list):
            return json.dumps(list, sort_keys=True)
        elif isinstance(value, dict):
            return json.dumps(dict, sort_keys=True)
        elif isinstance(value, datetime):
            fmt = Delorean(value, timezone=UTC).datetime.strftime(DATETIME_FORMAT)
            fmt = "{0}:{1}".format(fmt[:-2], fmt[-2:])
            return fmt
        elif isinstance(value, bool):
            return int(bool)
        else:
            return value

    @classmethod
    def schema(cls):
        """
        Returns the schema for this table
        """
        if cls.attributes is None:
            cls.attributes = {}
            for item in dir(cls):
                item_cls = getattr(cls, item).__class__
                if issubclass(item_cls, (Attribute, )):
                    cls.attributes[item] = getattr(cls, item)
        schema = {
            pythonic(ATTR_DEFINITIONS): [],
            pythonic(KEY_SCHEMA): []
        }
        for attr_name, attr_cls in cls.attributes.items():
            schema[pythonic(ATTR_DEFINITIONS)].append({
                pythonic(ATTR_NAME): attr_name,
                pythonic(ATTR_TYPE): ATTR_TYPE_MAP[attr_cls.attr_type]
            })
            if attr_cls.is_hash_key:
                schema[pythonic(KEY_SCHEMA)].append({
                    pythonic(KEY_TYPE): HASH,
                    pythonic(ATTR_NAME): attr_name
                })
            elif attr_cls.is_range_key:
                schema[pythonic(KEY_SCHEMA)].append({
                    pythonic(KEY_TYPE): RANGE,
                    pythonic(ATTR_NAME): attr_name
                })
        return schema

    @classmethod
    def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None):
        """
        Create the table for this model
        """
        schema = cls.schema()
        schema['read_capacity_units'] = read_capacity_units
        schema['write_capacity_units'] = write_capacity_units
        return cls.get_connection().create_table(
            **schema
        )