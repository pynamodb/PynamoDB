"""
DynamoDB Models for PynamoDB
"""
import json
from delorean import Delorean
from datetime import datetime
from .connection.constants import UTC, DATETIME_FORMAT
from .connection.table import TableConnection


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

    @classmethod
    def get(cls, hash_key, range_key=None, consistent_read=False, attributes=None):
        """
        Returns a single object using the provided keys
        """
        return cls.get_connection().get_item(
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes=attributes)

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
