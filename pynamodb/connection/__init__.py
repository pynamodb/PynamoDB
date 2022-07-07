"""
PynamoDB lowest level connection
"""

from pynamodb.connection.base import Connection
from pynamodb.connection.table import TableConnection


__all__ = [
    "Connection",
    "TableConnection",
]
