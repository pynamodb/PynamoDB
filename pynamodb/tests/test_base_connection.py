"""
Tests for the base connection class
"""
from pynamodb.connection.base import Connection
from pynamodb.connection.constants import DEFAULT_REGION
from unittest import TestCase
from unittest.mock import patch, MagicMock


class ConnectionTestCase(TestCase):
    """
    Tests for the base connection class
    """
    def setUp(self):
        self.test_table_name = 'ci-table'
        self.region = DEFAULT_REGION

    def test_create_connection(self):
        """
        Construct a connection
        """
        conn = Connection(self.test_table_name)
        self.assertIsNotNone(conn)

    def test_create_table(self):
        """
        Create tables
        """
        conn = Connection(self.region)
        kwargs = {
            'read_capacity_units': 1,
            'write_capacity_units': 1,

        }
        self.assertRaises(ValueError, conn.create_table, self.test_table_name, **kwargs)
        kwargs['attribute_definitions'] = [
            {
                'attribute_name': 'key1',
                'attribute_type': 'S'
            },
            {
                'attribute_name': 'key2',
                'attribute_type': 'S'
            }
        ]
        self.assertRaises(ValueError, conn.create_table, self.test_table_name, **kwargs)
        kwargs['key_schema'] = [
            {
                'attribute_name': 'key1',
                'key_type': 'hash'
            },
            {
                'attribute_name': 'key2',
                'key_type': 'range'
            }
        ]

        conn._endpoint = MagicMock()
        conn.create_table(
            self.test_table_name,
            **kwargs
        )

