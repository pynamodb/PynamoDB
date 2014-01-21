"""
Tests for the base connection class
"""
from pynamodb.connection.base import Connection
from pynamodb.connection.constants import DEFAULT_REGION, HTTP_OK
from unittest import TestCase
from unittest.mock import patch
from .response import Response

PATCH_METHOD = 'botocore.operation.Operation.call'

LIST_TABLE_DATA = {
    "LastEvaluatedTableName": "Thread",
    "TableNames": ["Forum", "Reply", "Thread"]
}

DESCRIBE_TABLE_DATA = {
    "Table": {
        "AttributeDefinitions": [
            {
                "AttributeName": "ForumName",
                "AttributeType": "S"
            },
            {
                "AttributeName": "LastPostDateTime",
                "AttributeType": "S"
            },
            {
                "AttributeName": "Subject",
                "AttributeType": "S"
            }
        ],
        "CreationDateTime": 1.363729002358E9,
        "ItemCount": 0,
        "KeySchema": [
            {
                "AttributeName": "ForumName",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "Subject",
                "KeyType": "RANGE"
            }
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "LastPostIndex",
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "KeySchema": [
                    {
                        "AttributeName": "ForumName",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "LastPostDateTime",
                        "KeyType": "RANGE"
                    }
                ],
                "Projection": {
                    "ProjectionType": "KEYS_ONLY"
                }
            }
        ],
        "ProvisionedThroughput": {
            "NumberOfDecreasesToday": 0,
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        },
        "TableName": "Thread",
        "TableSizeBytes": 0,
        "TableStatus": "ACTIVE"
    }
}

GET_ITEM_DATA = {
    "ConsumedCapacity": {
        "CapacityUnits": 1,
        "TableName": "Thread"
    },
    "Item": {
        "Tags": {
            "SS": ["Update", "Multiple Items", "HelpMe"]
        },
        "LastPostDateTime": {
            "S": "201303190436"
        },
        "Message": {
            "S": "I want to update multiple items in a single API call. What's the best way to do that?"
        }
    }
}


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
        params = {
            'table_name': 'ci-table',
            'provisioned_throughput': {
                'WriteCapacityUnits': 1,
                'ReadCapacityUnits': 1
            },
            'attribute_definitions': [
                {
                    'AttributeType': 'S',
                    'AttributeName': 'key1'
                },
                {
                    'AttributeType': 'S',
                    'AttributeName': 'key2'
                }
            ],
            'key_schema': [
                {
                    'KeyType': 'HASH',
                    'AttributeName': 'key1'
                },
                {
                    'KeyType': 'RANGE',
                    'AttributeName': 'key2'
                }
            ]
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), None
            conn.create_table(
                self.test_table_name,
                **kwargs
            )
            kwargs = req.call_args[1]
            self.assertEqual(kwargs, params)

    def test_delete_table(self):
        """
        Test deleting a table
        """
        params = {'table_name': 'ci-table'}
        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), None
            conn = Connection(self.region)
            conn.delete_table(self.test_table_name)
            kwargs = req.call_args[1]
            self.assertEqual(kwargs, params)

    def test_update_table(self):
        """
        Test updating a table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), None
            conn = Connection(self.region)
            params = {
                'provisioned_throughput':
                    {
                        'WriteCapacityUnits': 2,
                        'ReadCapacityUnits': 2
                    },
                'table_name': 'ci-table'
            }
            conn.update_table(
                self.test_table_name,
                read_capacity_units=2,
                write_capacity_units=2
            )
            self.assertEqual(req.call_args[1], params)
        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), None
            conn = Connection(self.region)

            global_secondary_index_updates = [
                {
                    "index_name": "foo-index",
                    "read_capacity_units": 2,
                    "write_capacity_units": 2
                }
            ]
            params = {
                'table_name': 'ci-table',
                'provisioned_throughput': {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 2,
                },
                'global_secondary_index_updates': [
                    {
                        'Update': {
                            'IndexName': 'foo-index',
                            'ProvisionedThroughput': {
                                'ReadCapacityUnits': 2,
                                'WriteCapacityUnits': 2,
                            }
                        }
                    }

                ]
            }
            conn.update_table(
                self.test_table_name,
                read_capacity_units=2,
                write_capacity_units=2,
                global_secondary_index_updates=global_secondary_index_updates
            )
            self.assertEqual(req.call_args[1], params)

    def test_describe_table(self):
        """
        Tests describing a table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), DESCRIBE_TABLE_DATA
            conn = Connection(self.region)
            conn.describe_table(self.test_table_name)
            self.assertEqual(req.call_args[1], {'table_name': 'ci-table'})

    def test_list_tables(self):
        """
        Test listing tables
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables(exclusive_start_table_name='Thread')
            self.assertEqual(req.call_args[1], {'exclusive_start_table_name': 'Thread'})

        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables(limit=3)
            self.assertEqual(req.call_args[1], {'limit': 3})

        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables()
            self.assertEqual(req.call_args[1], {})

    def test_get_item(self):
        """
        Tests getting an item
        """
        conn = Connection(self.region)
        table_name = 'Thread'
        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), GET_ITEM_DATA
            item = conn.get_item(table_name, "Amazon DynamoDB", "How do I update multiple items?")
            self.assertEqual(item, GET_ITEM_DATA)

    def test_put_item(self):
        """
        Tests putting an item
        """
        conn = Connection(self.region)
        table_name = 'Thread'
        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), {}
            conn.put_item(
                table_name,
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {'table_name': 'Thread',
                      'item': {'ForumName': {'S': 'foo-value'}, 'Subject': {'S': 'foo-range-key'}}}
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = Response(HTTP_OK, None), {}
            conn.put_item(
                table_name,
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {'item': {'ForumName': {'S': 'foo-value'}, 'Subject': {'S': 'foo-range-key'}},
                      'table_name': 'Thread'}
            self.assertEqual(req.call_args[1], params)
