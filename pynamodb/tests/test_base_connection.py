"""
Tests for the base connection class
"""
import base64
import json
import six
from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.connection import Connection
from pynamodb.connection.base import MetaTable
from botocore.vendored import requests
from pynamodb.exceptions import (VerboseClientError,
    TableError, DeleteError, UpdateError, PutError, GetError, ScanError, QueryError, TableDoesNotExist)
from pynamodb.constants import (
    DEFAULT_REGION, UNPROCESSED_ITEMS, STRING_SHORT, BINARY_SHORT, DEFAULT_ENCODING, TABLE_KEY)
from pynamodb.expressions.operand import Path
from pynamodb.tests.data import DESCRIBE_TABLE_DATA, GET_ITEM_DATA, LIST_TABLE_DATA
from pynamodb.tests.deep_eq import deep_eq
from botocore.exceptions import BotoCoreError
from botocore.client import ClientError
if six.PY3:
    from unittest.mock import patch
    from unittest import mock
else:
    from mock import patch
    import mock

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'


class MetaTableTestCase(TestCase):
    """
    Tests for the meta table class
    """

    def setUp(self):
        self.meta_table = MetaTable(DESCRIBE_TABLE_DATA.get(TABLE_KEY))

    def test_get_key_names(self):
        key_names = self.meta_table.get_key_names()
        self.assertEqual(key_names, ["ForumName", "Subject"])

    def test_get_key_names_index(self):
        key_names = self.meta_table.get_key_names("LastPostIndex")
        self.assertEqual(key_names, ["ForumName", "Subject", "LastPostDateTime"])


class ConnectionTestCase(TestCase):
    """
    Tests for the base connection class
    """

    def setUp(self):
        self.test_table_name = 'ci-table'
        self.region = DEFAULT_REGION

    def test_create_connection(self):
        """
        Connection()
        """
        conn = Connection()
        self.assertIsNotNone(conn)
        conn = Connection(host='http://foohost')
        self.assertIsNotNone(conn.client)
        self.assertIsNotNone(conn)
        self.assertEqual(repr(conn), "Connection<{0}>".format(conn.host))

    def test_subsequent_client_is_not_cached_when_credentials_none(self):
        with patch('pynamodb.connection.Connection.session') as session_mock:
            session_mock.create_client.return_value._request_signer._credentials = None
            conn = Connection()

            # make two calls to .client property, expect two calls to create client
            self.assertIsNotNone(conn.client)
            self.assertIsNotNone(conn.client)

            session_mock.create_client.assert_has_calls(
                [
                    mock.call('dynamodb', 'us-east-1', endpoint_url=None),
                    mock.call('dynamodb', 'us-east-1', endpoint_url=None),
                ],
                any_order=True
            )

    def test_subsequent_client_is_cached_when_credentials_truthy(self):
        with patch('pynamodb.connection.Connection.session') as session_mock:
            session_mock.create_client.return_value._request_signer._credentials = True
            conn = Connection()

            # make two calls to .client property, expect one call to create client
            self.assertIsNotNone(conn.client)
            self.assertIsNotNone(conn.client)

            self.assertEquals(
                session_mock.create_client.mock_calls.count(mock.call('dynamodb', 'us-east-1', endpoint_url=None)),
                1
            )

    def test_create_table(self):
        """
        Connection.create_table
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
            'TableName': 'ci-table',
            'ProvisionedThroughput': {
                'WriteCapacityUnits': 1,
                'ReadCapacityUnits': 1
            },
            'AttributeDefinitions': [
                {
                    'AttributeType': 'S',
                    'AttributeName': 'key1'
                },
                {
                    'AttributeType': 'S',
                    'AttributeName': 'key2'
                }
            ],
            'KeySchema': [
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
            req.side_effect = BotoCoreError
            self.assertRaises(TableError, conn.create_table, self.test_table_name, **kwargs)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn.create_table(
                self.test_table_name,
                **kwargs
            )
            self.assertEqual(req.call_args[0][1], params)

        kwargs['global_secondary_indexes'] = [
            {
                'index_name': 'alt-index',
                'key_schema': [
                    {
                        'KeyType': 'HASH',
                        'AttributeName': 'AltKey'
                    }
                ],
                'projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
                'provisioned_throughput': {
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1,
                },
            }
        ]
        params['GlobalSecondaryIndexes'] = [{'IndexName': 'alt-index', 'Projection': {'ProjectionType': 'KEYS_ONLY'},
                                               'KeySchema': [{'AttributeName': 'AltKey', 'KeyType': 'HASH'}],
                                               'ProvisionedThroughput': {'ReadCapacityUnits': 1,
                                                                         'WriteCapacityUnits': 1}}]
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn.create_table(
                self.test_table_name,
                **kwargs
            )
            # Ensure that the hash key is first when creating indexes
            self.assertEqual(req.call_args[0][1]['GlobalSecondaryIndexes'][0]['KeySchema'][0]['KeyType'], 'HASH')
            self.assertEqual(req.call_args[0][1], params)
        del(kwargs['global_secondary_indexes'])
        del(params['GlobalSecondaryIndexes'])

        kwargs['local_secondary_indexes'] = [
            {
                'index_name': 'alt-index',
                'projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
                'key_schema': [
                    {
                        'AttributeName': 'AltKey', 'KeyType': 'HASH'
                    }
                ],
                'provisioned_throughput': {
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1
                }
            }
        ]
        params['LocalSecondaryIndexes'] = [
            {
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
                'KeySchema': [
                    {
                        'KeyType': 'HASH',
                        'AttributeName': 'AltKey'
                    }
                ],
                'IndexName': 'alt-index'
            }
        ]
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn.create_table(
                self.test_table_name,
                **kwargs
            )
            self.assertEqual(req.call_args[0][1], params)

        kwargs['stream_specification'] = {
                'stream_enabled': True,
                'stream_view_type': 'NEW_IMAGE'
        }
        params['StreamSpecification'] = {
                'StreamEnabled': True,
                'StreamViewType': 'NEW_IMAGE'
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn.create_table(
                self.test_table_name,
                **kwargs
            )
            self.assertEqual(req.call_args[0][1], params)

    def test_delete_table(self):
        """
        Connection.delete_table
        """
        params = {'TableName': 'ci-table'}
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn = Connection(self.region)
            conn.delete_table(self.test_table_name)
            kwargs = req.call_args[0][1]
            self.assertEqual(kwargs, params)

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            conn = Connection(self.region)
            self.assertRaises(TableError, conn.delete_table, self.test_table_name)

    def test_update_table(self):
        """
        Connection.update_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn = Connection(self.region)
            params = {
                'ProvisionedThroughput': {
                    'WriteCapacityUnits': 2,
                    'ReadCapacityUnits': 2
                },
                'TableName': 'ci-table'
            }
            conn.update_table(
                self.test_table_name,
                read_capacity_units=2,
                write_capacity_units=2
            )
            self.assertEqual(req.call_args[0][1], params)

        self.assertRaises(ValueError, conn.update_table, self.test_table_name, read_capacity_units=2)

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            conn = Connection(self.region)
            self.assertRaises(
                TableError,
                conn.update_table,
                self.test_table_name,
                read_capacity_units=2,
                write_capacity_units=2)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn = Connection(self.region)

            global_secondary_index_updates = [
                {
                    "index_name": "foo-index",
                    "read_capacity_units": 2,
                    "write_capacity_units": 2
                }
            ]
            params = {
                'TableName': 'ci-table',
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 2,
                },
                'GlobalSecondaryIndexUpdates': [
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
            self.assertEqual(req.call_args[0][1], params)

    def test_describe_table(self):
        """
        Connection.describe_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn = Connection(self.region)
            conn.describe_table(self.test_table_name)
            self.assertEqual(req.call_args[0][1], {'TableName': 'ci-table'})

        with self.assertRaises(TableDoesNotExist):
            with patch(PATCH_METHOD) as req:
                req.side_effect = ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}}, "DescribeTable")
                conn = Connection(self.region)
                conn.describe_table(self.test_table_name)

        with self.assertRaises(TableDoesNotExist):
            with patch(PATCH_METHOD) as req:
                req.side_effect = ValueError()
                conn = Connection(self.region)
                conn.describe_table(self.test_table_name)

    def test_list_tables(self):
        """
        Connection.list_tables
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables(exclusive_start_table_name='Thread')
            self.assertEqual(req.call_args[0][1], {'ExclusiveStartTableName': 'Thread'})

        with patch(PATCH_METHOD) as req:
            req.return_value = LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables(limit=3)
            self.assertEqual(req.call_args[0][1], {'Limit': 3})

        with patch(PATCH_METHOD) as req:
            req.return_value = LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables()
            self.assertEqual(req.call_args[0][1], {})

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            conn = Connection(self.region)
            self.assertRaises(TableError, conn.list_tables)

    def test_delete_item(self):
        """
        Connection.delete_item
        """
        conn = Connection(self.region)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(DeleteError, conn.delete_item, self.test_table_name, "foo", "bar")

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?")
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'TableName': self.test_table_name}
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                return_values='ALL_NEW'
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'TableName': self.test_table_name,
                'ReturnValues': 'ALL_NEW'
            }
            self.assertEqual(req.call_args[0][1], params)

        self.assertRaises(
            ValueError,
            conn.delete_item,
            self.test_table_name,
            "foo",
            "bar",
            return_values='bad_values')

        self.assertRaises(
            ValueError,
            conn.delete_item,
            self.test_table_name,
            "foo",
            "bar",
            return_consumed_capacity='badvalue')

        self.assertRaises(
            ValueError,
            conn.delete_item,
            self.test_table_name,
            "foo",
            "bar",
            return_item_collection_metrics='badvalue')

        self.assertRaises(
            ValueError,
            conn.delete_item,
            self.test_table_name,
            "foo",
            "bar",
            conditional_operator='notone')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                return_consumed_capacity='TOTAL'
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'TableName': self.test_table_name,
                'ReturnConsumedCapacity': 'TOTAL'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                return_item_collection_metrics='SIZE'
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'TableName': self.test_table_name,
                'ReturnItemCollectionMetrics': 'SIZE',
                'ReturnConsumedCapacity': 'TOTAL'
            }
            self.assertEqual(req.call_args[0][1], params)

        self.assertRaises(
            ValueError,
            conn.delete_item,
            self.test_table_name,
            "Foo", "Bar",
            expected={'Bad': {'Value': False}}
        )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                condition=Path('ForumName').does_not_exist(),
                return_item_collection_metrics='SIZE'
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'TableName': self.test_table_name,
                'ReturnConsumedCapacity': 'TOTAL',
                'ReturnItemCollectionMetrics': 'SIZE'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                expected={'ForumName': {'Exists': False}},
                return_item_collection_metrics='SIZE'
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'TableName': self.test_table_name,
                'ReturnConsumedCapacity': 'TOTAL',
                'ReturnItemCollectionMetrics': 'SIZE'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                conditional_operator='and',
                expected={'ForumName': {'Exists': False}},
                return_item_collection_metrics='SIZE'
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'TableName': self.test_table_name,
                'ReturnConsumedCapacity': 'TOTAL',
                'ReturnItemCollectionMetrics': 'SIZE'
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_get_item(self):
        """
        Connection.get_item
        """
        conn = Connection(self.region)
        table_name = 'Thread'
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = GET_ITEM_DATA
            item = conn.get_item(table_name, "Amazon DynamoDB", "How do I update multiple items?")
            self.assertEqual(item, GET_ITEM_DATA)

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                GetError,
                conn.get_item,
                table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?"
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = GET_ITEM_DATA
            conn.get_item(
                table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                attributes_to_get=['ForumName']
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'ProjectionExpression': '#0',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'ConsistentRead': False,
                'TableName': 'Thread'
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_update_item(self):
        """
        Connection.update_item
        """
        conn = Connection()
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        self.assertRaises(ValueError, conn.update_item, self.test_table_name, 'foo-key')

        self.assertRaises(ValueError, conn.update_item, self.test_table_name, 'foo', actions=[], attribute_updates={})

        attr_updates = {
            'Subject': {
                'Value': 'foo-subject',
                'Action': 'PUT'
            },
        }

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                UpdateError,
                conn.update_item,
                self.test_table_name,
                'foo-key',
                attribute_updates=attr_updates,
                range_key='foo-range-key',
            )

        with patch(PATCH_METHOD) as req:
            bad_attr_updates = {
                'Subject': {
                    'Value': 'foo-subject',
                    'Action': 'BADACTION'
                },
            }
            req.return_value = {}
            self.assertRaises(
                ValueError,
                conn.update_item,
                self.test_table_name,
                'foo-key',
                attribute_updates=bad_attr_updates,
                range_key='foo-range-key',
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                return_consumed_capacity='TOTAL',
                return_item_collection_metrics='NONE',
                return_values='ALL_NEW',
                actions=[Path('Subject').set('foo-subject')],
                condition=Path('Forum').does_not_exist(),
                range_key='foo-range-key',
            )
            params = {
                'ReturnValues': 'ALL_NEW',
                'ReturnItemCollectionMetrics': 'NONE',
                'ReturnConsumedCapacity': 'TOTAL',
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'ConditionExpression': 'attribute_not_exists (#0)',
                'UpdateExpression': 'SET #1 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'Forum',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo-subject'
                    }
                },
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                return_consumed_capacity='TOTAL',
                return_item_collection_metrics='NONE',
                return_values='ALL_NEW',
                condition=Path('Forum').does_not_exist(),
                attribute_updates=attr_updates,
                range_key='foo-range-key',
            )
            params = {
                'ReturnValues': 'ALL_NEW',
                'ReturnItemCollectionMetrics': 'NONE',
                'ReturnConsumedCapacity': 'TOTAL',
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'ConditionExpression': 'attribute_not_exists (#0)',
                'UpdateExpression': 'SET #1 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'Forum',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo-subject'
                    }
                },
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                return_consumed_capacity='TOTAL',
                return_item_collection_metrics='NONE',
                return_values='ALL_NEW',
                expected={'Forum': {'Exists': False}},
                attribute_updates=attr_updates,
                range_key='foo-range-key',
            )
            params = {
                'ReturnValues': 'ALL_NEW',
                'ReturnItemCollectionMetrics': 'NONE',
                'ReturnConsumedCapacity': 'TOTAL',
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'ConditionExpression': 'attribute_not_exists (#1)',
                'UpdateExpression': 'SET #0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'Subject',
                    '#1': 'Forum'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo-subject'
                    }
                },
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                attribute_updates=attr_updates,
                range_key='foo-range-key',
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'UpdateExpression': 'SET #0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo-subject'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

        attr_updates = {
            'Subject': {
                'Value': {'S': 'foo-subject'},
                'Action': 'PUT'
            },
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                attribute_updates=attr_updates,
                range_key='foo-range-key',
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'UpdateExpression': 'SET #0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo-subject'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

        attr_updates = {
            'Subject': {
                'Value': {'S': 'Foo'},
                'Action': 'PUT'
            },
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                attribute_updates=attr_updates,
                range_key='foo-range-key',
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'UpdateExpression': 'SET #0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            # Invalid conditional operator
            with self.assertRaises(ValueError):
                conn.update_item(
                    self.test_table_name,
                    'foo-key',
                    attribute_updates={
                        'Subject': {
                            'Value': {'N': '1'},
                            'Action': 'ADD'
                        },
                    },
                    conditional_operator='foobar',
                    range_key='foo-range-key',
                )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            # attributes are missing
            with self.assertRaises(ValueError):
                conn.update_item(
                    self.test_table_name,
                    'foo-key',
                    range_key='foo-range-key',
                )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                actions=[Path('Subject').set('Bar')],
                condition=(Path('ForumName').does_not_exist() & (Path('Subject') == 'Foo')),
                range_key='foo-range-key',
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'ConditionExpression': '(attribute_not_exists (#0) AND #1 = :0)',
                'UpdateExpression': 'SET #1 = :1',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    },
                    ':1': {
                        'S': 'Bar'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                attribute_updates={
                    'Subject': {
                        'Value': {'S': 'Bar'},
                        'Action': 'PUT'
                    },
                },
                condition=(Path('ForumName').does_not_exist() & (Path('Subject') == 'Foo')),
                range_key='foo-range-key',
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'ConditionExpression': '(attribute_not_exists (#0) AND #1 = :0)',
                'UpdateExpression': 'SET #1 = :1',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    },
                    ':1': {
                        'S': 'Bar'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.update_item(
                self.test_table_name,
                'foo-key',
                attribute_updates={
                    'Subject': {
                        'Value': {'S': 'Bar'},
                        'Action': 'PUT'
                    },
                },
                expected={
                    'ForumName': {'Exists': False},
                    'Subject': {
                        'ComparisonOperator': 'NE',
                        'Value': 'Foo'
                    }
                },
                conditional_operator='and',
                range_key='foo-range-key',
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'ConditionExpression': '(attribute_not_exists (#1) AND #0 = :1)',
                'UpdateExpression': 'SET #0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'Subject',
                    '#1': 'ForumName'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Bar'
                    },
                    ':1': {
                        'S': 'Foo'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'ci-table'
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_put_item(self):
        """
        Connection.put_item
        """
        conn = Connection(self.region)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                TableError,
                conn.put_item,
                'foo-key',
                self.test_table_name,
                return_values='ALL_NEW',
                attributes={'ForumName': 'foo-value'}
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                self.test_table_name,
                'foo-key',
                range_key='foo-range-key',
                return_consumed_capacity='TOTAL',
                return_item_collection_metrics='SIZE',
                return_values='ALL_NEW',
                attributes={'ForumName': 'foo-value'}
            )
            params = {
                'ReturnValues': 'ALL_NEW',
                'ReturnConsumedCapacity': 'TOTAL',
                'ReturnItemCollectionMetrics': 'SIZE',
                'TableName': self.test_table_name,
                'Item': {
                    'ForumName': {
                        'S': 'foo-value'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                PutError,
                conn.put_item,
                self.test_table_name,
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                self.test_table_name,
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {'TableName': self.test_table_name,
                      'ReturnConsumedCapacity': 'TOTAL',
                      'Item': {'ForumName': {'S': 'foo-value'}, 'Subject': {'S': 'foo-range-key'}}}
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                self.test_table_name,
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'Item': {
                    'ForumName': {
                        'S': 'foo-value'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'TableName': self.test_table_name
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                self.test_table_name,
                'item1-hash',
                range_key='item1-range',
                attributes={'foo': {'S': 'bar'}},
                condition=(Path('Forum').does_not_exist() & (Path('Subject') == 'Foo'))
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': self.test_table_name,
                'ConditionExpression': '(attribute_not_exists (#0) AND #1 = :0)',
                'ExpressionAttributeNames': {
                    '#0': 'Forum',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    }
                },
                'Item': {
                    'ForumName': {
                        'S': 'item1-hash'
                    },
                    'foo': {
                        'S': 'bar'
                    },
                    'Subject': {
                        'S': 'item1-range'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                self.test_table_name,
                'item1-hash',
                range_key='item1-range',
                attributes={'foo': {'S': 'bar'}},
                expected={
                    'Forum': {'Exists': False},
                    'Subject': {
                        'ComparisonOperator': 'NE',
                        'Value': 'Foo'
                    }
                }
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': self.test_table_name,
                'ConditionExpression': '(attribute_not_exists (#0) AND #1 = :0)',
                'ExpressionAttributeNames': {
                    '#0': 'Forum',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    }
                },
                'Item': {
                    'ForumName': {
                        'S': 'item1-hash'
                    },
                    'foo': {
                        'S': 'bar'
                    },
                    'Subject': {
                        'S': 'item1-range'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                self.test_table_name,
                'item1-hash',
                range_key='item1-range',
                attributes={'foo': {'S': 'bar'}},
                condition=(Path('ForumName') == 'item1-hash')
            )
            params = {
                'TableName': self.test_table_name,
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'item1-hash'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'Item': {
                    'ForumName': {
                        'S': 'item1-hash'
                    },
                    'foo': {
                        'S': 'bar'
                    },
                    'Subject': {
                        'S': 'item1-range'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                self.test_table_name,
                'item1-hash',
                range_key='item1-range',
                attributes={'foo': {'S': 'bar'}},
                expected={'ForumName': {'Value': 'item1-hash'}}
            )
            params = {
                'TableName': self.test_table_name,
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'item1-hash'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'Item': {
                    'ForumName': {
                        'S': 'item1-hash'
                    },
                    'foo': {
                        'S': 'bar'
                    },
                    'Subject': {
                        'S': 'item1-range'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_batch_write_item(self):
        """
        Connection.batch_write_item
        """
        items = []
        conn = Connection()
        table_name = 'Thread'
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{0}".format(i)}
            )
        self.assertRaises(
            ValueError,
            conn.batch_write_item,
            table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_write_item(
                table_name,
                put_items=items,
                return_item_collection_metrics='SIZE',
                return_consumed_capacity='TOTAL'
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'ReturnItemCollectionMetrics': 'SIZE',
                'RequestItems': {
                    'Thread': [
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-0'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-1'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-2'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-3'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-4'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-5'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-6'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-7'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-8'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-9'}}}}
                    ]
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_write_item(
                table_name,
                put_items=items
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'Thread': [
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-0'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-1'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-2'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-3'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-4'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-5'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-6'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-7'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-8'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-9'}}}}
                    ]
                }
            }
            self.assertEqual(req.call_args[0][1], params)
        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                PutError,
                conn.batch_write_item,
                table_name,
                delete_items=items
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_write_item(
                table_name,
                delete_items=items
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'Thread': [
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-0'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-1'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-2'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-3'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-4'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-5'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-6'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-7'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-8'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-9'}}}}
                    ]
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_write_item(
                table_name,
                delete_items=items,
                return_consumed_capacity='TOTAL',
                return_item_collection_metrics='SIZE'
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'ReturnItemCollectionMetrics': 'SIZE',
                'RequestItems': {
                    'Thread': [
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-0'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-1'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-2'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-3'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-4'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-5'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-6'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-7'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-8'}}}},
                        {'DeleteRequest': {'Key': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-9'}}}}
                    ]
                }
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_batch_get_item(self):
        """
        Connection.batch_get_item
        """
        items = []
        conn = Connection()
        table_name = 'Thread'
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{0}".format(i)}
            )
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                GetError,
                conn.batch_get_item,
                table_name,
                items,
                consistent_read=True,
                return_consumed_capacity='TOTAL',
                attributes_to_get=['ForumName']
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_get_item(
                table_name,
                items,
                consistent_read=True,
                return_consumed_capacity='TOTAL',
                attributes_to_get=['ForumName']
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'Thread': {
                        'ConsistentRead': True,
                        'ProjectionExpression': '#0',
                        'ExpressionAttributeNames': {
                            '#0': 'ForumName'
                        },
                        'Keys': [
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-0'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-1'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-2'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-3'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-4'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-5'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-6'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-7'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-8'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-9'}}
                        ]
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_get_item(
                table_name,
                items
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'Thread': {
                        'Keys': [
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-0'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-1'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-2'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-3'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-4'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-5'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-6'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-7'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-8'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-9'}}
                        ]
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_query(self):
        """
        Connection.query
        """
        conn = Connection()
        table_name = 'Thread'
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            conditional_operator='NOT_A_VALID_ONE',
            return_consumed_capacity='TOTAL',
            key_conditions={'Subject': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
        )

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            return_consumed_capacity='TOTAL',
            key_conditions={'Subject': {'ComparisonOperator': 'BAD_OPERATOR', 'AttributeValueList': ['thread']}}
        )

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            return_consumed_capacity='TOTAL',
            select='BAD_VALUE',
            key_conditions={'Subject': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
        )

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            Path('NotRangeKey').startswith('thread'),
            Path('Foo') == 'Bar',
            return_consumed_capacity='TOTAL'
        )

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            Path('Subject') != 'thread',  # invalid sort key condition
            return_consumed_capacity='TOTAL'
        )

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            Path('Subject').startswith('thread'),
            Path('ForumName') == 'FooForum',  # filter containing hash key
            return_consumed_capacity='TOTAL'
        )

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            Path('Subject').startswith('thread'),
            Path('Subject').startswith('thread'),  # filter containing range key
            return_consumed_capacity='TOTAL'
        )

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                QueryError,
                conn.query,
                table_name,
                "FooForum",
                scan_index_forward=True,
                return_consumed_capacity='TOTAL',
                select='ALL_ATTRIBUTES',
                key_conditions={'Subject': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                table_name,
                "FooForum",
                Path('Subject').startswith('thread'),
                scan_index_forward=True,
                return_consumed_capacity='TOTAL',
                select='ALL_ATTRIBUTES'
            )
            params = {
                'ScanIndexForward': True,
                'Select': 'ALL_ATTRIBUTES',
                'ReturnConsumedCapacity': 'TOTAL',
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'FooForum'
                    },
                    ':1': {
                        'S': 'thread'
                    }
                },
                'TableName': 'Thread'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                table_name,
                "FooForum",
                scan_index_forward=True,
                return_consumed_capacity='TOTAL',
                select='ALL_ATTRIBUTES',
                key_conditions={'Subject': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
            )
            params = {
                'ScanIndexForward': True,
                'Select': 'ALL_ATTRIBUTES',
                'ReturnConsumedCapacity': 'TOTAL',
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'FooForum'
                    },
                    ':1': {
                        'S': 'thread'
                    }
                },
                'TableName': 'Thread'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                table_name,
                "FooForum",
                Path('Subject').startswith('thread')
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'FooForum'
                    },
                    ':1': {
                        'S': 'thread'
                    }
                },
                'TableName': 'Thread'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                table_name,
                "FooForum",
                key_conditions={'Subject': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'FooForum'
                    },
                    ':1': {
                        'S': 'thread'
                    }
                },
                'TableName': 'Thread'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                table_name,
                "FooForum",
                limit=1,
                index_name='LastPostIndex',
                attributes_to_get=['ForumName'],
                exclusive_start_key="FooForum",
                consistent_read=True
            )
            params = {
                'Limit': 1,
                'ReturnConsumedCapacity': 'TOTAL',
                'ConsistentRead': True,
                'ExclusiveStartKey': {
                    'ForumName': {
                        'S': 'FooForum'
                    }
                },
                'IndexName': 'LastPostIndex',
                'ProjectionExpression': '#0',
                'KeyConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'FooForum'
                    }
                },
                'TableName': 'Thread'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                table_name,
                "FooForum",
                select='ALL_ATTRIBUTES',
                exclusive_start_key="FooForum"
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'ExclusiveStartKey': {
                    'ForumName': {
                        'S': 'FooForum'
                    }
                },
                'KeyConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'FooForum'
                    }
                },
                'TableName': 'Thread',
                'Select': 'ALL_ATTRIBUTES'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                table_name,
                "FooForum",
                select='ALL_ATTRIBUTES',
                conditional_operator='AND',
                exclusive_start_key="FooForum"
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'ExclusiveStartKey': {
                    'ForumName': {
                        'S': 'FooForum'
                    }
                },
                'KeyConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'FooForum'
                    }
                },
                'TableName': 'Thread',
                'Select': 'ALL_ATTRIBUTES'
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_rate_limited_scan(self):
        """
        Connection.rate_limited_scan
        """
        conn = Connection()
        table_name = 'Thread'
        SCAN_METHOD_TO_PATCH = 'pynamodb.connection.Connection.scan'

        def verify_scan_call_args(call_args,
                                  table_name,
                                  exclusive_start_key=None,
                                  total_segments=None,
                                  attributes_to_get=None,
                                  conditional_operator=None,
                                  limit=10,
                                  return_consumed_capacity='TOTAL',
                                  scan_filter=None,
                                  segment=None):
            self.assertEqual(call_args[0][0], table_name)
            self.assertEqual(call_args[1]['exclusive_start_key'], exclusive_start_key)
            self.assertEqual(call_args[1]['total_segments'], total_segments)
            self.assertEqual(call_args[1]['attributes_to_get'], attributes_to_get)
            self.assertEqual(call_args[1]['conditional_operator'], conditional_operator)
            self.assertEqual(call_args[1]['limit'], limit)
            self.assertEqual(call_args[1]['return_consumed_capacity'], return_consumed_capacity)
            self.assertEqual(call_args[1]['scan_filter'], scan_filter)
            self.assertEqual(call_args[1]['segment'], segment)

        with patch(SCAN_METHOD_TO_PATCH) as req:
            req.return_value = {'Items': [], 'ConsumedCapacity': {'TableName': table_name, 'CapacityUnits': 10.0}}
            resp = conn.rate_limited_scan(
                table_name
            )
            values = list(resp)
            self.assertEqual(0, len(values))
            verify_scan_call_args(req.call_args, table_name)

        # Attempts to use rate limited scanning should fail with a ScanError if the DynamoDB implementation
        # does not return ConsumedCapacity (e.g. DynamoDB Local).
        with patch(SCAN_METHOD_TO_PATCH) as req:
            req.return_value = {'Items': []}
            self.assertRaises(ScanError, lambda: list(conn.rate_limited_scan(table_name)))

        # ... unless explicitly indicated that it's okay to proceed without rate limiting through an explicit parameter
        # (or through settings, which isn't tested here).
        with patch(SCAN_METHOD_TO_PATCH) as req:
            req.return_value = {'Items': []}
            list(conn.rate_limited_scan(table_name, allow_rate_limited_scan_without_consumed_capacity=True))

        with patch(SCAN_METHOD_TO_PATCH) as req:
            req.return_value = {'Items': [], 'ConsumedCapacity': {'TableName': table_name, 'CapacityUnits': 10.0}}
            resp = conn.rate_limited_scan(
                table_name,
                limit=10,
                segment=20,
                total_segments=22,
            )

            values = list(resp)
            self.assertEqual(0, len(values))
            verify_scan_call_args(req.call_args,
                                  table_name,
                                  segment=20,
                                  total_segments=22,
                                  limit=10)

        with patch(SCAN_METHOD_TO_PATCH) as req:
            req.return_value = {'Items': [], 'ConsumedCapacity': {'TableName': table_name, 'CapacityUnits': 10.0}}
            scan_filter = {
                'ForumName': {
                    'AttributeValueList': [
                        {'S': 'Foo'}
                    ],
                    'ComparisonOperator': 'BEGINS_WITH'
                },
                'Subject': {
                    'AttributeValueList': [
                        {'S': 'Foo'}
                    ],
                    'ComparisonOperator': 'CONTAINS'
                }
            }

            resp = conn.rate_limited_scan(
                table_name,
                exclusive_start_key='FooForum',
                page_size=1,
                segment=2,
                total_segments=4,
                scan_filter=scan_filter,
                conditional_operator='AND',
                attributes_to_get=['ForumName']
            )

            values = list(resp)
            self.assertEqual(0, len(values))
            verify_scan_call_args(req.call_args,
                                  table_name,
                                  exclusive_start_key='FooForum',
                                  limit=1,
                                  segment=2,
                                  total_segments=4,
                                  attributes_to_get=['ForumName'],
                                  scan_filter=scan_filter,
                                  conditional_operator='AND')

        with patch(SCAN_METHOD_TO_PATCH) as req:
            req.return_value = {'Items': [], 'ConsumedCapacity': {'TableName': table_name, 'CapacityUnits': 10.0}}
            resp = conn.rate_limited_scan(
                table_name,
                page_size=5,
                limit=10,
                read_capacity_to_consume_per_second=2
            )
            values = list(resp)
            self.assertEqual(0, len(values))
            verify_scan_call_args(req.call_args,
                                  table_name,
                                  limit=5)

        with patch(SCAN_METHOD_TO_PATCH) as req:
            req.return_value = {'Items': [], 'ConsumedCapacity': {'TableName': table_name, 'CapacityUnits': 10.0}}
            resp = conn.rate_limited_scan(
                table_name,
                limit=10,
                read_capacity_to_consume_per_second=4
            )
            values = list(resp)
            self.assertEqual(0, len(values))
            verify_scan_call_args(req.call_args,
                                  table_name,
                                  limit=4)

    def test_scan(self):
        """
        Connection.scan
        """
        conn = Connection()
        table_name = 'Thread'

        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
                segment=0,
                total_segments=22,
                consistent_read=True
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': table_name,
                'Segment': 0,
                'TotalSegments': 22,
                'ConsistentRead': True
            }
            self.assertDictEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
                segment=0,
                total_segments=22,
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': table_name,
                'Segment': 0,
                'TotalSegments': 22,
            }
            self.assertDictEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
                return_consumed_capacity='TOTAL',
                exclusive_start_key="FooForum",
                limit=1,
                segment=2,
                total_segments=4,
                attributes_to_get=['ForumName'],
                index_name='LastPostIndex',
            )
            params = {
                'ProjectionExpression': '#0',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'ExclusiveStartKey': {
                    "ForumName": {
                        "S": "FooForum"
                    }
                },
                'TableName': table_name,
                'Limit': 1,
                'Segment': 2,
                'TotalSegments': 4,
                'ReturnConsumedCapacity': 'TOTAL',
                'IndexName': 'LastPostIndex'
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': table_name
            }
            self.assertEqual(req.call_args[0][1], params)

        kwargs = {
            'scan_filter': {
                'ForumName': {
                    'ComparisonOperator': 'BadOperator',
                    'AttributeValueList': ['Foo']
                }
            }
        }
        self.assertRaises(
            ValueError,
            conn.scan,
            table_name,
            **kwargs)

        kwargs = {
            'scan_filter': {
                'ForumName': {
                    'ComparisonOperator': 'BEGINS_WITH',
                    'AttributeValueList': ['Foo']
                }
            }
        }
        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                ScanError,
                conn.scan,
                table_name,
                **kwargs)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
                **kwargs
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': table_name,
                'FilterExpression': 'begins_with (#0, :0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
                Path('ForumName').startswith('Foo')
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': table_name,
                'FilterExpression': 'begins_with (#0, :0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        kwargs = {
            'scan_filter': {
                'ForumName': {
                    'ComparisonOperator': 'BEGINS_WITH',
                    'AttributeValueList': ['Foo']
                },
                'Subject': {
                    'ComparisonOperator': 'CONTAINS',
                    'AttributeValueList': ['Foo']
                }
            },
            'conditional_operator': 'AND'
        }

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
                **kwargs
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': table_name,
                'FilterExpression': '(begins_with (#0, :0) AND contains (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    },
                    ':1': {
                        'S': 'Foo'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
                Path('ForumName').startswith('Foo') & Path('Subject').contains('Foo')
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': table_name,
                'FilterExpression': '(begins_with (#0, :0) AND contains (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'Foo'
                    },
                    ':1': {
                        'S': 'Foo'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        kwargs['conditional_operator'] = 'invalid'

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            self.assertRaises(
                ValueError,
                conn.scan,
                table_name,
                **kwargs)

    @mock.patch('time.time')
    @mock.patch('pynamodb.connection.Connection.scan')
    def test_ratelimited_scan_with_pagination_ends(self, scan_mock, time_mock):
        c = Connection()
        time_mock.side_effect = [1, 10, 20, 30, 40]
        scan_mock.side_effect = [
            {'Items': ['Item-1'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 1}, 'LastEvaluatedKey': 'XX' },
            {'Items': ['Item-2'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 1}}
        ]
        resp = c.rate_limited_scan('Table_1')
        values = list(resp)
        self.assertEqual(2, len(values))
        self.assertEqual(None, scan_mock.call_args_list[0][1]['exclusive_start_key'])
        self.assertEqual('XX', scan_mock.call_args_list[1][1]['exclusive_start_key'])

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch(PATCH_METHOD)
    def test_ratelimited_scan_retries_on_throttling(self, api_mock, sleep_mock, time_mock):
        c = Connection()
        time_mock.side_effect = [1, 2, 3, 4, 5]

        botocore_expected_format = {'Error': {'Message': 'm', 'Code': 'ProvisionedThroughputExceededException'}}

        api_mock.side_effect = [
            VerboseClientError(botocore_expected_format, 'operation_name', {}),
            {'Items': ['Item-1', 'Item-2'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 40}}
        ]
        resp = c.rate_limited_scan('Table_1')
        values = list(resp)
        self.assertEqual(2, len(values))
        self.assertEqual(1, len(sleep_mock.call_args_list))
        self.assertEqual(1.0, sleep_mock.call_args[0][0])

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch(PATCH_METHOD)
    def test_ratelimited_scan_exception_on_max_threshold(self, api_mock, sleep_mock, time_mock):
        c = Connection()
        time_mock.side_effect = [1, 2, 3, 4, 5]
        botocore_expected_format = {'Error': {'Message': 'm', 'Code': 'ProvisionedThroughputExceededException'}}

        api_mock.side_effect = VerboseClientError(botocore_expected_format, 'operation_name', {})

        with self.assertRaises(ScanError):
            resp = c.rate_limited_scan('Table_1', max_consecutive_exceptions=1)
            values = list(resp)
            self.assertEqual(0, len(values))
        self.assertEqual(1, len(sleep_mock.call_args_list))
        self.assertEqual(2, len(api_mock.call_args_list))

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch(PATCH_METHOD)
    def test_ratelimited_scan_raises_other_client_errors(self, api_mock, sleep_mock, time_mock):
        c = Connection()
        botocore_expected_format = {'Error': {'Message': 'm', 'Code': 'ConditionCheckFailedException'}}

        api_mock.side_effect = VerboseClientError(botocore_expected_format, 'operation_name', {})

        with self.assertRaises(ScanError):
            resp = c.rate_limited_scan('Table_1')
            values = list(resp)
            self.assertEqual(0, len(values))

        self.assertEqual(1, len(api_mock.call_args_list))
        self.assertEqual(0, len(sleep_mock.call_args_list))

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch(PATCH_METHOD)
    def test_ratelimited_scan_raises_non_client_error(self, api_mock, sleep_mock, time_mock):
        c = Connection()

        api_mock.side_effect = ScanError('error')

        with self.assertRaises(ScanError):
            resp = c.rate_limited_scan('Table_1')
            values = list(resp)
            self.assertEqual(0, len(values))

        self.assertEqual(1, len(api_mock.call_args_list))
        self.assertEqual(0, len(sleep_mock.call_args_list))

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch('pynamodb.connection.Connection.scan')
    def test_rate_limited_scan_retries_on_rate_unavailable(self, scan_mock, sleep_mock, time_mock):
        c = Connection()
        sleep_mock.return_value = 1
        time_mock.side_effect = [1, 4, 6, 12]
        scan_mock.side_effect = [
            {'Items': ['Item-1'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 80}, 'LastEvaluatedKey': 'XX' },
            {'Items': ['Item-2'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 41}}
        ]
        resp = c.rate_limited_scan('Table_1')
        values = list(resp)

        self.assertEqual(2, len(values))
        self.assertEqual(2, len(scan_mock.call_args_list))
        self.assertEqual(2, len(sleep_mock.call_args_list))
        self.assertEqual(3.0, sleep_mock.call_args_list[0][0][0])
        self.assertEqual(2.0, sleep_mock.call_args_list[1][0][0])

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch('pynamodb.connection.Connection.scan')
    def test_rate_limited_scan_retries_on_rate_unavailable_within_s(self, scan_mock, sleep_mock, time_mock):
        c = Connection()
        sleep_mock.return_value = 1
        time_mock.side_effect = [1.0, 1.5, 4.0]
        scan_mock.side_effect = [
            {'Items': ['Item-1'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 10}, 'LastEvaluatedKey': 'XX' },
            {'Items': ['Item-2'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 11}}
        ]
        resp = c.rate_limited_scan('Table_1', read_capacity_to_consume_per_second=5)
        values = list(resp)

        self.assertEqual(2, len(values))
        self.assertEqual(2, len(scan_mock.call_args_list))
        self.assertEqual(1, len(sleep_mock.call_args_list))
        self.assertEqual(2.0, sleep_mock.call_args_list[0][0][0])

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch('pynamodb.connection.Connection.scan')
    def test_rate_limited_scan_retries_max_sleep(self, scan_mock, sleep_mock, time_mock):
        c = Connection()
        sleep_mock.return_value = 1
        time_mock.side_effect = [1.0, 1.5, 250, 350]
        scan_mock.side_effect = [
            {'Items': ['Item-1'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 1000}, 'LastEvaluatedKey': 'XX' },
            {'Items': ['Item-2'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 11}}
        ]
        resp = c.rate_limited_scan(
            'Table_1',
            read_capacity_to_consume_per_second=5,
            max_sleep_between_retry=8
        )
        values = list(resp)

        self.assertEqual(2, len(values))
        self.assertEqual(2, len(scan_mock.call_args_list))
        self.assertEqual(1, len(sleep_mock.call_args_list))
        self.assertEqual(8.0, sleep_mock.call_args_list[0][0][0])

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch('pynamodb.connection.Connection.scan')
    def test_rate_limited_scan_retries_min_sleep(self, scan_mock, sleep_mock, time_mock):
        c = Connection()
        sleep_mock.return_value = 1
        time_mock.side_effect = [1, 2, 3, 4]
        scan_mock.side_effect = [
            {'Items': ['Item-1'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 10}, 'LastEvaluatedKey': 'XX' },
            {'Items': ['Item-2'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 11}}
        ]
        resp = c.rate_limited_scan('Table_1', read_capacity_to_consume_per_second=8)
        values = list(resp)

        self.assertEqual(2, len(values))
        self.assertEqual(2, len(scan_mock.call_args_list))
        self.assertEqual(1, len(sleep_mock.call_args_list))
        self.assertEqual(1.0, sleep_mock.call_args_list[0][0][0])

    @mock.patch('time.time')
    @mock.patch('time.sleep')
    @mock.patch('pynamodb.connection.Connection.scan')
    def test_rate_limited_scan_retries_timeout(self, scan_mock, sleep_mock, time_mock):
        c = Connection()
        sleep_mock.return_value = 1
        time_mock.side_effect = [1, 20, 30, 40]
        scan_mock.side_effect = [
            {'Items': ['Item-1'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 1000}, 'LastEvaluatedKey': 'XX' },
            {'Items': ['Item-2'], 'ConsumedCapacity': {'TableName': 'Table_1', 'CapacityUnits': 11}}
        ]
        resp = c.rate_limited_scan(
            'Table_1',
            read_capacity_to_consume_per_second=1,
            timeout_seconds=15
        )
        with self.assertRaises(ScanError):
            values = list(resp)
            self.assertEqual(0, len(values))

        self.assertEqual(0, len(sleep_mock.call_args_list))

    @mock.patch('pynamodb.connection.Connection.session')
    @mock.patch('pynamodb.connection.Connection.requests_session')
    def test_make_api_call_throws_verbose_error_after_backoff(self, requests_session_mock, session_mock):

        # mock response
        response = requests.Response()
        response.status_code = 500
        response._content = json.dumps({'message': 'There is a problem', '__type': 'InternalServerError'}).encode('utf-8')
        response.headers['x-amzn-RequestId'] = 'abcdef'
        requests_session_mock.send.return_value = response

        c = Connection()

        with self.assertRaises(ClientError):
            try:
                c._make_api_call('CreateTable', {'TableName': 'MyTable'})
            except Exception as e:
                self.assertEqual(
                    str(e),
                    'An error occurred (InternalServerError) on request (abcdef) on table (MyTable) when calling the CreateTable operation: There is a problem'
                )
                raise

    @mock.patch('random.randint')
    @mock.patch('pynamodb.connection.Connection.session')
    @mock.patch('pynamodb.connection.Connection.requests_session')
    def test_make_api_call_throws_verbose_error_after_backoff_later_succeeds(self, requests_session_mock, session_mock, rand_int_mock):

        # mock response
        bad_response = requests.Response()
        bad_response.status_code = 500
        bad_response._content = json.dumps({'message': 'There is a problem', '__type': 'InternalServerError'}).encode(
            'utf-8')
        bad_response.headers['x-amzn-RequestId'] = 'abcdef'

        good_response_content = {'TableDescription': {'TableName': 'table', 'TableStatus': 'Creating'}}
        good_response = requests.Response()
        good_response.status_code = 200
        good_response._content = json.dumps(good_response_content).encode('utf-8')

        requests_session_mock.send.side_effect = [
            bad_response,
            bad_response,
            good_response,
        ]

        rand_int_mock.return_value = 1

        c = Connection()

        self.assertEqual(good_response_content, c._make_api_call('CreateTable', {'TableName': 'MyTable'}))
        self.assertEqual(len(requests_session_mock.send.mock_calls), 3)

        assert rand_int_mock.call_args_list == [mock.call(0, 25), mock.call(0, 50)]

    @mock.patch('pynamodb.connection.Connection.session')
    @mock.patch('pynamodb.connection.Connection.requests_session')
    def test_create_prepared_request(self, requests_session_mock, session_mock):
        prepared_request = requests.Request('POST',
                                            'http://lyft.com',
                                            data='data',
                                            headers={'s': 's'}).prepare()
        mock_client = session_mock.create_client.return_value
        mock_client._endpoint.create_request.return_value = prepared_request

        c = Connection()
        c._max_retry_attempts_exception = 3
        c._create_prepared_request({'x': 'y'}, {'a': 'b'})

        self.assertEqual(len(requests_session_mock.mock_calls), 1)

        self.assertEqual(requests_session_mock.mock_calls[0][:2][0],
                         'prepare_request')

        called_request_object = requests_session_mock.mock_calls[0][:2][1][0]
        expected_request_object = requests.Request(prepared_request.method,
                                prepared_request.url,
                                data=prepared_request.body,
                                headers=prepared_request.headers)

        self.assertEqual(len(mock_client._endpoint.create_request.mock_calls), 1)
        self.assertEqual(mock_client._endpoint.create_request.mock_calls[0],
                         mock.call({'x': 'y'}, {'a': 'b'}))

        self.assertEqual(called_request_object.method, expected_request_object.method)
        self.assertEqual(called_request_object.url, expected_request_object.url)
        self.assertEqual(called_request_object.data, expected_request_object.data)
        self.assertEqual(called_request_object.headers, expected_request_object.headers)

    @mock.patch('pynamodb.connection.Connection.session')
    @mock.patch('pynamodb.connection.Connection.requests_session')
    def test_make_api_call_retries_properly(self, requests_session_mock, session_mock):
        # mock response
        deserializable_response = requests.Response()
        deserializable_response._content = json.dumps({'hello': 'world'}).encode('utf-8')
        deserializable_response.status_code = 200
        bad_response = requests.Response()
        bad_response._content = 'not_json'.encode('utf-8')
        bad_response.status_code = 503

        prepared_request = requests.Request('GET', 'http://lyft.com').prepare()

        requests_session_mock.send.side_effect = [
            bad_response,
            requests.Timeout('problems!'),
            bad_response,
            deserializable_response
        ]
        c = Connection()
        c._max_retry_attempts_exception = 3
        c._create_prepared_request = mock.Mock()
        c._create_prepared_request.return_value = prepared_request

        c._make_api_call('DescribeTable', {'TableName': 'MyTable'})
        self.assertEqual(len(requests_session_mock.mock_calls), 4)

        for call in requests_session_mock.mock_calls:
            self.assertEqual(call[:2], ('send', (prepared_request,)))

    @mock.patch('pynamodb.connection.Connection.session')
    @mock.patch('pynamodb.connection.Connection.requests_session')
    def test_make_api_call_throws_when_retries_exhausted(self, requests_session_mock, session_mock):
        prepared_request = requests.Request('GET', 'http://lyft.com').prepare()

        requests_session_mock.send.side_effect = [
            requests.ConnectionError('problems!'),
            requests.ConnectionError('problems!'),
            requests.ConnectionError('problems!'),
            requests.Timeout('problems!'),
        ]
        c = Connection()
        c._max_retry_attempts_exception = 3
        c._create_prepared_request = mock.Mock()
        c._create_prepared_request.return_value = prepared_request

        with self.assertRaises(requests.Timeout):
            c._make_api_call('DescribeTable', {'TableName': 'MyTable'})

        self.assertEqual(len(requests_session_mock.mock_calls), 4)
        assert requests_session_mock.send.call_args[1]['timeout'] == 60
        for call in requests_session_mock.mock_calls:
            self.assertEqual(call[:2], ('send', (prepared_request,)))

    @mock.patch('random.randint')
    @mock.patch('pynamodb.connection.Connection.session')
    @mock.patch('pynamodb.connection.Connection.requests_session')
    def test_make_api_call_throws_retry_disabled(self, requests_session_mock, session_mock, rand_int_mock):
        prepared_request = requests.Request('GET', 'http://lyft.com').prepare()

        requests_session_mock.send.side_effect = [
            requests.Timeout('problems!'),
        ]
        c = Connection(request_timeout_seconds=11, base_backoff_ms=3, max_retry_attempts=0)
        c._create_prepared_request = mock.Mock()
        c._create_prepared_request.return_value = prepared_request

        assert c._base_backoff_ms == 3
        with self.assertRaises(requests.Timeout):
            c._make_api_call('DescribeTable', {'TableName': 'MyTable'})

        self.assertEqual(len(requests_session_mock.mock_calls), 1)
        rand_int_mock.assert_not_called()

        assert requests_session_mock.send.call_args[1]['timeout'] == 11
        for call in requests_session_mock.mock_calls:
            self.assertEqual(call[:2], ('send', (prepared_request,)))

    def test_handle_binary_attributes_for_unprocessed_items(self):
        binary_blob = six.b('\x00\xFF\x00\xFF')

        unprocessed_items = []
        for idx in range(0, 5):
            unprocessed_items.append({
                'PutRequest': {
                    'Item': {
                        'name': {STRING_SHORT: 'daniel'},
                        'picture': {BINARY_SHORT: base64.b64encode(binary_blob).decode(DEFAULT_ENCODING)}
                    }
                }
            })

        expected_unprocessed_items = []
        for idx in range(0, 5):
            expected_unprocessed_items.append({
                'PutRequest': {
                    'Item': {
                        'name': {STRING_SHORT: 'daniel'},
                        'picture': {BINARY_SHORT: binary_blob}
                    }
                }
            })

        deep_eq(
            Connection._handle_binary_attributes({UNPROCESSED_ITEMS: {'someTable': unprocessed_items}}),
            {UNPROCESSED_ITEMS: {'someTable': expected_unprocessed_items}},
            _assert=True
        )

    def test_handle_binary_attributes_for_unprocessed_keys(self):
        binary_blob = six.b('\x00\xFF\x00\xFF')
        unprocessed_keys = {
            'UnprocessedKeys': {
                'MyTable': {
                    'AttributesToGet': ['ForumName'],
                    'Keys': [
                        {
                            'ForumName': {'S': 'FooForum'},
                            'Subject': {'B': base64.b64encode(binary_blob).decode(DEFAULT_ENCODING)}
                        },
                        {
                            'ForumName': {'S': 'FooForum'},
                            'Subject': {'S': 'thread-1'}
                        }
                    ],
                    'ConsistentRead': False
                },
                'MyOtherTable': {
                    'AttributesToGet': ['ForumName'],
                    'Keys': [
                        {
                            'ForumName': {'S': 'FooForum'},
                            'Subject': {'B': base64.b64encode(binary_blob).decode(DEFAULT_ENCODING)}
                        },
                        {
                            'ForumName': {'S': 'FooForum'},
                            'Subject': {'S': 'thread-1'}
                        }
                    ],
                    'ConsistentRead': False
                }
            }
        }
        data = Connection._handle_binary_attributes(unprocessed_keys)
        self.assertEqual(data['UnprocessedKeys']['MyTable']['Keys'][0]['Subject']['B'], binary_blob)
        self.assertEqual(data['UnprocessedKeys']['MyOtherTable']['Keys'][0]['Subject']['B'], binary_blob)

    def test_get_expected_map(self):
        conn = Connection(self.region)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        expected = {'ForumName': {'Exists': True}}
        self.assertEqual(
            conn.get_expected_map(self.test_table_name, expected),
            {'Expected': {'ForumName': {'Exists': True}}}
        )

        expected = {'ForumName': {'Value': 'foo'}}
        self.assertEqual(
            conn.get_expected_map(self.test_table_name, expected),
            {'Expected': {'ForumName': {'Value': {'S': 'foo'}}}}
        )

        expected = {'ForumName': {'ComparisonOperator': 'Null'}}
        self.assertEqual(
            conn.get_expected_map(self.test_table_name, expected),
            {'Expected': {'ForumName': {'ComparisonOperator': 'Null', 'AttributeValueList': []}}}
        )

        expected = {'ForumName': {'ComparisonOperator': 'EQ', 'AttributeValueList': ['foo']}}
        self.assertEqual(
            conn.get_expected_map(self.test_table_name, expected),
            {'Expected': {'ForumName': {'ComparisonOperator': 'EQ', 'AttributeValueList': [{'S': 'foo'}]}}}
        )

    def test_get_query_filter_map(self):
        conn = Connection(self.region)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        query_filters = {'ForumName': {'ComparisonOperator': 'EQ', 'AttributeValueList': ['foo']}}
        self.assertEqual(
            conn.get_query_filter_map(self.test_table_name, query_filters),
            {'QueryFilter': {'ForumName': {'ComparisonOperator': 'EQ', 'AttributeValueList': [{'S': 'foo'}]}}}
        )
