"""
Tests for the base connection class
"""
import base64
import json
from unittest import mock, TestCase
from unittest.mock import patch

import botocore.exceptions
from botocore.awsrequest import AWSPreparedRequest, AWSRequest, AWSResponse
from botocore.client import ClientError
from botocore.exceptions import BotoCoreError

import pytest

from pynamodb.connection import Connection
from pynamodb.connection.base import MetaTable
from pynamodb.exceptions import (
    TableError, DeleteError, PutError, ScanError, GetError, UpdateError, TableDoesNotExist)
from pynamodb.constants import (
    DEFAULT_REGION, UNPROCESSED_ITEMS, STRING_SHORT, BINARY_SHORT, DEFAULT_ENCODING, TABLE_KEY,
    PROVISIONED_BILLING_MODE, PAY_PER_REQUEST_BILLING_MODE)
from pynamodb.expressions.operand import Path, Value
from pynamodb.expressions.update import SetAction
from .data import DESCRIBE_TABLE_DATA, GET_ITEM_DATA, LIST_TABLE_DATA
from .deep_eq import deep_eq

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

    def test_get_attribute_type(self):
        assert self.meta_table.get_attribute_type('ForumName') == 'S'
        with pytest.raises(ValueError):
            self.meta_table.get_attribute_type('wrongone')

    def test_has_index_name(self):
        self.assertTrue(self.meta_table.has_index_name("LastPostIndex"))
        self.assertFalse(self.meta_table.has_index_name("NonExistentIndexName"))


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
        self.assertEqual(repr(conn), "Connection<{}>".format(conn.host))

    def test_subsequent_client_is_not_cached_when_credentials_none(self):
        with patch('pynamodb.connection.Connection.session') as session_mock:
            session_mock.create_client.return_value._request_signer._credentials = None
            conn = Connection()

            # make two calls to .client property, expect two calls to create client
            self.assertIsNotNone(conn.client)
            self.assertIsNotNone(conn.client)

            session_mock.create_client.assert_has_calls(
                [
                    mock.call('dynamodb', 'us-east-1', endpoint_url=None, config=mock.ANY),
                    mock.call('dynamodb', 'us-east-1', endpoint_url=None, config=mock.ANY),
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

            self.assertEqual(
                session_mock.create_client.mock_calls.count(mock.call('dynamodb', 'us-east-1', endpoint_url=None, config=mock.ANY)),
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

        kwargs['billing_mode'] = PAY_PER_REQUEST_BILLING_MODE
        params['BillingMode'] = PAY_PER_REQUEST_BILLING_MODE
        del params['ProvisionedThroughput']
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

    @pytest.mark.filterwarnings("ignore:Legacy conditional")
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

    @pytest.mark.filterwarnings("ignore")
    def test_update_item(self):
        """
        Connection.update_item
        """
        conn = Connection()
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        self.assertRaises(ValueError, conn.update_item, self.test_table_name, 'foo-key')

        self.assertRaises(ValueError, conn.update_item, self.test_table_name, 'foo', actions=[])

        attr_updates = {
            'Subject': {
                'Value': 'foo-subject',
                'Action': 'PUT'
            },
        }

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
            req.side_effect = BotoCoreError
            self.assertRaises(
                UpdateError,
                conn.update_item,
                self.test_table_name,
                'foo-key',
                range_key='foo-range-key',
                actions=[SetAction(Path('bar'), Value('foobar'))],
            )

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

    def test_transact_write_items(self):
        conn = Connection()
        with patch(PATCH_METHOD) as req:
            conn.transact_write_items([], [], [], [])
            self.assertEqual(req.call_args[0][0], 'TransactWriteItems')
            self.assertDictEqual(
                req.call_args[0][1], {
                    'TransactItems': [],
                    'ReturnConsumedCapacity': 'TOTAL'
                }
            )

    def test_transact_get_items(self):
        conn = Connection()
        with patch(PATCH_METHOD) as req:
            conn.transact_get_items([])
            self.assertEqual(req.call_args[0][0], 'TransactGetItems')
            self.assertDictEqual(
                req.call_args[0][1], {
                    'TransactItems': [],
                    'ReturnConsumedCapacity': 'TOTAL'
                }
            )

    def test_batch_write_item(self):
        """
        Connection.batch_write_item
        """
        items = []
        conn = Connection()
        table_name = 'Thread'
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{}".format(i)}
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
                {"ForumName": "FooForum", "Subject": "thread-{}".format(i)}
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

        with pytest.raises(ValueError, match="Table Thread has no index: NonExistentIndexName"):
            conn.query(table_name, "FooForum", limit=1, index_name='NonExistentIndexName')

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

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(
                ScanError,
                conn.scan,
                table_name)

    @mock.patch('pynamodb.connection.Connection.client')
    def test_make_api_call_throws_verbose_error_after_backoff(self, client_mock):
        response = AWSResponse(
            url='http://lyft.com',
            status_code=500,
            raw='',  # todo: use stream, like `botocore.tests.RawResponse`?
            headers={'x-amzn-RequestId': 'abcdef'},
        )
        response._content = json.dumps({'message': 'There is a problem', '__type': 'InternalServerError'}).encode('utf-8')
        client_mock._endpoint.http_session.send.return_value = response

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
    @mock.patch('pynamodb.connection.Connection.client')
    def test_make_api_call_throws_verbose_error_after_backoff_later_succeeds(self, client_mock, rand_int_mock):
        # mock response
        bad_response = mock.Mock(spec=AWSResponse)
        bad_response.status_code = 500
        bad_response.headers = {'x-amzn-RequestId': 'abcdef'}
        bad_response.text = json.dumps({'message': 'There is a problem', '__type': 'InternalServerError'})
        bad_response.content = bad_response.text.encode()

        good_response_content = {'TableDescription': {'TableName': 'table', 'TableStatus': 'Creating'}}
        good_response = mock.Mock(spec=AWSResponse)
        good_response.status_code = 200
        good_response.headers = {}
        good_response.text = json.dumps(good_response_content)
        good_response.content = good_response.text.encode()

        send_mock = client_mock._endpoint.http_session.send
        send_mock.side_effect = [
            bad_response,
            bad_response,
            good_response,
        ]

        rand_int_mock.return_value = 1

        c = Connection()

        self.assertEqual(good_response_content, c._make_api_call('CreateTable', {'TableName': 'MyTable'}))
        self.assertEqual(len(send_mock.mock_calls), 3)

        assert rand_int_mock.call_args_list == [mock.call(0, 25), mock.call(0, 50)]

    @mock.patch('pynamodb.connection.Connection.client')
    def test_make_api_call_retries_properly(self, client_mock):
        deserializable_response = AWSResponse(
            url='',
            status_code=200,
            headers={},
            raw='',
        )
        deserializable_response._content = json.dumps({'hello': 'world'}).encode('utf-8')

        bad_response = AWSResponse(
            url='',
            status_code=503,
            headers={},
            raw='',
        )
        bad_response._content = 'not_json'.encode('utf-8')

        prepared_request = AWSRequest('GET', 'http://lyft.com').prepare()

        send_mock = client_mock._endpoint.http_session.send
        send_mock.side_effect = [
            bad_response,
            botocore.exceptions.ReadTimeoutError(endpoint_url='http://lyft.com'),
            bad_response,
            deserializable_response,
        ]
        c = Connection()
        c._max_retry_attempts_exception = 3
        c._create_prepared_request = mock.Mock()
        c._create_prepared_request.return_value = prepared_request

        c._make_api_call('DescribeTable', {'TableName': 'MyTable'})
        self.assertEqual(len(send_mock.mock_calls), 4)

        for call in send_mock.mock_calls:
            self.assertEqual(call[1][0], prepared_request)

    def test_connection__timeout(self):
        c = Connection(connect_timeout_seconds=5, read_timeout_seconds=10, max_pool_connections=20)
        assert c.client._client_config.connect_timeout == 5
        assert c.client._client_config.read_timeout == 10
        assert c.client._client_config.max_pool_connections == 20

    def test_sign_request(self):
        request = AWSRequest(method='POST', url='http://localhost:8000/', headers={}, data={'foo': 'bar'})
        c = Connection(region='us-west-1')
        c._sign_request(request)
        assert 'X-Amz-Date' in request.headers
        assert 'Authorization' in request.headers
        assert 'us-west-1' in request.headers['Authorization']
        assert request.headers['Authorization'].startswith('AWS4-HMAC-SHA256')

    @mock.patch('pynamodb.connection.Connection.client')
    def test_make_api_call___extra_headers(self, client_mock):
        good_response = mock.Mock(spec=AWSResponse, status_code=200, headers={}, text='{}', content=b'{}')

        send_mock = client_mock._endpoint.http_session.send
        send_mock.return_value = good_response

        client_mock._convert_to_request_dict.return_value = {'method': 'POST', 'url': '', 'headers': {}, 'body': '', 'context': {}}

        mock_req = mock.Mock(spec=AWSPreparedRequest, headers={})
        create_request_mock = client_mock._endpoint.prepare_request
        create_request_mock.return_value = mock_req

        c = Connection(extra_headers={'foo': 'bar'})
        c._make_api_call('DescribeTable', {'TableName': 'MyTable'})

        assert send_mock.call_count == 1
        assert send_mock.call_args[0][0].headers.get('foo') == 'bar'

    @mock.patch('pynamodb.connection.Connection.client')
    def test_make_api_call_throws_when_retries_exhausted(self, client_mock):
        prepared_request = AWSRequest('GET', 'http://lyft.com').prepare()

        send_mock = client_mock._endpoint.http_session.send
        send_mock.side_effect = [
            botocore.exceptions.ConnectionError(error="problems"),
            botocore.exceptions.ConnectionError(error="problems"),
            botocore.exceptions.ConnectionError(error="problems"),
            botocore.exceptions.ReadTimeoutError(endpoint_url="http://lyft.com"),
        ]
        c = Connection()
        c._max_retry_attempts_exception = 3
        c._create_prepared_request = mock.Mock()
        c._create_prepared_request.return_value = prepared_request

        with self.assertRaises(botocore.exceptions.ReadTimeoutError):
            c._make_api_call('DescribeTable', {'TableName': 'MyTable'})

        self.assertEqual(len(send_mock.mock_calls), 4)
        for call in send_mock.mock_calls:
            self.assertEqual(call[1][0], prepared_request)

    @mock.patch('random.randint')
    @mock.patch('pynamodb.connection.Connection.client')
    def test_make_api_call_throws_retry_disabled(self, client_mock, rand_int_mock):
        prepared_request = AWSRequest('GET', 'http://lyft.com').prepare()

        send_mock = client_mock._endpoint.http_session.send
        send_mock.side_effect = [
            botocore.exceptions.ReadTimeoutError(endpoint_url='http://lyft.com'),
        ]
        c = Connection(read_timeout_seconds=11, base_backoff_ms=3, max_retry_attempts=0)
        c._create_prepared_request = mock.Mock()
        c._create_prepared_request.return_value = prepared_request

        assert c._base_backoff_ms == 3
        with self.assertRaises(botocore.exceptions.ReadTimeoutError):
            c._make_api_call('DescribeTable', {'TableName': 'MyTable'})

        self.assertEqual(len(send_mock.mock_calls), 1)
        rand_int_mock.assert_not_called()

        for call in send_mock.mock_calls:
            self.assertEqual(call[1][0], prepared_request)

    def test_handle_binary_attributes_for_unprocessed_items(self):
        binary_blob = b'\x00\xFF\x00\xFF'

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
        binary_blob = b'\x00\xFF\x00\xFF'
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

    def test_update_time_to_live_fail(self):
        conn = Connection(self.region)
        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            self.assertRaises(TableError, conn.update_time_to_live, 'test table', 'my_ttl')
