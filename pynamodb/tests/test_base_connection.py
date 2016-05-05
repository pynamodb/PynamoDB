"""
Tests for the base connection class
"""
import json
import six
from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.connection import Connection
from botocore.vendored import requests
from pynamodb.exceptions import (
    TableError, DeleteError, UpdateError, PutError, GetError, ScanError, QueryError, TableDoesNotExist)
from pynamodb.constants import DEFAULT_REGION
from pynamodb.tests.data import DESCRIBE_TABLE_DATA, GET_ITEM_DATA, LIST_TABLE_DATA
from botocore.exceptions import BotoCoreError
from botocore.client import ClientError

if six.PY3:
    from unittest.mock import patch
    from unittest import mock
else:
    from mock import patch
    import mock

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'


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
                'Expected': {
                    'ForumName': {
                        'Exists': False
                    }
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
                'Expected': {
                    'ForumName': {
                        'Exists': False
                    }
                },
                'TableName': self.test_table_name,
                'ConditionalOperator': 'AND',
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
                'AttributesToGet': ['ForumName'],
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
                'Expected': {
                    'Forum': {
                        'Exists': False
                    }
                },
                'AttributeUpdates': {
                    'Subject': {
                        'Value': {
                            'S': 'foo-subject'
                        },
                        'Action': 'PUT'
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
                'AttributeUpdates': {
                    'Subject': {
                        'Value': {
                            'S': 'foo-subject'
                        },
                        'Action': 'PUT'
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
                'AttributeUpdates': {
                    'Subject': {
                        'Value': {
                            'S': 'foo-subject'
                        },
                        'Action': 'PUT'
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
                'AttributeUpdates': {
                    'Subject': {
                        'Value': {
                            'S': 'Foo'
                        },
                        'Action': 'PUT'
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
                'AttributeUpdates': {
                    'Subject': {
                        'Value': {
                            'S': 'Bar'
                        },
                        'Action': 'PUT'
                    }
                },
                'Expected': {
                    'ForumName': {
                        'Exists': False
                    },
                    'Subject': {
                        'Value': {
                            'S': 'Foo'
                        }
                    }
                },
                'ConditionalOperator': 'AND',
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
                'Expected': {
                    'Forum': {
                        'Exists': False
                    },
                    'Subject': {
                        'Value': {'S': 'Foo'}
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
                expected={'ForumName': {'Value': 'item1-hash'}}
            )
            params = {
                'TableName': self.test_table_name,
                'Expected': {
                    'ForumName': {
                        'Value': {
                            'S': 'item1-hash'
                        }
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
                        'AttributesToGet': ['ForumName'],
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
            key_conditions={'ForumName': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
        )

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            return_consumed_capacity='TOTAL',
            key_conditions={'ForumName': {'ComparisonOperator': 'BAD_OPERATOR', 'AttributeValueList': ['thread']}}
        )

        self.assertRaises(
            ValueError,
            conn.query,
            table_name,
            "FooForum",
            return_consumed_capacity='TOTAL',
            select='BAD_VALUE',
            key_conditions={'ForumName': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
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
                key_conditions={'ForumName': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                table_name,
                "FooForum",
                scan_index_forward=True,
                return_consumed_capacity='TOTAL',
                select='ALL_ATTRIBUTES',
                key_conditions={'ForumName': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
            )
            params = {
                'ScanIndexForward': True,
                'Select': 'ALL_ATTRIBUTES',
                'ReturnConsumedCapacity': 'TOTAL',
                'KeyConditions': {
                    'ForumName': {
                        'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': [{
                            'S': 'thread'
                        }]
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
                key_conditions={'ForumName': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'KeyConditions': {
                    'ForumName': {
                        'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': [{
                            'S': 'thread'
                        }]
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
                'AttributesToGet': ['ForumName'],
                'KeyConditions': {
                    'ForumName': {
                        'ComparisonOperator': 'EQ', 'AttributeValueList': [{
                            'S': 'FooForum'
                        }]
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
                'KeyConditions': {
                    'ForumName': {
                        'ComparisonOperator': 'EQ', 'AttributeValueList': [{
                            'S': 'FooForum'
                        }]
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
                'KeyConditions': {
                    'ForumName': {
                        'ComparisonOperator': 'EQ', 'AttributeValueList': [{
                            'S': 'FooForum'
                        }]
                    }
                },
                'TableName': 'Thread',
                'Select': 'ALL_ATTRIBUTES',
                'ConditionalOperator': 'AND'
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
                attributes_to_get=['ForumName']
            )
            params = {
                'AttributesToGet': ['ForumName'],
                'ExclusiveStartKey': {
                    "ForumName": {
                        "S": "FooForum"
                    }
                },
                'TableName': table_name,
                'Limit': 1,
                'Segment': 2,
                'TotalSegments': 4,
                'ReturnConsumedCapacity': 'TOTAL'
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
                'ScanFilter': {
                    'ForumName': {
                        'AttributeValueList': [
                            {'S': 'Foo'}
                        ],
                        'ComparisonOperator': 'BEGINS_WITH'
                    }
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.scan(
                table_name,
                **kwargs
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': table_name,
                'ScanFilter': {
                    'ForumName': {
                        'AttributeValueList': [
                            {'S': 'Foo'}
                        ],
                        'ComparisonOperator': 'BEGINS_WITH'
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
                'ScanFilter': {
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
                },
                'ConditionalOperator': 'AND'
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

    @mock.patch('pynamodb.connection.Connection.session')
    @mock.patch('pynamodb.connection.Connection.requests_session')
    def test_make_api_call_throws_verbose_error(self, requests_session_mock, session_mock):

        # mock response
        response = requests.Response()
        response.status_code = 400
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
        session_mock.create_client.return_value._endpoint.create_request.return_value = prepared_request

        requests_session_mock.send.side_effect = [
            requests.ConnectionError('problems!'),
            requests.Timeout('problems!'),
            bad_response,
            deserializable_response
        ]
        c = Connection()
        c._max_retry_attempts_exception = 4

        c._make_api_call('DescribeTable', {'TableName': 'MyTable'})
        self.assertEqual(len(requests_session_mock.mock_calls), 4)
        for call in requests_session_mock.mock_calls:
            self.assertEqual(call[:2], ('send', (prepared_request,)))


    @mock.patch('pynamodb.connection.Connection.session')
    @mock.patch('pynamodb.connection.Connection.requests_session')
    def test_make_api_call_throws_when_retries_exhausted(self, requests_session_mock, session_mock):
        prepared_request = requests.Request('GET', 'http://lyft.com').prepare()
        session_mock.create_client.return_value._endpoint.create_request.return_value = prepared_request

        requests_session_mock.send.side_effect = [
            requests.ConnectionError('problems!'),
            requests.ConnectionError('problems!'),
            requests.ConnectionError('problems!'),
            requests.Timeout('problems!'),
        ]
        c = Connection()
        c._max_retry_attempts_exception = 4

        with self.assertRaises(requests.Timeout):
            c._make_api_call('DescribeTable', {'TableName': 'MyTable'})

        self.assertEqual(len(requests_session_mock.mock_calls), 4)
        for call in requests_session_mock.mock_calls:
            self.assertEqual(call[:2], ('send', (prepared_request,)))
