"""
Test suite for the table class
"""
import six
from unittest import TestCase

from pynamodb.connection import TableConnection
from pynamodb.constants import DEFAULT_REGION, PROVISIONED_BILLING_MODE
from pynamodb.expressions.operand import Path
from .data import DESCRIBE_TABLE_DATA, GET_ITEM_DATA
from .response import HttpOK

if six.PY3:
    from unittest.mock import patch
else:
    from mock import patch

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
        TableConnection()
        """
        conn = TableConnection(self.test_table_name)
        self.assertIsNotNone(conn)

    def test_connection_session_set_credentials(self):
        conn = TableConnection(
            self.test_table_name,
            aws_access_key_id='access_key_id',
            aws_secret_access_key='secret_access_key')

        credentials = conn.connection.session.get_credentials()

        self.assertEqual(credentials.access_key, 'access_key_id')
        self.assertEqual(credentials.secret_key, 'secret_access_key')

    def test_connection_session_set_credentials_with_session_token(self):
        conn = TableConnection(
            self.test_table_name,
            aws_access_key_id='access_key_id',
            aws_secret_access_key='secret_access_key',
            aws_session_token='session_token')

        credentials = conn.connection.session.get_credentials()

        self.assertEqual(credentials.access_key, 'access_key_id')
        self.assertEqual(credentials.secret_key, 'secret_access_key')
        self.assertEqual(credentials.token, 'session_token')

    def test_create_table(self):
        """
        TableConnection.create_table
        """
        conn = TableConnection(self.test_table_name)
        kwargs = {
            'read_capacity_units': 1,
            'write_capacity_units': 1,
        }
        self.assertRaises(ValueError, conn.create_table, **kwargs)
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
        self.assertRaises(ValueError, conn.create_table, **kwargs)
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
            'BillingMode': PROVISIONED_BILLING_MODE,
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
            req.return_value = {}
            conn.create_table(
                **kwargs
            )
            kwargs = req.call_args[0][1]
            self.assertEqual(kwargs, params)

    def test_update_time_to_live(self):
        """
        TableConnection.update_time_to_live
        """
        params = {
            'TableName': 'ci-table',
            'TimeToLiveSpecification': {
                'AttributeName': 'ttl_attr',
                'Enabled': True,
            }
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = TableConnection(self.test_table_name)
            conn.update_time_to_live('ttl_attr')
            kwargs = req.call_args[0][1]
            self.assertEqual(kwargs, params)

    def test_delete_table(self):
        """
        TableConnection.delete_table
        """
        params = {'TableName': 'ci-table'}
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = TableConnection(self.test_table_name)
            conn.delete_table()
            kwargs = req.call_args[0][1]
            self.assertEqual(kwargs, params)

    def test_update_table(self):
        """
        TableConnection.update_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = TableConnection(self.test_table_name)
            params = {
                'ProvisionedThroughput': {
                    'WriteCapacityUnits': 2,
                    'ReadCapacityUnits': 2
                },
                'TableName': self.test_table_name
            }
            conn.update_table(
                read_capacity_units=2,
                write_capacity_units=2
            )
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = TableConnection(self.test_table_name)

            global_secondary_index_updates = [
                {
                    "index_name": "foo-index",
                    "read_capacity_units": 2,
                    "write_capacity_units": 2
                }
            ]
            params = {
                'TableName': self.test_table_name,
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
                read_capacity_units=2,
                write_capacity_units=2,
                global_secondary_index_updates=global_secondary_index_updates
            )
            self.assertEqual(req.call_args[0][1], params)

    def test_describe_table(self):
        """
        TableConnection.describe_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn = TableConnection(self.test_table_name)
            conn.describe_table()
            self.assertEqual(conn.table_name, self.test_table_name)
            self.assertEqual(req.call_args[0][1], {'TableName': 'ci-table'})

    def test_delete_item(self):
        """
        TableConnection.delete_item
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
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
                'TableName': self.test_table_name
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_update_item(self):
        """
        TableConnection.update_item
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table()

        attr_updates = {
            'Subject': {
                'Value': 'foo-subject',
                'Action': 'PUT'
            },
        }

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.update_item(
                'foo-key',
                actions=[Path('Subject').set('foo-subject')],
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

    def test_get_item(self):
        """
        TableConnection.get_item
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = GET_ITEM_DATA
            item = conn.get_item("Amazon DynamoDB", "How do I update multiple items?")
            self.assertEqual(item, GET_ITEM_DATA)

    def test_put_item(self):
        """
        TableConnection.put_item
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': self.test_table_name,
                'Item': {'ForumName': {'S': 'foo-value'}, 'Subject': {'S': 'foo-range-key'}}
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
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
            req.return_value = HttpOK(), {}
            conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'},
                condition=Path('ForumName').does_not_exist()
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
                'TableName': self.test_table_name,
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                }
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_batch_write_item(self):
        """
        TableConnection.batch_write_item
        """
        items = []
        conn = TableConnection(self.test_table_name)
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{}".format(i)}
            )
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table()
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_write_item(
                put_items=items
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    self.test_table_name: [
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

    def test_batch_get_item(self):
        """
        TableConnection.batch_get_item
        """
        items = []
        conn = TableConnection(self.test_table_name)
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{}".format(i)}
            )
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_get_item(
                items
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    self.test_table_name: {
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
        TableConnection.query
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
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
                'TableName': self.test_table_name
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_scan(self):
        """
        TableConnection.scan
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn.describe_table()
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.scan()
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': self.test_table_name
            }
            self.assertEqual(req.call_args[0][1], params)
