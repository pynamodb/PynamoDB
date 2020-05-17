"""
Test suite for the table class
"""
import six
from unittest import TestCase

from pynamodb.connection import TableConnection
from pynamodb.constants import DEFAULT_REGION
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
        self.test_table_with_schema_name = 'ci-table-w-schema'
        self.test_predefined_schema = {
            "attribute_definitions": [
                {
                    "attribute_name": "ForumName",
                    "attribute_type": "S"
                },
                {
                    "attribute_name": "LastPostDateTime",
                    "attribute_type": "S"
                },
                {
                    "attribute_name": "Subject",
                    "attribute_type": "S"
                }
            ],
            "key_schema": [
                {
                    "attribute_name": "ForumName",
                    "key_type": "HASH"
                },
                {
                    "attribute_name": "Subject",
                    "key_type": "RANGE"
                }
            ],
            "global_secondary_indexes": [
                {
                    "index_name": "LastPostIndex",
                    "key_schema": [
                        {
                            "AttributeName": "ForumName",
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "LastPostDateTime",
                            "KeyType": "RANGE"
                        }
                    ],
                    "projection": {
                        "ProjectionType": "KEYS_ONLY"
                    }
                }
            ],
            "local_secondary_indexes": [
                {
                    "index_name": "LastPostIndex",
                    "key_schema": [
                        {
                            "AttributeName": "ForumName",
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "LastPostDateTime",
                            "KeyType": "RANGE"
                        }
                    ],
                    "projection": {
                        "ProjectionType": "KEYS_ONLY"
                    }
                }
            ],
        }
        self.region = DEFAULT_REGION

    def _prepare_connections(self):
        """
        Instantiate two new connection instances:
        with predefined schema (from self.test_predefined_schema) and without
        :return:
        """
        return (TableConnection(self.test_table_name),
                TableConnection(self.test_table_with_schema_name,
                                predefined_schema=self.test_predefined_schema))

    def _run_describe_table_for_connection(self, connection, describe_table_data):
        """
        Run TableConnection.describe_table in a mocked environment
        to populate the cache used for following API calls
        :param connection:
        :param describe_table_data:
        :return:
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = describe_table_data
            connection.describe_table()

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
        for conn in self._prepare_connections():
            table_name = conn.table_name
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
                'TableName': table_name,
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
            'TableName': self.test_table_name,
            'TimeToLiveSpecification': {
                'AttributeName': 'ttl_attr',
                'Enabled': True,
            }
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = self._prepare_connections()[0]
            conn.update_time_to_live('ttl_attr')
            kwargs = req.call_args[0][1]
            self.assertEqual(kwargs, params)

    def test_delete_table(self):
        """
        TableConnection.delete_table
        """
        for conn in self._prepare_connections():
            table_name = conn.table_name
            with patch(PATCH_METHOD) as req:
                req.return_value = HttpOK(), None
                conn.delete_table()
                kwargs = req.call_args[0][1]
                self.assertEqual(kwargs, {'TableName': table_name})

    def test_update_table(self):
        """
        TableConnection.update_table
        """
        for conn in self._prepare_connections():
            table_name = conn.table_name
            with patch(PATCH_METHOD) as req:
                req.return_value = HttpOK(), None
                params = {
                    'ProvisionedThroughput': {
                        'WriteCapacityUnits': 2,
                        'ReadCapacityUnits': 2
                    },
                    'TableName': table_name
                }
                conn.update_table(
                    read_capacity_units=2,
                    write_capacity_units=2
                )
                self.assertEqual(req.call_args[0][1], params)

        for conn in self._prepare_connections():
            table_name = conn.table_name
            with patch(PATCH_METHOD) as req:
                req.return_value = HttpOK(), None

                global_secondary_index_updates = [
                    {
                        "index_name": "foo-index",
                        "read_capacity_units": 2,
                        "write_capacity_units": 2
                    }
                ]
                params = {
                    'TableName': table_name,
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
        for conn in self._prepare_connections():
            table_name = conn.table_name
            with patch(PATCH_METHOD) as req:
                req.return_value = DESCRIBE_TABLE_DATA
                conn.describe_table()
                conn.describe_table()
                self.assertEqual(req.call_args[0][1], {'TableName': table_name})
                assert len(req.mock_calls) == 2, 'The request is not supposed to be cached'

    def test_delete_item(self):
        """
        TableConnection.delete_item
        """
        params = {
            'ReturnConsumedCapacity': 'TOTAL',
            'Key': {
                'ForumName': {
                    'S': 'Amazon DynamoDB'
                },
                'Subject': {
                    'S': 'How do I update multiple items?'
                }
            }
        }
        conn, conn_w_schema = self._prepare_connections()
        params['TableName'] = conn.table_name
        self._run_describe_table_for_connection(conn, DESCRIBE_TABLE_DATA)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.delete_item(
                "Amazon DynamoDB",
                "How do I update multiple items?")
            self.assertEqual(req.call_args[0][1], params)

        params['TableName'] = conn_w_schema.table_name
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn_w_schema.delete_item(
                "Amazon DynamoDB",
                "How do I update multiple items?")
            self.assertEqual(req.call_args[0][1], params)
            assert len(req.mock_calls) == 1, ('Only one api call is expected,'
                                              ' because describe table request is not needed')

    def test_update_item(self):
        """
        TableConnection.update_item
        """
        attr_updates = {
            'Subject': {
                'Value': 'foo-subject',
                'Action': 'PUT'
            },
        }
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
            'ReturnConsumedCapacity': 'TOTAL'
        }
        conn, conn_w_schema = self._prepare_connections()
        params['TableName'] = conn.table_name
        self._run_describe_table_for_connection(conn, DESCRIBE_TABLE_DATA)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.update_item(
                'foo-key',
                actions=[Path('Subject').set('foo-subject')],
                range_key='foo-range-key',
            )
            self.assertEqual(req.call_args[0][1], params)

        params['TableName'] = conn_w_schema.table_name
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn_w_schema.update_item(
                'foo-key',
                actions=[Path('Subject').set('foo-subject')],
                range_key='foo-range-key',
            )
            self.assertEqual(req.call_args[0][1], params)
            assert len(req.mock_calls) == 1, ('Only one api call is expected,'
                                              ' because describe table request is not needed')

    def test_get_item(self):
        """
        TableConnection.get_item
        """
        conn, conn_w_schema = self._prepare_connections()
        self._run_describe_table_for_connection(conn, DESCRIBE_TABLE_DATA)

        with patch(PATCH_METHOD) as req:
            req.return_value = GET_ITEM_DATA
            item = conn.get_item("Amazon DynamoDB", "How do I update multiple items?")
            self.assertEqual(item, GET_ITEM_DATA)

        with patch(PATCH_METHOD) as req:
            req.return_value = GET_ITEM_DATA
            item = conn_w_schema.get_item("Amazon DynamoDB", "How do I update multiple items?")
            self.assertEqual(item, GET_ITEM_DATA)
            assert len(req.mock_calls) == 1, ('Only one api call is expected,'
                                              ' because describe table request is not needed')

    def test_put_item(self):
        """
        TableConnection.put_item
        """
        conn, conn_w_schema = self._prepare_connections()

        self._run_describe_table_for_connection(conn, DESCRIBE_TABLE_DATA)
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': conn.table_name,
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
                'TableName': conn.table_name,
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn_w_schema.put_item(
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
                'TableName': conn_w_schema.table_name,
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                }
            }
            self.assertEqual(req.call_args[0][1], params)
            assert len(req.mock_calls) == 1, ('Only one api call is expected,'
                                              ' because describe table request is not needed')

    def test_batch_write_item(self):
        """
        TableConnection.batch_write_item
        """
        items = [{"ForumName": "FooForum", "Subject": "thread-{}".format(i)}
                 for i in range(10)]
        param_items = [{'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'},
                                                'Subject': {'S': 'thread-{}'.format(i)}}}}
                       for i in range(10)]

        conn, conn_w_schema = self._prepare_connections()

        self._run_describe_table_for_connection(conn, DESCRIBE_TABLE_DATA)
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_write_item(put_items=items)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    conn.table_name: param_items
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn_w_schema.batch_write_item(put_items=items)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    conn_w_schema.table_name: param_items
                }
            }
            self.assertEqual(req.call_args[0][1], params)
            assert len(req.mock_calls) == 1, ('Only one api call is expected,'
                                              ' because describe table request is not needed')

    def test_batch_get_item(self):
        """
        TableConnection.batch_get_item
        """
        items = [{"ForumName": "FooForum", "Subject": "thread-{}".format(i)}
                 for i in range(10)]
        param_items = [{'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-{}'.format(i)}}
                       for i in range(10)]

        conn, conn_w_schema = self._prepare_connections()

        self._run_describe_table_for_connection(conn, DESCRIBE_TABLE_DATA)
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.batch_get_item(items)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    conn.table_name: {'Keys': param_items}
                }
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn_w_schema.batch_get_item(items)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    conn_w_schema.table_name: {'Keys': param_items}
                }
            }
            self.assertEqual(req.call_args[0][1], params)
            assert len(req.mock_calls) == 1, ('Only one api call is expected,'
                                              ' because describe table request is not needed')

    def test_query(self):
        """
        TableConnection.query
        """
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
        }

        conn, conn_w_schema = self._prepare_connections()

        self._run_describe_table_for_connection(conn, DESCRIBE_TABLE_DATA)
        params['TableName'] = conn.table_name

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn.query(
                "FooForum",
                Path('Subject').startswith('thread')
            )
            self.assertEqual(req.call_args[0][1], params)

        params['TableName'] = conn_w_schema.table_name

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            conn_w_schema.query(
                "FooForum",
                Path('Subject').startswith('thread')
            )
            self.assertEqual(req.call_args[0][1], params)
            assert len(req.mock_calls) == 1, ('Only one api call is expected,'
                                              ' because describe table request is not needed')

    def test_scan(self):
        """
        TableConnection.scan
        """
        conn, conn_w_schema = self._prepare_connections()

        self._run_describe_table_for_connection(conn, DESCRIBE_TABLE_DATA)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.scan()
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': conn.table_name
            }
            self.assertEqual(req.call_args[0][1], params)

        self._run_describe_table_for_connection(conn_w_schema, DESCRIBE_TABLE_DATA)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn_w_schema.scan()
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': conn_w_schema.table_name
            }
            self.assertEqual(req.call_args[0][1], params)
            assert len(req.mock_calls) == 1, ('Only one api call is expected,'
                                              ' because describe table request is not needed')
