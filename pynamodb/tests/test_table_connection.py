"""
Test suite for the table class
"""
import six
from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.connection import TableConnection
from pynamodb.constants import DEFAULT_REGION
from .data import DESCRIBE_TABLE_DATA, GET_ITEM_DATA
from .response import HttpOK

if six.PY3:
    from unittest.mock import patch
else:
    from mock import patch

PATCH_METHOD = 'botocore.operation.Operation.call'


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
            req.return_value = HttpOK(), None
            conn.create_table(
                **kwargs
            )
            kwargs = req.call_args[1]
            self.assertEqual(kwargs, params)

    def test_delete_table(self):
        """
        TableConnection.delete_table
        """
        params = {'table_name': 'ci-table'}
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = TableConnection(self.test_table_name)
            conn.delete_table()
            kwargs = req.call_args[1]
            self.assertEqual(kwargs, params)

    def test_update_table(self):
        """
        TableConnection.update_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = TableConnection(self.test_table_name)
            params = {
                'provisioned_throughput': {
                    'WriteCapacityUnits': 2,
                    'ReadCapacityUnits': 2
                },
                'table_name': self.test_table_name
            }
            conn.update_table(
                read_capacity_units=2,
                write_capacity_units=2
            )
            self.assertEqual(req.call_args[1], params)
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
                'table_name': self.test_table_name,
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
                read_capacity_units=2,
                write_capacity_units=2,
                global_secondary_index_updates=global_secondary_index_updates
            )
            self.assertEqual(req.call_args[1], params)

    def test_describe_table(self):
        """
        TableConnection.describe_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn = TableConnection(self.test_table_name)
            conn.describe_table()
            self.assertEqual(conn.table_name, self.test_table_name)
            self.assertEqual(req.call_args[1], {'table_name': 'ci-table'})

    def test_delete_item(self):
        """
        TableConnection.delete_item
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.delete_item(
                "Amazon DynamoDB",
                "How do I update multiple items?")
            params = {
                'return_consumed_capacity': 'TOTAL',
                'key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'table_name': self.test_table_name
            }
            self.assertEqual(req.call_args[1], params)

    def test_update_item(self):
        """
        TableConnection.delete_item
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
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
                attribute_updates=attr_updates,
                range_key='foo-range-key',
            )
            params = {
                'key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'attribute_updates': {
                    'Subject': {
                        'Value': {
                            'S': 'foo-subject'
                        },
                        'Action': 'PUT'
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'ci-table'
            }
            self.assertEqual(req.call_args[1], params)

    def test_get_item(self):
        """
        TableConnection.get_item
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), GET_ITEM_DATA
            item = conn.get_item("Amazon DynamoDB", "How do I update multiple items?")
            self.assertEqual(item, GET_ITEM_DATA)

    def test_put_item(self):
        """
        TableConnection.put_item
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'table_name': self.test_table_name,
                'item': {'ForumName': {'S': 'foo-value'}, 'Subject': {'S': 'foo-range-key'}}
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'item': {
                    'ForumName': {
                        'S': 'foo-value'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'table_name': self.test_table_name
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'},
                conditional_operator='and',
                expected={
                    'ForumName': {
                        'Exists': False
                    }
                }
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'item': {
                    'ForumName': {
                        'S': 'foo-value'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'table_name': self.test_table_name,
                'conditional_operator': 'AND',
                'expected': {
                    'ForumName': {
                        'Exists': False
                    }
                }
            }
            self.assertEqual(req.call_args[1], params)

    def test_batch_write_item(self):
        """
        TableConnection.batch_write_item
        """
        items = []
        conn = TableConnection(self.test_table_name)
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{0}".format(i)}
            )
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table()
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.batch_write_item(
                put_items=items
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'request_items': {
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
            self.assertEqual(req.call_args[1], params)

    def test_batch_get_item(self):
        """
        TableConnection.batch_get_item
        """
        items = []
        conn = TableConnection(self.test_table_name)
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{0}".format(i)}
            )
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.batch_get_item(
                items
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'request_items': {
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
            self.assertEqual(req.call_args[1], params)

    def test_query(self):
        """
        TableConnection.query
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table()
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.query(
                "FooForum",
                key_conditions={'ForumName': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'key_conditions': {
                    'ForumName': {
                        'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': [{
                            'S': 'thread'
                        }]
                    }
                },
                'table_name': self.test_table_name
            }
            self.assertEqual(req.call_args[1], params)

    def test_scan(self):
        """
        TableConnection.scan
        """
        conn = TableConnection(self.test_table_name)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table()
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.scan()
            params = {
                'return_consumed_capacity': 'TOTAL',
                'table_name': self.test_table_name
            }
            self.assertEqual(req.call_args[1], params)
