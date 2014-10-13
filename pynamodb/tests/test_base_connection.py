"""
Tests for the base connection class
"""
import six
from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.connection import Connection
from pynamodb.exceptions import (
    TableError, DeleteError, UpdateError, PutError, GetError, ScanError, QueryError, TableDoesNotExist)
from pynamodb.constants import DEFAULT_REGION
from .data import DESCRIBE_TABLE_DATA, GET_ITEM_DATA, LIST_TABLE_DATA
from .response import HttpBadRequest, HttpOK, HttpUnavailable

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
        Connection()
        """
        conn = Connection()
        self.assertIsNotNone(conn)
        conn = Connection(host='foo-host')
        self.assertIsNotNone(conn.endpoint)
        self.assertIsNotNone(conn)
        self.assertEqual(repr(conn), "Connection<{0}>".format(conn.endpoint.host))

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
            req.return_value = HttpBadRequest(), None
            self.assertRaises(TableError, conn.create_table, self.test_table_name, **kwargs)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn.create_table(
                self.test_table_name,
                **kwargs
            )
            self.assertEqual(req.call_args[1], params)

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
        params['global_secondary_indexes'] = [{'IndexName': 'alt-index', 'Projection': {'ProjectionType': 'KEYS_ONLY'},
                                               'KeySchema': [{'AttributeName': 'AltKey', 'KeyType': 'HASH'}],
                                               'ProvisionedThroughput': {'ReadCapacityUnits': 1,
                                                                         'WriteCapacityUnits': 1}}]
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn.create_table(
                self.test_table_name,
                **kwargs
            )
            # Ensure that the hash key is first when creating indexes
            self.assertEqual(req.call_args[1]['global_secondary_indexes'][0]['KeySchema'][0]['KeyType'], 'HASH')
            self.assertEqual(req.call_args[1], params)
        del(kwargs['global_secondary_indexes'])
        del(params['global_secondary_indexes'])

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
        params['local_secondary_indexes'] = [
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
            req.return_value = HttpOK(), None
            conn.create_table(
                self.test_table_name,
                **kwargs
            )
            self.assertEqual(req.call_args[1], params)

    def test_delete_table(self):
        """
        Connection.delete_table
        """
        params = {'table_name': 'ci-table'}
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = Connection(self.region)
            conn.delete_table(self.test_table_name)
            kwargs = req.call_args[1]
            self.assertEqual(kwargs, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), None
            conn = Connection(self.region)
            self.assertRaises(TableError, conn.delete_table, self.test_table_name)

    def test_update_table(self):
        """
        Connection.update_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            conn = Connection(self.region)
            params = {
                'provisioned_throughput': {
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

        self.assertRaises(ValueError, conn.update_table, self.test_table_name, read_capacity_units=2)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), None
            conn = Connection(self.region)
            self.assertRaises(
                TableError,
                conn.update_table,
                self.test_table_name,
                read_capacity_units=2,
                write_capacity_units=2)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
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
        Connection.describe_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn = Connection(self.region)
            conn.describe_table(self.test_table_name)
            self.assertEqual(req.call_args[1], {'table_name': 'ci-table'})

        with self.assertRaises(TableDoesNotExist):
            with patch(PATCH_METHOD) as req:
                req.return_value = HttpBadRequest(), DESCRIBE_TABLE_DATA
                conn = Connection(self.region)
                conn.describe_table(self.test_table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpUnavailable(), None
            conn = Connection(self.region)
            self.assertRaises(TableError, conn.describe_table, self.test_table_name)

    def test_list_tables(self):
        """
        Connection.list_tables
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables(exclusive_start_table_name='Thread')
            self.assertEqual(req.call_args[1], {'exclusive_start_table_name': 'Thread'})

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables(limit=3)
            self.assertEqual(req.call_args[1], {'limit': 3})

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), LIST_TABLE_DATA
            conn = Connection(self.region)
            conn.list_tables()
            self.assertEqual(req.call_args[1], {})

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), None
            conn = Connection(self.region)
            self.assertRaises(TableError, conn.list_tables)

    def test_delete_item(self):
        """
        Connection.delete_item
        """
        conn = Connection(self.region)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), {}
            self.assertRaises(DeleteError, conn.delete_item, self.test_table_name, "foo", "bar")

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.delete_item(
                self.test_table_name,
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
                'table_name': self.test_table_name}
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                return_values='ALL_NEW'
            )
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
                'table_name': self.test_table_name,
                'return_values': 'ALL_NEW'
            }
            self.assertEqual(req.call_args[1], params)

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
            req.return_value = HttpOK(), {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                return_consumed_capacity='TOTAL'
            )
            params = {
                'key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'table_name': self.test_table_name,
                'return_consumed_capacity': 'TOTAL'
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                return_item_collection_metrics='SIZE'
            )
            params = {
                'key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'table_name': self.test_table_name,
                'return_item_collection_metrics': 'SIZE',
                'return_consumed_capacity': 'TOTAL'
            }
            self.assertEqual(req.call_args[1], params)

        self.assertRaises(
            ValueError,
            conn.delete_item,
            self.test_table_name,
            "Foo", "Bar",
            expected={'Bad': {'Value': False}}
        )

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                expected={'ForumName': {'Exists': False}},
                return_item_collection_metrics='SIZE'
            )
            params = {
                'key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'expected': {
                    'ForumName': {
                        'Exists': False
                    }
                },
                'table_name': self.test_table_name,
                'return_consumed_capacity': 'TOTAL',
                'return_item_collection_metrics': 'SIZE'
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.delete_item(
                self.test_table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                conditional_operator='and',
                expected={'ForumName': {'Exists': False}},
                return_item_collection_metrics='SIZE'
            )
            params = {
                'key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'expected': {
                    'ForumName': {
                        'Exists': False
                    }
                },
                'table_name': self.test_table_name,
                'conditional_operator': 'AND',
                'return_consumed_capacity': 'TOTAL',
                'return_item_collection_metrics': 'SIZE'
            }
            self.assertEqual(req.call_args[1], params)

    def test_get_item(self):
        """
        Connection.get_item
        """
        conn = Connection(self.region)
        table_name = 'Thread'
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), GET_ITEM_DATA
            item = conn.get_item(table_name, "Amazon DynamoDB", "How do I update multiple items?")
            self.assertEqual(item, GET_ITEM_DATA)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), None
            self.assertRaises(
                GetError,
                conn.get_item,
                table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?"
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), GET_ITEM_DATA
            conn.get_item(
                table_name,
                "Amazon DynamoDB",
                "How do I update multiple items?",
                attributes_to_get=['ForumName']
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'attributes_to_get': ['ForumName'],
                'key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'consistent_read': False,
                'table_name': 'Thread'
            }
            self.assertEqual(req.call_args[1], params)

    def test_update_item(self):
        """
        Connection.update_item
        """
        conn = Connection()
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        self.assertRaises(ValueError, conn.update_item, self.test_table_name, 'foo-key')

        attr_updates = {
            'Subject': {
                'Value': 'foo-subject',
                'Action': 'PUT'
            },
        }

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), {}
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
            req.return_value = HttpOK(), {}
            self.assertRaises(
                ValueError,
                conn.update_item,
                self.test_table_name,
                'foo-key',
                attribute_updates=bad_attr_updates,
                range_key='foo-range-key',
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
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
                'return_values': 'ALL_NEW',
                'return_item_collection_metrics': 'NONE',
                'return_consumed_capacity': 'TOTAL',
                'key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'expected': {
                    'Forum': {
                        'Exists': False
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
                'table_name': 'ci-table'
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.update_item(
                self.test_table_name,
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

        attr_updates = {
            'Subject': {
                'Value': {'S': 'foo-subject'},
                'Action': 'PUT'
            },
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.update_item(
                self.test_table_name,
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

        attr_updates = {
            'Subject': {
                'Value': {'S': 'Foo'},
                'Action': 'PUT'
            },
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.update_item(
                self.test_table_name,
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
                            'S': 'Foo'
                        },
                        'Action': 'PUT'
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'ci-table'
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
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
            req.return_value = HttpOK(), {}
            # attributes are missing
            with self.assertRaises(ValueError):
                conn.update_item(
                    self.test_table_name,
                    'foo-key',
                    range_key='foo-range-key',
                )

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
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
                            'S': 'Bar'
                        },
                        'Action': 'PUT'
                    }
                },
                'expected': {
                    'ForumName': {
                        'Exists': False
                    },
                    'Subject': {
                        'Value': {
                            'S': 'Foo'
                        }
                    }
                },
                'conditional_operator': 'AND',
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'ci-table'
            }
            self.assertEqual(req.call_args[1], params)

    def test_put_item(self):
        """
        Connection.put_item
        """
        conn = Connection(self.region)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table(self.test_table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), None
            self.assertRaises(
                TableError,
                conn.put_item,
                'foo-key',
                self.test_table_name,
                return_values='ALL_NEW',
                attributes={'ForumName': 'foo-value'}
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
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
                'return_values': 'ALL_NEW',
                'return_consumed_capacity': 'TOTAL',
                'return_item_collection_metrics': 'SIZE',
                'table_name': self.test_table_name,
                'item': {
                    'ForumName': {
                        'S': 'foo-value'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                }
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), {}
            self.assertRaises(
                PutError,
                conn.put_item,
                self.test_table_name,
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.put_item(
                self.test_table_name,
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {'table_name': self.test_table_name,
                      'return_consumed_capacity': 'TOTAL',
                      'item': {'ForumName': {'S': 'foo-value'}, 'Subject': {'S': 'foo-range-key'}}}
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.put_item(
                self.test_table_name,
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
                'return_consumed_capacity': 'TOTAL',
                'table_name': self.test_table_name,
                'expected': {
                    'Forum': {
                        'Exists': False
                    },
                    'Subject': {
                        'Value': {'S': 'Foo'}
                    }
                },
                'item': {
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
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.put_item(
                self.test_table_name,
                'item1-hash',
                range_key='item1-range',
                attributes={'foo': {'S': 'bar'}},
                expected={'ForumName': {'Value': 'item1-hash'}}
            )
            params = {
                'table_name': self.test_table_name,
                'expected': {
                    'ForumName': {
                        'Value': {
                            'S': 'item1-hash'
                        }
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'item': {
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
            self.assertEqual(req.call_args[1], params)

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
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.batch_write_item(
                table_name,
                put_items=items,
                return_item_collection_metrics='SIZE',
                return_consumed_capacity='TOTAL'
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'return_item_collection_metrics': 'SIZE',
                'request_items': {
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
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.batch_write_item(
                table_name,
                put_items=items
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'request_items': {
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
            self.assertEqual(req.call_args[1], params)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), {}
            self.assertRaises(
                PutError,
                conn.batch_write_item,
                table_name,
                delete_items=items
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.batch_write_item(
                table_name,
                delete_items=items
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'request_items': {
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
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.batch_write_item(
                table_name,
                delete_items=items,
                return_consumed_capacity='TOTAL',
                return_item_collection_metrics='SIZE'
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'return_item_collection_metrics': 'SIZE',
                'request_items': {
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
            self.assertEqual(req.call_args[1], params)

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
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpBadRequest(), {}
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
            req.return_value = HttpOK(), {}
            conn.batch_get_item(
                table_name,
                items,
                consistent_read=True,
                return_consumed_capacity='TOTAL',
                attributes_to_get=['ForumName']
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'request_items': {
                    'Thread': {
                        'consistent_read': True,
                        'attributes_to_get': ['ForumName'],
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

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.batch_get_item(
                table_name,
                items
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'request_items': {
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
            self.assertEqual(req.call_args[1], params)

    def test_query(self):
        """
        Connection.query
        """
        conn = Connection()
        table_name = 'Thread'
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

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
            req.return_value = HttpBadRequest(), {}
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
            req.return_value = HttpOK(), {}
            conn.query(
                table_name,
                "FooForum",
                scan_index_forward=True,
                return_consumed_capacity='TOTAL',
                select='ALL_ATTRIBUTES',
                key_conditions={'ForumName': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
            )
            params = {
                'scan_index_forward': True,
                'select': 'ALL_ATTRIBUTES',
                'return_consumed_capacity': 'TOTAL',
                'key_conditions': {
                    'ForumName': {
                        'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': [{
                            'S': 'thread'
                        }]
                    }
                },
                'table_name': 'Thread'
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.query(
                table_name,
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
                'table_name': 'Thread'
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
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
                'limit': 1,
                'return_consumed_capacity': 'TOTAL',
                'consistent_read': True,
                'exclusive_start_key': {
                    'ForumName': {
                        'S': 'FooForum'
                    }
                },
                'index_name': 'LastPostIndex',
                'attributes_to_get': ['ForumName'],
                'key_conditions': {
                    'ForumName': {
                        'ComparisonOperator': 'EQ', 'AttributeValueList': [{
                            'S': 'FooForum'
                        }]
                    }
                },
                'table_name': 'Thread'
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.query(
                table_name,
                "FooForum",
                select='ALL_ATTRIBUTES',
                exclusive_start_key="FooForum"
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'exclusive_start_key': {
                    'ForumName': {
                        'S': 'FooForum'
                    }
                },
                'key_conditions': {
                    'ForumName': {
                        'ComparisonOperator': 'EQ', 'AttributeValueList': [{
                            'S': 'FooForum'
                        }]
                    }
                },
                'table_name': 'Thread',
                'select': 'ALL_ATTRIBUTES'
            }
            self.assertEqual(req.call_args[1], params)

    def test_scan(self):
        """
        Connection.scan
        """
        conn = Connection()
        table_name = 'Thread'

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), DESCRIBE_TABLE_DATA
            conn.describe_table(table_name)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.scan(
                table_name,
                segment=0,
                total_segments=22,
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'table_name': table_name,
                'segment': 0,
                'total_segments': 22,
            }
            self.assertDictEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
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
                'attributes_to_get': ['ForumName'],
                'exclusive_start_key': {
                    "ForumName": {
                        "S": "FooForum"
                    }
                },
                'table_name': table_name,
                'limit': 1,
                'segment': 2,
                'total_segments': 4,
                'return_consumed_capacity': 'TOTAL'
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.scan(
                table_name,
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'table_name': table_name
            }
            self.assertEqual(req.call_args[1], params)

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
            req.return_value = HttpBadRequest(), {}
            self.assertRaises(
                ScanError,
                conn.scan,
                table_name,
                **kwargs)


        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.scan(
                table_name,
                **kwargs
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'table_name': table_name,
                'scan_filter': {
                    'ForumName': {
                        'AttributeValueList': [
                            {'S': 'Foo'}
                        ],
                        'ComparisonOperator': 'BEGINS_WITH'
                    }
                }
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            conn.scan(
                table_name,
                **kwargs
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'table_name': table_name,
                'scan_filter': {
                    'ForumName': {
                        'AttributeValueList': [
                            {'S': 'Foo'}
                        ],
                        'ComparisonOperator': 'BEGINS_WITH'
                    }
                }
            }
            self.assertEqual(req.call_args[1], params)

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
            req.return_value = HttpOK(), {}
            conn.scan(
                table_name,
                **kwargs
            )
            params = {
                'return_consumed_capacity': 'TOTAL',
                'table_name': table_name,
                'scan_filter': {
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
                'conditional_operator': 'AND'
            }
            self.assertEqual(req.call_args[1], params)

        kwargs['conditional_operator'] = 'invalid'

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            self.assertRaises(
                ValueError,
                conn.scan,
                table_name,
                **kwargs)
