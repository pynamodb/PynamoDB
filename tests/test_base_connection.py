"""
Tests for the base connection class
"""
import base64
import json
from datetime import datetime
from uuid import UUID

import urllib3
from unittest import mock
from unittest.mock import patch

import botocore.exceptions
from botocore.awsrequest import AWSResponse
from botocore.client import ClientError
from botocore.exceptions import BotoCoreError

import pytest
from freezegun import freeze_time

from pynamodb.connection import Connection
from pynamodb.connection.base import MetaTable
from pynamodb.exceptions import (
    TableError, DeleteError, PutError, ScanError, GetError, UpdateError, TableDoesNotExist, VerboseClientError)
from pynamodb.constants import (
    UNPROCESSED_ITEMS, STRING, BINARY, DEFAULT_ENCODING, TABLE_KEY,
    PAY_PER_REQUEST_BILLING_MODE)
from pynamodb.expressions.operand import Path, Value
from pynamodb.expressions.update import SetAction
from .data import DESCRIBE_TABLE_DATA, GET_ITEM_DATA, LIST_TABLE_DATA

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'
TEST_TABLE_NAME = DESCRIBE_TABLE_DATA['Table']['TableName']
REGION = 'us-east-1'


@pytest.fixture
def meta_table():
    return MetaTable(DESCRIBE_TABLE_DATA.get(TABLE_KEY))


def test_meta_table_get_key_names(meta_table):
    key_names = meta_table.get_key_names()
    assert key_names == ["ForumName", "Subject"]


def test_meta_table_get_key_names__index(meta_table):
    key_names = meta_table.get_key_names("LastPostIndex")
    assert key_names == ["ForumName", "Subject", "LastPostDateTime"]


def test_meta_table_get_attribute_type(meta_table):
    assert meta_table.get_attribute_type('ForumName') == 'S'
    with pytest.raises(ValueError):
        meta_table.get_attribute_type('wrongone')


def test_meta_table_has_index_name(meta_table):
    assert meta_table.has_index_name("LastPostIndex")
    assert not meta_table.has_index_name("NonExistentIndexName")


def test_connection__create():
    _ = Connection()
    conn = Connection(host='http://foohost')
    assert conn.client
    assert repr(conn) == "Connection<http://foohost>"


def test_connection__subsequent_client_is_not_cached_when_credentials_none():
    with patch('pynamodb.connection.Connection.session') as session_mock:
        session_mock.create_client.return_value._request_signer._credentials = None
        conn = Connection()

        # make two calls to .client property, expect two calls to create client
        assert conn.client
        conn.client

        session_mock.create_client.assert_has_calls(
            [
                mock.call('dynamodb', None, endpoint_url=None, config=mock.ANY),
                mock.call('dynamodb', None, endpoint_url=None, config=mock.ANY),
            ],
            any_order=True
        )


def test_connection__subsequent_client_is_cached_when_credentials_truthy():
    with patch('pynamodb.connection.Connection.session') as session_mock:
        session_mock.create_client.return_value._request_signer._credentials = True
        conn = Connection()

        # make two calls to .client property, expect one call to create client
        assert conn.client
        assert conn.client

        assert (
            session_mock.create_client.mock_calls.count(mock.call('dynamodb', None, endpoint_url=None, config=mock.ANY)) ==
            1
        )


def test_connection__client_is_passed_region_when_set():
    with patch('pynamodb.connection.Connection.session') as session_mock:
        session_mock.create_client.return_value._request_signer._credentials = True
        conn = Connection(REGION)

        assert conn.client

        assert (
            session_mock.create_client.mock_calls.count(mock.call('dynamodb', REGION, endpoint_url=None, config=mock.ANY)) ==
            1
        )


def test_connection_create_table():
    """
    Connection.create_table
    """
    conn = Connection(REGION)
    kwargs = {
        'read_capacity_units': 1,
        'write_capacity_units': 1,
    }
    with pytest.raises(ValueError):
        conn.create_table(TEST_TABLE_NAME, **kwargs)

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
    with pytest.raises(ValueError):
        conn.create_table(TEST_TABLE_NAME, **kwargs)

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
        'TableName': TEST_TABLE_NAME,
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
        with pytest.raises(TableError):
            conn.create_table(TEST_TABLE_NAME, **kwargs)

    with patch(PATCH_METHOD) as req:
        req.return_value = None
        conn.create_table(
            TEST_TABLE_NAME,
            **kwargs
        )
        assert req.call_args[0][1] == params

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
            TEST_TABLE_NAME,
            **kwargs
        )
        # Ensure that the hash key is first when creating indexes
        assert req.call_args[0][1]['GlobalSecondaryIndexes'][0]['KeySchema'][0]['KeyType'] == 'HASH'
        assert req.call_args[0][1] == params
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
            TEST_TABLE_NAME,
            **kwargs
        )
        assert req.call_args[0][1] == params

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
            TEST_TABLE_NAME,
            **kwargs
        )
        assert req.call_args[0][1] == params

    kwargs['billing_mode'] = PAY_PER_REQUEST_BILLING_MODE
    params['BillingMode'] = PAY_PER_REQUEST_BILLING_MODE
    del params['ProvisionedThroughput']
    with patch(PATCH_METHOD) as req:
        req.return_value = None
        conn.create_table(
            TEST_TABLE_NAME,
            **kwargs
        )
        assert req.call_args[0][1] == params


def test_connection_delete_table():
    """
    Connection.delete_table
    """
    params = {'TableName': TEST_TABLE_NAME}
    with patch(PATCH_METHOD) as req:
        req.return_value = None
        conn = Connection(REGION)
        conn.delete_table(TEST_TABLE_NAME)
        kwargs = req.call_args[0][1]
        assert kwargs == params

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        conn = Connection(REGION)
        with pytest.raises(TableError):
            conn.delete_table(TEST_TABLE_NAME)


def test_connection_update_table():
    """
    Connection.update_table
    """
    with patch(PATCH_METHOD) as req:
        req.return_value = None
        conn = Connection(REGION)
        params = {
            'ProvisionedThroughput': {
                'WriteCapacityUnits': 2,
                'ReadCapacityUnits': 2
            },
            'TableName': TEST_TABLE_NAME,
        }
        conn.update_table(
            TEST_TABLE_NAME,
            read_capacity_units=2,
            write_capacity_units=2
        )
        assert req.call_args[0][1] == params

    with pytest.raises(ValueError):
        conn.update_table(TEST_TABLE_NAME, read_capacity_units=2)

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        conn = Connection(REGION)
        with pytest.raises(TableError):
            conn.update_table(TEST_TABLE_NAME, read_capacity_units=2, write_capacity_units=2)

    with patch(PATCH_METHOD) as req:
        req.return_value = None
        conn = Connection(REGION)

        global_secondary_index_updates = [
            {
                "index_name": "foo-index",
                "read_capacity_units": 2,
                "write_capacity_units": 2
            }
        ]
        params = {
            'TableName': TEST_TABLE_NAME,
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
            TEST_TABLE_NAME,
            read_capacity_units=2,
            write_capacity_units=2,
            global_secondary_index_updates=global_secondary_index_updates
        )
        assert req.call_args[0][1] == params


def test_connection_describe_table():
    """
    Connection.describe_table
    """
    with patch(PATCH_METHOD) as req:
        req.return_value = DESCRIBE_TABLE_DATA
        conn = Connection(REGION)
        conn.describe_table(TEST_TABLE_NAME)
        assert req.call_args[0][1] == {'TableName': TEST_TABLE_NAME}

    with pytest.raises(TableDoesNotExist):
        with patch(PATCH_METHOD) as req:
            req.side_effect = ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}}, "DescribeTable")
            conn = Connection(REGION)
            conn.describe_table(TEST_TABLE_NAME)


def test_connection_list_tables():
    """
    Connection.list_tables
    """
    with patch(PATCH_METHOD) as req:
        req.return_value = LIST_TABLE_DATA
        conn = Connection(REGION)
        conn.list_tables(exclusive_start_table_name='Thread')
        assert req.call_args[0][1] == {'ExclusiveStartTableName': 'Thread'}

    with patch(PATCH_METHOD) as req:
        req.return_value = LIST_TABLE_DATA
        conn = Connection(REGION)
        conn.list_tables(limit=3)
        assert req.call_args[0][1] == {'Limit': 3}

    with patch(PATCH_METHOD) as req:
        req.return_value = LIST_TABLE_DATA
        conn = Connection(REGION)
        conn.list_tables()
        assert req.call_args[0][1] == {}

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        conn = Connection(REGION)
        with pytest.raises(TableError):
            conn.list_tables()


@pytest.mark.filterwarnings("ignore:Legacy conditional")
def test_connection_delete_item():
    """
    Connection.delete_item
    """
    conn = Connection(REGION)
    conn.add_meta_table(MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(DeleteError):
            conn.delete_item(TEST_TABLE_NAME, "foo", "bar")

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.delete_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME}
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.delete_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME,
            'ReturnValues': 'ALL_NEW'
        }
        assert req.call_args[0][1] == params

    with pytest.raises(ValueError):
        conn.delete_item(TEST_TABLE_NAME, "foo", "bar", return_values='bad_values')

    with pytest.raises(ValueError):
        conn.delete_item(TEST_TABLE_NAME, "foo", "bar", return_consumed_capacity='badvalue')

    with pytest.raises(ValueError):
        conn.delete_item(TEST_TABLE_NAME, "foo", "bar", return_item_collection_metrics='badvalue')

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.delete_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME,
            'ReturnConsumedCapacity': 'TOTAL'
        }
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.delete_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME,
            'ReturnItemCollectionMetrics': 'SIZE',
            'ReturnConsumedCapacity': 'TOTAL'
        }
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.delete_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME,
            'ReturnConsumedCapacity': 'TOTAL',
            'ReturnItemCollectionMetrics': 'SIZE'
        }
        assert req.call_args[0][1] == params


def test_connection_get_item():
    """
    Connection.get_item
    """
    conn = Connection(REGION)
    table_name = 'Thread'
    conn.add_meta_table(MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

    with patch(PATCH_METHOD) as req:
        req.return_value = GET_ITEM_DATA
        item = conn.get_item(table_name, "Amazon DynamoDB", "How do I update multiple items?")
        assert item == GET_ITEM_DATA

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(GetError):
            conn.get_item(table_name, "Amazon DynamoDB", "How do I update multiple items?")

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
        assert req.call_args[0][1] == params


@pytest.mark.filterwarnings("ignore")
def test_connection_update_item():
    """
    Connection.update_item
    """
    conn = Connection()
    conn.add_meta_table(MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

    with pytest.raises(ValueError):
        conn.update_item(TEST_TABLE_NAME, 'foo-key')

    with pytest.raises(ValueError):
        conn.update_item(TEST_TABLE_NAME, 'foo', actions=[])

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.update_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME
        }
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        # attributes are missing
        with pytest.raises(ValueError):
            conn.update_item(
                TEST_TABLE_NAME,
                'foo-key',
                range_key='foo-range-key',
            )

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.update_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME,
        }
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(UpdateError):
            conn.update_item(TEST_TABLE_NAME, 'foo-key', range_key='foo-range-key', actions=[SetAction(Path('bar'), Value('foobar'))])


def test_connection_put_item():
    """
    Connection.put_item
    """
    conn = Connection(REGION)
    conn.add_meta_table(MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(TableError):
            conn.put_item('foo-key', TEST_TABLE_NAME, return_values='ALL_NEW', attributes={'ForumName': 'foo-value'})

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.put_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME,
            'Item': {
                'ForumName': {
                    'S': 'foo-value'
                },
                'Subject': {
                    'S': 'foo-range-key'
                }
            }
        }
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(PutError):
            conn.put_item(TEST_TABLE_NAME, 'foo-key', range_key='foo-range-key', attributes={'ForumName': 'foo-value'})

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.put_item(
            TEST_TABLE_NAME,
            'foo-key',
            range_key='foo-range-key',
            attributes={'ForumName': 'foo-value'}
        )
        params = {'TableName': TEST_TABLE_NAME,
                  'ReturnConsumedCapacity': 'TOTAL',
                  'Item': {'ForumName': {'S': 'foo-value'}, 'Subject': {'S': 'foo-range-key'}}}
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.put_item(
            TEST_TABLE_NAME,
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
            'TableName': TEST_TABLE_NAME
        }
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.put_item(
            TEST_TABLE_NAME,
            'item1-hash',
            range_key='item1-range',
            attributes={'foo': {'S': 'bar'}},
            condition=(Path('Forum').does_not_exist() & (Path('Subject') == 'Foo'))
        )
        params = {
            'ReturnConsumedCapacity': 'TOTAL',
            'TableName': TEST_TABLE_NAME,
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
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.put_item(
            TEST_TABLE_NAME,
            'item1-hash',
            range_key='item1-range',
            attributes={'foo': {'S': 'bar'}},
            condition=(Path('ForumName') == 'item1-hash')
        )
        params = {
            'TableName': TEST_TABLE_NAME,
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
        assert req.call_args[0][1] == params


def test_connection_transact_write_items():
    conn = Connection()
    with patch(PATCH_METHOD) as req:
        conn.transact_write_items([], [], [], [])
        assert req.call_args[0][0] == 'TransactWriteItems'
        assert req.call_args[0][1] == {
            'TransactItems': [],
            'ReturnConsumedCapacity': 'TOTAL'
        }


def test_connection_transact_get_items():
    conn = Connection()
    with patch(PATCH_METHOD) as req:
        conn.transact_get_items([])
        assert req.call_args[0][0] == 'TransactGetItems'
        assert req.call_args[0][1] == {
            'TransactItems': [],
            'ReturnConsumedCapacity': 'TOTAL'
        }


def test_connection_batch_write_item():
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
    with pytest.raises(ValueError):
        conn.batch_write_item(table_name)

    conn.add_meta_table(MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

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
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params
    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(PutError):
            conn.batch_write_item(table_name, delete_items=items)

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
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params


def test_connection_batch_get_item():
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
    conn.add_meta_table(MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(GetError):
            conn.batch_get_item(
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
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params


def test_connection_query():
    """
    Connection.query
    """
    conn = Connection()
    table_name = 'Thread'
    conn.add_meta_table(MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

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
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params


def test_connection_scan():
    """
    Connection.scan
    """
    conn = Connection()
    table_name = 'Thread'

    conn.add_meta_table(MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

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
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        conn.scan(
            table_name,
        )
        params = {
            'ReturnConsumedCapacity': 'TOTAL',
            'TableName': table_name
        }
        assert req.call_args[0][1] == params

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
        assert req.call_args[0][1] == params

    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(ScanError):
            conn.scan(table_name)


@mock.patch('botocore.httpsession.URLLib3Session.send')
def test_connection__make_api_call__wraps_verbose_client_error_create(send_mock):
    response = AWSResponse(
        url='',
        status_code=500,
        raw='',  # todo: use stream, like `botocore.tests.RawResponse`?
        headers={'X-Amzn-RequestId': 'abcdef'},
    )
    response._content = json.dumps({
        '__type': 'InternalServerError',
        'message': 'There is a problem',
        'code': 'InternalServerError',
    }).encode('utf-8')
    send_mock.return_value = response

    c = Connection(max_retry_attempts=0)

    with pytest.raises(VerboseClientError) as excinfo:
        c._make_api_call('CreateTable', {'TableName': 'MyTable'})
    assert (
        'An error occurred (InternalServerError) on request (abcdef) on table (MyTable) when calling the CreateTable operation: There is a problem'
        in str(excinfo.value)
    )

@mock.patch('botocore.httpsession.URLLib3Session.send')
def test_connection__make_api_call__wraps_verbose_client_error_batch(send_mock):
    response = AWSResponse(
        url='',
        status_code=500,
        raw='',  # todo: use stream, like `botocore.tests.RawResponse`?
        headers={'X-Amzn-RequestId': 'abcdef'},
    )
    response._content = json.dumps({
        '__type': 'InternalServerError',
        'message': 'There is a problem',
        'code': 'InternalServerError',
    }).encode('utf-8')
    send_mock.return_value = response

    c = Connection(max_retry_attempts=0)

    with pytest.raises(VerboseClientError) as excinfo:
        c._make_api_call('BatchGetItem', {
            'RequestItems': {
                'table_one': {
                    "Keys": [
                        {"ID": {"S": "1"}},
                        {"ID": {"S": "2"}},
                    ]
                },
                'table_two': {
                    "Keys": [
                        {"ID": {"S": "3"}}
                    ],
                },
            },
        })
    assert (
        'An error occurred (InternalServerError) on request (abcdef) on table (table_one,table_two) when calling the BatchGetItem operation: There is a problem'
        in str(excinfo.value)
    )


@mock.patch('botocore.httpsession.URLLib3Session.send')
def test_connection__make_api_call__wraps_verbose_client_error_transact(send_mock):
    response = AWSResponse(
        url='',
        status_code=500,
        raw='',  # todo: use stream, like `botocore.tests.RawResponse`?
        headers={'X-Amzn-RequestId': 'abcdef'},
    )
    response._content = json.dumps({
        '__type': 'InternalServerError',
        'message': 'There is a problem',
        'code': 'InternalServerError',
    }).encode('utf-8')
    send_mock.return_value = response

    c = Connection(max_retry_attempts=0)

    with pytest.raises(VerboseClientError) as excinfo:
        c._make_api_call('TransactWriteItems', {
            'ClientRequestToken': "some_token",
            'TransactItems': [
                {
                    'Put': {
                        'Item': {'id': {'S': 'item_id_one'}},
                        'TableName': 'table_one',
                    },
                },
                {
                    'Update': {
                        'Key': {'id': {'S': 'item_id_two'}},
                        'TableName': 'table_two',
                    }
                },
            ],
        })
    assert (
        'An error occurred (InternalServerError) on request (abcdef) on table (table_one,table_two) when calling the TransactWriteItems operation: There is a problem'
        in str(excinfo.value)
    )

@mock.patch('botocore.httpsession.URLLib3Session.send')
def test_connection__make_api_call_throws_verbose_error_after_backoff_later_succeeds(send_mock):
    # mock response
    bad_response = mock.Mock(spec=AWSResponse)
    bad_response.status_code = 500
    bad_response.headers = {'x-amzn-RequestId': 'abcdef'}
    bad_response.text = json.dumps({'message': 'There is a problem', '__type': 'InternalServerError'})
    bad_response.content = bad_response.text.encode()

    good_response_content = {
        'TableDescription': {'TableName': 'table', 'TableStatus': 'Creating'},
        'ResponseMetadata': {'HTTPHeaders': {}, 'HTTPStatusCode': 200, 'RetryAttempts': 2},
    }
    good_response = mock.Mock(spec=AWSResponse)
    good_response.status_code = 200
    good_response.headers = {}
    good_response.text = json.dumps(good_response_content)
    good_response.content = good_response.text.encode()

    send_mock.side_effect = [
        bad_response,
        bad_response,
        good_response,
    ]


    c = Connection()

    assert c._make_api_call('CreateTable', {'TableName': 'MyTable'}) == good_response_content
    assert len(send_mock.mock_calls) == 3


@mock.patch('botocore.httpsession.URLLib3Session.send')
def test_connection_make_api_call__retries_properly(send_mock):
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

    send_mock.side_effect = [
        bad_response,
        botocore.exceptions.ReadTimeoutError(endpoint_url='http://lyft.com'),
        bad_response,
        deserializable_response,
    ]
    c = Connection(max_retry_attempts=3)

    c._make_api_call('DescribeTable', {'TableName': 'MyTable'})
    assert len(send_mock.mock_calls) == 4


def test_connection__botocore_config():
    c = Connection(connect_timeout_seconds=5, read_timeout_seconds=10, max_pool_connections=20)
    assert c.client._client_config.connect_timeout == 5
    assert c.client._client_config.read_timeout == 10
    assert c.client._client_config.max_pool_connections == 20


@freeze_time()
def test_connection_make_api_call___extra_headers(mocker):
    good_response = mock.Mock(spec=AWSResponse, status_code=200, headers={}, text='{}', content=b'{}')
    send_mock = mocker.patch('botocore.httpsession.URLLib3Session.send', return_value=good_response)

    # return constant UUID
    mocker.patch('uuid.uuid4', return_value=UUID('01FC4BDB-B223-4B86-88F4-DEE79B77F275'))

    c = Connection(extra_headers={'foo': 'bar'}, max_retry_attempts=0)
    c._make_api_call(
        'DescribeTable',
        {'TableName': 'MyTable'},
    )

    assert send_mock.call_count == 1
    request = send_mock.call_args[0][0]
    assert request.headers['foo'] == 'bar'

    c = Connection(extra_headers={'foo': 'baz'}, max_retry_attempts=0)
    c._make_api_call(
        'DescribeTable',
        {'TableName': 'MyTable'},
    )

    assert send_mock.call_count == 2
    request2 = send_mock.call_args[0][0]
    # all headers, including signatures, and except 'foo', should match
    assert {**request.headers, 'foo': ''} == {**request2.headers, 'foo': ''}


@mock.patch('botocore.httpsession.URLLib3Session.send')
def test_connection_make_api_call__throws_when_retries_exhausted(send_mock):
    send_mock.side_effect = [
        botocore.exceptions.ConnectionError(error="problems"),
        botocore.exceptions.ConnectionError(error="problems"),
        botocore.exceptions.ConnectionError(error="problems"),
        botocore.exceptions.ReadTimeoutError(endpoint_url="http://lyft.com"),
    ]
    c = Connection(max_retry_attempts=3)

    with pytest.raises(botocore.exceptions.ReadTimeoutError):
        c._make_api_call('DescribeTable', {'TableName': 'MyTable'})

    assert len(send_mock.mock_calls) == 4


@mock.patch('botocore.httpsession.URLLib3Session.send')
def test_connection_make_api_call__throws_retry_disabled(send_mock):
    send_mock.side_effect = [
        botocore.exceptions.ReadTimeoutError(endpoint_url='http://lyft.com'),
    ]
    c = Connection(read_timeout_seconds=11, max_retry_attempts=0)

    with pytest.raises(botocore.exceptions.ReadTimeoutError):
        c._make_api_call('DescribeTable', {'TableName': 'MyTable'})

    assert len(send_mock.mock_calls) == 1


@mock.patch('urllib3.connectionpool.HTTPConnectionPool.urlopen')
def test_connection_make_api_call__throws_conn_closed(urlopen_mock):
    urlopen_mock.side_effect = [
        urllib3.exceptions.ProtocolError(),
    ]
    c = Connection(read_timeout_seconds=11, max_retry_attempts=0)

    with pytest.raises(botocore.exceptions.ConnectionClosedError):
        c._make_api_call('DescribeTable', {'TableName': 'MyTable'})


@mock.patch('botocore.httpsession.URLLib3Session.send')
def test_connection_make_api_call__binary_attributes(send_mock):
    binary_blob = b'\x00\xFF\x00\xFF'
    resp_text = json.dumps({
        UNPROCESSED_ITEMS: {
            'someTable': [{
                'PutRequest': {
                    'Item': {
                        'name': {STRING: 'daniel'},
                        'picture': {BINARY: base64.b64encode(binary_blob).decode(DEFAULT_ENCODING)},
                    }
                }
            }],
        }
    })

    resp = mock.Mock(
        spec=AWSResponse,
        status_code=200,
        headers={},
        content=resp_text.encode(),
    )

    send_mock.return_value = resp

    resp = Connection()._make_api_call('BatchWriteItem', {})

    assert resp['UnprocessedItems']['someTable'] == [{
        'PutRequest': {
            'Item': {
                'name': {STRING: 'daniel'},
                'picture': {BINARY: binary_blob}
            }
        }
    }]


def test_connection_update_time_to_live__fail():
    conn = Connection(REGION)
    with patch(PATCH_METHOD) as req:
        req.side_effect = BotoCoreError
        with pytest.raises(TableError):
            conn.update_time_to_live('test table', 'my_ttl')
