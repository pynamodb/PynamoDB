"""
Runs tests against dynamodb
"""
import time
from pynamodb.attributes import UnicodeAttribute

from pynamodb.connection import Connection
from pynamodb.connection.table import TableConnection
from pynamodb.constants import MAP, PROVISIONED_THROUGHPUT, READ_CAPACITY_UNITS
from pynamodb.expressions.condition import BeginsWith, NotExists
from pynamodb.expressions.operand import Path, Value
from pynamodb.exceptions import TableDoesNotExist
from pynamodb.expressions.update import Action, SetAction
from pynamodb.models import Model
from pynamodb.types import STRING, HASH, RANGE, NUMBER

import pytest


@pytest.mark.ddblocal
def test_connection_integration(ddb_url):
    table_name = 'pynamodb-ci-connection'

    # For use with a fake dynamodb connection
    # See: http://aws.amazon.com/dynamodb/developer-resources/
    conn = Connection(host=ddb_url)

    print(conn)
    print("conn.describe_table...")
    table = None
    try:
        table = conn.describe_table(table_name)
    except TableDoesNotExist:
        params = {
            'read_capacity_units': 1,
            'write_capacity_units': 1,
            'attribute_definitions': [
                {
                    'attribute_type': STRING,
                    'attribute_name': 'Forum'
                },
                {
                    'attribute_type': STRING,
                    'attribute_name': 'Thread'
                },
                {
                    'attribute_type': STRING,
                    'attribute_name': 'AltKey'
                },
                {
                    'attribute_type': NUMBER,
                    'attribute_name': 'number'
                }
            ],
            'key_schema': [
                {
                    'key_type': HASH,
                    'attribute_name': 'Forum'
                },
                {
                    'key_type': RANGE,
                    'attribute_name': 'Thread'
                }
            ],
            'global_secondary_indexes': [
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
                    }
                }
            ],
            'local_secondary_indexes': [
                {
                    'index_name': 'view-index',
                    'key_schema': [
                        {
                            'KeyType': 'HASH',
                            'AttributeName': 'Forum'
                        },
                        {
                            'KeyType': 'RANGE',
                            'AttributeName': 'AltKey'
                        }
                    ],
                    'projection': {
                        'ProjectionType': 'KEYS_ONLY'
                    }
                }
            ]
        }
        print("conn.create_table...")
        conn.create_table(table_name, **params)

    while table is None:
        time.sleep(1)
        table = conn.describe_table(table_name)

    while table['TableStatus'] == 'CREATING':
        time.sleep(2)
        table = conn.describe_table(table_name)
    print("conn.list_tables")
    conn.list_tables()
    print("conn.update_table...")

    conn.update_table(
        table_name,
        read_capacity_units=table.get(PROVISIONED_THROUGHPUT).get(READ_CAPACITY_UNITS) + 1,
        write_capacity_units=2
    )

    table = conn.describe_table(table_name)

    while table['TableStatus'] != 'ACTIVE':
        time.sleep(2)
        table = conn.describe_table(table_name)

    print("conn.put_item")
    conn.put_item(
        table_name,
        'item1-hash',
        range_key='item1-range',
        attributes={'foo': {'S': 'bar'}},
        condition=NotExists(Path('Forum')),
    )
    conn.get_item(
        table_name,
        'item1-hash',
        range_key='item1-range'
    )
    conn.delete_item(
        table_name,
        'item1-hash',
        range_key='item1-range'
    )

    items = []
    for i in range(10):
        items.append(
            {"Forum": "FooForum", "Thread": "thread-{}".format(i)}
        )
    print("conn.batch_write_items...")
    conn.batch_write_item(
        table_name,
        put_items=items
    )
    print("conn.batch_get_items...")
    data = conn.batch_get_item(
        table_name,
        items
    )
    print("conn.query...")
    conn.query(
        table_name,
        "FooForum",
        range_key_condition=(BeginsWith(Path('Thread'), Value('thread'))),
    )
    print("conn.scan...")
    conn.scan(
        table_name,
    )
    print("conn.delete_table...")
    conn.delete_table(table_name)


def test_conn_without_describe_table_called(ddb_url, table: str):
    conn = Connection(host=ddb_url)
    # conn.describe_table(table)  # bug: operations don't work without calling describe_table first

    conn.put_item(
        table,
        'item1-hash',
        attributes={'foo': {'S': 'bar'}},
    )
    get_response = conn.get_item(
        table,
        'item1-hash',
    )
    assert get_response.get('Item') == {'id': {'S': 'item1-hash'}, 'foo': {'S': 'bar'}}

    conn.update_item(
        table,
        'item1-hash',
        actions=[Path('foo').set("rab")]
    )

    get_response_after_update = conn.get_item(
        table,
        'item1-hash',
    )
    assert get_response_after_update.get('Item') == {'id': {'S': 'item1-hash'}, 'foo': {'S': 'rab'}}

    conn.delete_item(
        table,
        'item1-hash',
    )
    get_response_after_delete = conn.get_item(
        table,
        'item1-hash',
    )
    assert get_response_after_delete.get('Item') == None


@pytest.fixture
def table(ddb_url):
    table_name = 'pynamodb-ci-connection'

    conn = Connection(host=ddb_url)
    params = {
        'read_capacity_units': 1,
        'write_capacity_units': 1,
        'attribute_definitions': [
            {
                'attribute_type': STRING,
                'attribute_name': 'id'
            },
        ],
        'key_schema': [
            {
                'key_type': HASH,
                'attribute_name': 'id'
            }
        ],
    }
    conn.create_table(table_name, **params)
    for i in range(0,10):
        time.sleep(1)
        if conn.describe_table(table_name) is not None:
            break
        if i == 9:
            raise TimeoutError

    for i in range(0,10):
        time.sleep(1)
        if conn.describe_table(table_name)['TableStatus'] == 'ACTIVE':
            break
        if i == 9:
            raise TimeoutError

    yield table_name

    conn.delete_table(table_name)

