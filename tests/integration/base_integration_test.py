"""
Runs tests against dynamodb
"""
import time

from pynamodb.connection import Connection
from pynamodb.constants import PROVISIONED_THROUGHPUT, READ_CAPACITY_UNITS
from pynamodb.expressions.condition import BeginsWith, NotExists
from pynamodb.expressions.operand import Path, Value
from pynamodb.exceptions import TableDoesNotExist
from pynamodb.types import STRING, HASH, RANGE, NUMBER

import pytest


@pytest.mark.ddblocal
def test_connection_integration(ddb_url):
    table_name = 'pynamodb-ci-connection'

    # For use with a fake dynamodb connection
    # See: http://aws.amazon.com/dynamodb/developer-resources/
    conn = Connection(host=ddb_url)

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
        conn.create_table(table_name, **params)

    while table is None:
        time.sleep(0.1)
        table = conn.describe_table(table_name)

    while table['TableStatus'] == 'CREATING':
        time.sleep(0.1)
        table = conn.describe_table(table_name)
    conn.list_tables()

    conn.update_table(
        table_name,
        read_capacity_units=table.get(PROVISIONED_THROUGHPUT).get(READ_CAPACITY_UNITS) + 1,
        write_capacity_units=2
    )

    table = conn.describe_table(table_name)

    while table['TableStatus'] != 'ACTIVE':
        time.sleep(2)
        table = conn.describe_table(table_name)

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
    conn.batch_write_item(
        table_name,
        put_items=items
    )
    conn.batch_get_item(
        table_name,
        items
    )
    conn.query(
        table_name,
        "FooForum",
        range_key_condition=(BeginsWith(Path('Thread'), Value('thread'))),
    )
    conn.scan(
        table_name,
    )
    conn.delete_table(table_name)


@pytest.mark.ddblocal
def test_connection_integration__describe_as_needed(ddb_url):
    table_name = 'pynamodb-ci-connection-describe-as-needed'

    conn = Connection(host=ddb_url)

    conn.create_table(
        table_name,
        read_capacity_units=1,
        write_capacity_units=1,
        attribute_definitions=[
            {
                'attribute_type': NUMBER,
                'attribute_name': 'foo'
            }
        ],
        key_schema=[
            {
                'key_type': HASH,
                'attribute_name': 'foo'
            },
        ],
    )

    # we do not call describe_table here, and expect it to be done for us

    try:
        conn.put_item(table_name, '123', attributes={'bar': {'S': 'baz'}})
        resp = conn.get_item(table_name, '123')
        assert resp['Item']['bar']['S'] == 'baz'
    finally:
        conn.delete_table(table_name)
