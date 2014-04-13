"""
Run tests against dynamodb using the table abstraction
"""
import time
import config as cfg
from pynamodb.constants import PROVISIONED_THROUGHPUT, READ_CAPACITY_UNITS
from pynamodb.connection import TableConnection
from pynamodb.types import STRING, HASH, RANGE, NUMBER

table_name = 'pynamodb-ci'

# For use with a fake dynamodb connection
# See: http://aws.amazon.com/dynamodb/developer-resources/
conn = TableConnection('pynamodb', host=cfg.DYNAMODB_HOST)
print(conn)

print("conn.describe_table...")
table = conn.describe_table()
if table is None:
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
    conn.create_table(**params)

while table is None:
    time.sleep(2)
    table = conn.describe_table()
while table['TableStatus'] == 'CREATING':
    time.sleep(5)
    print(table['TableStatus'])
    table = conn.describe_table()
print("conn.update_table...")

conn.update_table(
    read_capacity_units=table.get(PROVISIONED_THROUGHPUT).get(READ_CAPACITY_UNITS) + 1,
    write_capacity_units=2
)

table = conn.describe_table()
while table['TableStatus'] != 'ACTIVE':
    time.sleep(2)
    table = conn.describe_table()

print("conn.put_item")
conn.put_item(
    'item1-hash',
    range_key='item1-range',
    attributes={'foo': {'S': 'bar'}},
    expected={'Forum': {'Exists': False}}
)
conn.get_item(
    'item1-hash',
    range_key='item1-range'
)
conn.delete_item(
    'item1-hash',
    range_key='item1-range'
)

items = []
for i in range(10):
    items.append(
        {"Forum": "FooForum", "Thread": "thread-{0}".format(i)}
    )
print("conn.batch_write_items...")
conn.batch_write_item(
    put_items=items
)
print("conn.batch_get_items...")
data = conn.batch_get_item(
    items
)
print("conn.query...")
conn.query(
    "FooForum",
    key_conditions={'Thread': {'ComparisonOperator': 'BEGINS_WITH', 'AttributeValueList': ['thread']}}
)
print("conn.scan...")
conn.scan()
print("conn.delete_table...")
conn.delete_table()
