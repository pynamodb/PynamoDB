"""
Runs tests against dynamodb
"""
import time
import pprint
from pynamodb.connection import Connection
from pynamodb.types import STRING, NUMBER

conn = Connection()
table = conn.describe_table('pynamodb-ci')
if table is None:
    print("Table does not exist or is not active, creating...")
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
                'attribute_type': NUMBER,
                'attribute_name': 'ViewCount'
            },
            {
                'attribute_type': STRING,
                'attribute_name': 'AltKey'
            }
        ],
        'key_schema': [
            {
                'key_type': 'HASH',
                'attribute_name': 'Forum'
            },
            {
                'key_type': 'RANGE',
                'attribute_name': 'Thread'
            }
        ],
        'global_secondary_indexes': [
            {
                'index_name': 'alt-index',
                'key_schema': [
                    {
                        'key_type': 'HASH',
                        'attribute_name': 'AltKey'
                    }
                ]
            }
        ],
        'local_secondary_indexes': [
            {
                'index_name': 'view-index',
                'key_schema': [
                    {
                        'key_type': 'HASH',
                        'attribute_name': 'ViewCount'
                    }
                ]
            }
        ]
    }
    conn.create_table('pynamodb-ci', **params)
pprint.pprint(table)

