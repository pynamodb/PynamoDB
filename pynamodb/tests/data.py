"""
Test fixtures
"""

LIST_TABLE_DATA = {
    "LastEvaluatedTableName": "Thread",
    "TableNames": ["Forum", "Reply", "Thread"]
}
SIMPLE_MODEL_TABLE_DATA = {
    "Table": {
        "AttributeDefinitions": [
            {
                "AttributeName": "user_name",
                "AttributeType": "S"
            },
            {
                "AttributeName": "email",
                "AttributeType": "S"
            },
        ],
        "CreationDateTime": 1.363729002358E9,
        "ItemCount": 0,
        "KeySchema": [
            {
                "AttributeName": "user_name",
                "KeyType": "HASH"
            }
        ],
        "ProvisionedThroughput": {
            "NumberOfDecreasesToday": 0,
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        },
        "TableName": "SimpleModel",
        "TableSizeBytes": 0,
        "TableStatus": "ACTIVE"
    }
}

MODEL_TABLE_DATA = {
    "Table": {
        "AttributeDefinitions": [
            {
                "AttributeName": "user_name",
                "AttributeType": "S"
            },
            {
                "AttributeName": "email",
                "AttributeType": "S"
            },
            {
                "AttributeName": "user_id",
                "AttributeType": "S"
            },
            {
                "AttributeName": "picture",
                "AttributeType": "B"
            },
            {
                "AttributeName": "zip_code",
                "AttributeType": "N"
            }
        ],
        "CreationDateTime": 1.363729002358E9,
        "ItemCount": 0,
        "KeySchema": [
            {
                "AttributeName": "user_name",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "user_id",
                "KeyType": "RANGE"
            }
        ],
        "ProvisionedThroughput": {
            "NumberOfDecreasesToday": 0,
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        },
        "TableName": "Thread",
        "TableSizeBytes": 0,
        "TableStatus": "ACTIVE"
    }
}

DESCRIBE_TABLE_DATA = {
    "Table": {
        "AttributeDefinitions": [
            {
                "AttributeName": "ForumName",
                "AttributeType": "S"
            },
            {
                "AttributeName": "LastPostDateTime",
                "AttributeType": "S"
            },
            {
                "AttributeName": "Subject",
                "AttributeType": "S"
            }
        ],
        "CreationDateTime": 1.363729002358E9,
        "ItemCount": 0,
        "KeySchema": [
            {
                "AttributeName": "ForumName",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "Subject",
                "KeyType": "RANGE"
            }
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "LastPostIndex",
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "KeySchema": [
                    {
                        "AttributeName": "ForumName",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "LastPostDateTime",
                        "KeyType": "RANGE"
                    }
                ],
                "Projection": {
                    "ProjectionType": "KEYS_ONLY"
                }
            }
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "LastPostIndex",
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "KeySchema": [
                    {
                        "AttributeName": "ForumName",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "LastPostDateTime",
                        "KeyType": "RANGE"
                    }
                ],
                "Projection": {
                    "ProjectionType": "KEYS_ONLY"
                }
            }
        ],
        "ProvisionedThroughput": {
            "NumberOfDecreasesToday": 0,
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        },
        "TableName": "Thread",
        "TableSizeBytes": 0,
        "TableStatus": "ACTIVE"
    }
}

GET_MODEL_ITEM_DATA = {
    'Item': {
        'user_name': {
            'S': 'foo'
        },
        'user_id': {
            'S': 'bar'
        },
        'zip_code': {
            'N': '88030'
        },
        'epoch': {
            'S': '2014-01-21T22:02:36.265046+0000'
        }
    }
}

GET_ITEM_DATA = {
    "ConsumedCapacity": {
        "CapacityUnits": 1,
        "TableName": "Thread"
    },
    "Item": {
        "Tags": {
            "SS": ["Update", "Multiple Items", "HelpMe"]
        },
        "LastPostDateTime": {
            "S": "201303190436"
        },
        "Message": {
            "S": "I want to update multiple items in a single API call. What's the best way to do that?"
        }
    }
}
SIMPLE_BATCH_GET_ITEMS = {
    'UnprocessedKeys': {},
    'Responses': {
        'SimpleModel': [
            {'user_name': {'S': '9'}, 'epoch': {'S': '2014-01-22T15:06:48.151712+0000'}, 'user_id': {'S': 'hash-9'}},
            {'user_name': {'S': '4'}, 'epoch': {'S': '2014-01-22T15:06:48.150765+0000'}, 'user_id': {'S': 'hash-4'}},
            {'user_name': {'S': '3'}, 'epoch': {'S': '2014-01-22T15:06:48.150574+0000'}, 'user_id': {'S': 'hash-3'}},
            {'user_name': {'S': '2'}, 'epoch': {'S': '2014-01-22T15:06:48.150382+0000'}, 'user_id': {'S': 'hash-2'}},
            {'user_name': {'S': '1'}, 'epoch': {'S': '2014-01-22T15:06:48.150190+0000'}, 'user_id': {'S': 'hash-1'}},
            {'user_name': {'S': '8'}, 'epoch': {'S': '2014-01-22T15:06:48.151523+0000'}, 'user_id': {'S': 'hash-8'}},
            {'user_name': {'S': '7'}, 'epoch': {'S': '2014-01-22T15:06:48.151334+0000'}, 'user_id': {'S': 'hash-7'}},
            {'user_name': {'S': '6'}, 'epoch': {'S': '2014-01-22T15:06:48.151145+0000'}, 'user_id': {'S': 'hash-6'}},
            {'user_name': {'S': '5'}, 'epoch': {'S': '2014-01-22T15:06:48.150957+0000'}, 'user_id': {'S': 'hash-5'}},
            {'user_name': {'S': '0'}, 'epoch': {'S': '2014-01-22T15:06:48.149991+0000'}, 'user_id': {'S': 'hash-0'}}
        ]
    }
}
BATCH_GET_ITEMS = {
    'UnprocessedKeys': {},
    'Responses': {
        'UserModel': [
            {'user_name': {'S': '9'}, 'epoch': {'S': '2014-01-22T15:06:48.151712+0000'}, 'user_id': {'S': 'hash-9'}},
            {'user_name': {'S': '4'}, 'epoch': {'S': '2014-01-22T15:06:48.150765+0000'}, 'user_id': {'S': 'hash-4'}},
            {'user_name': {'S': '3'}, 'epoch': {'S': '2014-01-22T15:06:48.150574+0000'}, 'user_id': {'S': 'hash-3'}},
            {'user_name': {'S': '2'}, 'epoch': {'S': '2014-01-22T15:06:48.150382+0000'}, 'user_id': {'S': 'hash-2'}},
            {'user_name': {'S': '1'}, 'epoch': {'S': '2014-01-22T15:06:48.150190+0000'}, 'user_id': {'S': 'hash-1'}},
            {'user_name': {'S': '8'}, 'epoch': {'S': '2014-01-22T15:06:48.151523+0000'}, 'user_id': {'S': 'hash-8'}},
            {'user_name': {'S': '7'}, 'epoch': {'S': '2014-01-22T15:06:48.151334+0000'}, 'user_id': {'S': 'hash-7'}},
            {'user_name': {'S': '6'}, 'epoch': {'S': '2014-01-22T15:06:48.151145+0000'}, 'user_id': {'S': 'hash-6'}},
            {'user_name': {'S': '5'}, 'epoch': {'S': '2014-01-22T15:06:48.150957+0000'}, 'user_id': {'S': 'hash-5'}},
            {'user_name': {'S': '0'}, 'epoch': {'S': '2014-01-22T15:06:48.149991+0000'}, 'user_id': {'S': 'hash-0'}}
        ]
    }
}
