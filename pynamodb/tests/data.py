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


INDEX_TABLE_DATA = {
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
                "AttributeName": "numbers",
                "AttributeType": "NS"
            },
        ],
        "CreationDateTime": 1.363729002358E9,
        "ItemCount": 0,
        "KeySchema": [
            {
                "AttributeName": "user_name",
                "KeyType": "HASH"
            },
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "custom_idx_name",
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "KeySchema": [
                    {
                        "AttributeName": "email",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "numbers",
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
        "TableName": "IndexedModel",
        "TableSizeBytes": 0,
        "TableStatus": "ACTIVE"
    }
}


CUSTOM_ATTR_NAME_ITEM_DATA = {
    'Item': {
        'user_name': {
            'S': 'foo'
        },
        'user_id': {
            'S': 'bar'
        },
        'foo_attr': {
            'S': '2014-01-21T22:02:36.265046+0000'
        }
    }
}

CUSTOM_ATTR_NAME_INDEX_TABLE_DATA = {
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
        "LocalSecondaryIndexes": [
            {
                "IndexName": "uid_index",
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "KeySchema": [
                    {
                        "AttributeName": "user_id",
                        "KeyType": "HASH"
                    },
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
        "TableName": "CustomAttrModel",
        "TableSizeBytes": 0,
        "TableStatus": "ACTIVE"
    }
}


LOCAL_INDEX_TABLE_DATA = {
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
                "AttributeName": "numbers",
                "AttributeType": "NS"
            },
        ],
        "CreationDateTime": 1.363729002358E9,
        "ItemCount": 0,
        "KeySchema": [
            {
                "AttributeName": "user_name",
                "KeyType": "HASH"
            },
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "email_index",
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "KeySchema": [
                    {
                        "AttributeName": "email",
                        "KeyType": "HASH"
                    },
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
        "TableName": "LocalIndexedModel",
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
COMPLEX_ITEM_DATA = {
    "ConsumedCapacity": {
        "CapacityUnits": 1,
        "TableName": "Thread"
    },
    'Item': {
        'date_created': {
            'S': '2014-02-03T23:58:10.963333+0000'
        },
        'name': {
            'S': 'bar'
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


COMPLEX_TABLE_DATA = {
    'Table': {
        'ItemCount': 0, 'TableName': 'ComplexKey',
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 2,
            'WriteCapacityUnits': 2,
            'NumberOfDecreasesToday': 0
        },
        'CreationDateTime': 1391471876.86,
        'TableStatus': 'ACTIVE',
        'AttributeDefinitions': [
            {
                'AttributeName': 'date_created', 'AttributeType': 'S'
            },
            {
                'AttributeName': 'name', 'AttributeType': 'S'
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'name', 'KeyType': 'HASH'
            },
            {
                'AttributeName': 'date_created', 'KeyType': 'RANGE'
            }
        ],
        'TableSizeBytes': 0
    }
}
