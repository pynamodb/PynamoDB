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
        "ItemCount": 42,
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

BINARY_ATTR_DATA = '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tACbkbUAioe0AI6NtQCcl7cAqp+5AM++yQDjv8sA7MrRAPPS1gDy0tMA8dLTAPHPzwD11tMA+eTmAPvl6gD/5/EA+ufwAPbf5wDd0NgAdXORAEJPfQBbb5IAT1uFAEtcgwBBV4EARFN6AEhVgQBMVYEAZ2mRAHtzogCRia4AmJC1AJSOtwBmZpQAfoOqAJqVtQCel7gAope/AKaVtgDgvsgA6cjMAOrJzQDwzs8A89HRAPjZ1gDrz9UA58vRAOfN0wDx1eIA4c3ZALuxzwBgZpEAR1qFAFFiiQBNXIoATViEAEdWhABEUoIAOkl3ADNAbAA+RHEAYF6MAFpXhQCUjbgAYGKSAGxpmwCAf6sAlY6zAJuTsQConb0AsqDHALaoxADjxcoA787SAPHR0gDx0M0A99HPAPvo6wD65usA/eXvAPjg6gDx2ugAfHekAEBJiABecZwAU2SLAExdhABGVoAARlWDAEJRfwBIUnoAS1d/AFRYiACHg60Am4y4AJOItgBfYJIAgn+sAJCIsACblLcAnZi1AKehugCjlMAAg3SuANK7ygDqyM4A68rOAO3OzQDw0c4A+dneAPTY3gDlyNEA2sPLAMq6zABdX6AAKkONAGJ6qABWaZQATV2CAEdafwBFVIIAR1GAAEdReQBIUXcAT1WCAIqHrgCgjrUAioCqAI2EtwCPhbQAlIy1AJ+YuwChnLsArqjBAIB8pQB/ebQAn5PDAOfFzwDpyMwA7szMAPDPzADty9EA7c7XAOjQ2ADn0NgAwbbXAFNdqgBIX5EAbZC4AGF4pQBMXYQAQ1Z7AENWfABBVHkAQVJ5AEhTeQBFUn4AjImwAJ2QtgChlLoAloy2AH58qgCFgK0AmJGyAKGcuQCuq8EAiIypAKuZwgBqYKYA1bnGAOTEyQDpycoA8M3KAOnHzQDpytMA59DYAOTN1QDKwtkAQlKUAEZeiABbeZwAWWuUAExbfABFWnkAQ1JzADtIbgBASnIAPU1yAD1QewCBhKoAlI2yAG5qkwBRWoAAdXmcAI6MsACblLUAo5ezAKejvAB5fZUAfn2dAG9lpwDdxM4A5sbLAO3NzgDx0s8A6cfOAN/AxwDo1NkA7dfjAO7a5wClqskAQlV2AFFggABOYIUASFZzACc0VAAaKz4AKzxRAC08XAA9SW0AQVF7AImFrgCXk7wARUpxADRCXgAWHzoALjVWAG9ulQCYjqwApaG6AG5viwBtfY4AwrG8AOHCyQDmx8oA783OAPLRzgDpx80A48LJAN7CyADlz9sA3cvYAHqFoAAxTGAASF1zAFFnigBEVHgASlh0AENVegA+UnUAN0hvADVIawBBUXsAVl6NAFxikQBGUH8AQE92AExUeQBgYIQAYF9/AIR+nQCfmLMAYWV9AGB6iADEtrwA1ru/AOHCxQDsysoA7svIAPHO0gDryc8A4sDGANy+yQDaw9IAWGmEACxDXQBAUm8AT116AEhTeQAzRWQAKzpaACc5VgAySmgATFh8AFtkjwBbYI0AYWSQAGBljABqa40AgHubAIeAowCbk7gAnpS4AJuUtQBYXHgAb4aWAKuruQDFs7QA0bm7AObHyADty8sA/NffAPnX3gDlx8wA0b7HAMq8xwBRXooAMEldADZMZQBOXX4ASV5+AEpahABIWYAATVmBAE1aiABma5gAcXajAGxulwBubp4AeXmpAI6JsACilcEApZO6AKKXvQCjl7sAj46uAFlheABddYEAhZ+tAMW0vQDWu78A48bJAOPHxgD0z9cA8c/WAOfH0gDaxdQAwrjIADxSfAAjPFAANkxlAE5aggBIW4EASFaGAEdWhABNV4YAVVuKAGhtmgBvdKEAcXGhAG9vnwB+fq4AhIOvAJqQuwCmlLsAo5i+AKSYvACEg6MAUmB2AE5ndwBzi58Asq60ANW6vgDUvsAA48HBAPbR1QDyzdUA68fTANy+yQC/tMQAR2aNABs7TgAqRVkASFt+AFFahQBGVoQAR1eFAEhXhQBYXo0AZGmWAG1rmQBycqAAd3mpAHh1pgCDgK4AlI2yAJ+TtwCpmsAAopW7AIWCogBbZnwATGJ7AGZ+lgCwp7EAz7W7ANK3uwDevr8A+tTaAP/d4wDy1+EA79rjANjY6ABPe6oAGj9hADFTawBBVXgATlyAAExZhQBRVYYAV1iKAF1ikwBhapYAZ2eVAGx3owB7eqYAfHmqAISBrwCflLwAqJa9AKWYvgChl7sAcXCQAEZXbABPbYAAZHyQALystwDEsLUAzLW5ANO7vQD70tkA887WAPrZ5wD22u0A1MrbAGuItQBPcp0AJUtuAChHaABBVHcAQFJ7AEVXgABMWYUATlmFAFJeiABYYYMAdXugAICDqQB/fqoAjYazAJGMuQCglb0AppS7AJ2WuQBVW3gAK0NbAEVccgBfd4sAu6q1AMKuswDFsLMA1Lm8APjP1wD0zdUA+dvmAPDW5gC4s8gAT2uUAFiFsQAgR24AEjdZABA4UQATNE4AITpUABk0TwAiOlgAGTRPACQ/WgAzTmkATll3AGtykwByep8AgYGlAI6NrwCQja0AX2SDAC1AVQAjQE8AM0thAHiElgC9qbUAv6uwAMKuswDRtrkA88rSAPHK0gDnydQA4sjYAH19oQBPbZwAKFF4AB1EagAbQGIACS5IAAgpPQAHKDsABSQzAAAaLgAAHjIABCU5AAQlPwALIz8AByI8AAkkPwAaLk0ALkBfADpIZQA6TGMAMkVaAC1CWAAuRlwAYnCCALWhrQC5pKwAvKitAL2oqwD20NYA8MjTAPXV4ADt0uIA39PlAFRvoQA9XY4AGDtmABQyVQAUL0oACSEzAAgmNwAHIzQAAx8wAAUZKgAAGSkACCI6AAAeNwADIjcABB0tAAYkQQAMKEYAIDtVACI6UAAiO08AN1BkAENRaABkdocAvKi1AMSvuADGrrYAxK+3APDH1gDwx9YA78vZAPLS5QDo1OcAj5i6AF+BvAApToAADjJaAAgmQQAAGCwAABktAAMhNAAAGy4AABgoAAkgNgAFIDoAFzNLAA4kPQAAEiYACipBAAQlOQAvSGgAM0tnAExkgABQaIAAWGJ6AKSgswDGrL4AzbC/AMyzvQDLsb4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='

SERIALIZED_TABLE_DATA = [
    [
        "foo",
        {
            "range_key": "id-0",
            "attributes": {
                "callable_field": {"N": "42"},
                "email": {"S": "email-7980"},
                "picture": {
                    "B": "aGVsbG8sIHdvcmxk"
                },
                "zip_code": {"N": "88030"}
            }
        }
    ],
    [
        "foo",
        {
            "range_key": "id-1",
            "attributes": {
                "callable_field": {"N": "42"},
                "email": {"S": "email-19770"},
                "picture": {
                    "B": "aGVsbG8sIHdvcmxk"
                },
                "zip_code": {"N": "88030"}
            }
        }
    ]
]

BOOLEAN_CONVERSION_MODEL_TABLE_DATA_OLD_STYLE = {
    'Table': {
        'ItemCount': 0, 'TableName': 'BooleanConversionTable',
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 2,
            'WriteCapacityUnits': 2,
            'NumberOfDecreasesToday': 0
        },
        'CreationDateTime': 1391471876.86,
        'TableStatus': 'ACTIVE',
        'AttributeDefinitions': [
            {
                'AttributeName': 'user_name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'is_human',
                'AttributeType': 'N'
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'user_name', 'KeyType': 'HASH'
            }
        ],
        'TableSizeBytes': 0
    }
}

BOOLEAN_CONVERSION_MODEL_TABLE_DATA = {
    'Table': {
        'ItemCount': 0, 'TableName': 'BooleanConversionTable',
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 2,
            'WriteCapacityUnits': 2,
            'NumberOfDecreasesToday': 0
        },
        'CreationDateTime': 1391471876.86,
        'TableStatus': 'ACTIVE',
        'AttributeDefinitions': [
            {
                'AttributeName': 'user_name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'is_human',
                'AttributeType': 'BOOL'
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'user_name', 'KeyType': 'HASH'
            }
        ],
        'TableSizeBytes': 0
    }
}

BOOLEAN_CONVERSION_MODEL_OLD_STYLE_TRUE_ITEM_DATA = {
    'Item': {
        'user_name': {
            'S': 'justin'
        },
        'is_human': {
            'N': '1'
        }
    }
}

BOOLEAN_CONVERSION_MODEL_OLD_STYLE_FALSE_ITEM_DATA = {
    'Item': {
        'user_name': {
            'S': 'alf'
        },
        'is_human': {
            'N': '0'
        }
    }
}

BOOLEAN_CONVERSION_MODEL_NEW_STYLE_TRUE_ITEM_DATA = {
    'Item': {
        'user_name': {
            'S': 'justin'
        },
        'is_human': {
            'BOOL': True
        }
    }
}

BOOLEAN_CONVERSION_MODEL_NEW_STYLE_FALSE_ITEM_DATA = {
    'Item': {
        'user_name': {
            'S': 'alf'
        },
        'is_human': {
            'BOOL': False
        }
    }
}
