import pytest
from pynamodb.attributes import NumberAttribute, UnicodeAttribute, VersionAttribute

from pynamodb.connection import Connection
from pynamodb.connection.base import MetaTable
from pynamodb.constants import TABLE_KEY
from pynamodb.transactions import Transaction, TransactGet, TransactWrite
from pynamodb.models import Model
from tests.test_base_connection import PATCH_METHOD

from unittest.mock import patch


class MockModel(Model):
    class Meta:
        table_name = 'mock'

    mock_hash = NumberAttribute(hash_key=True)
    mock_range = NumberAttribute(range_key=True)
    mock_toot = UnicodeAttribute(null=True)
    mock_version = VersionAttribute()


MOCK_TABLE_DESCRIPTOR = {
    "Table": {
        "TableName": "mock",
        "KeySchema": [
            {
                "AttributeName": "mock_hash",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "mock_range",
                "KeyType": "RANGE"
            }
        ],
        "AttributeDefinitions": [
            {
                "AttributeName": "mock_hash",
                "AttributeType": "N"
            },
            {
                "AttributeName": "mock_range",
                "AttributeType": "N"
            }
        ]
    }
}


class TestTransaction:

    def test_commit__not_implemented(self):
        t = Transaction(connection=Connection())
        with pytest.raises(NotImplementedError):
            t._commit()


class TestTransactGet:

    def test_commit(self, mocker):
        connection = Connection()
        connection.add_meta_table(MetaTable(MOCK_TABLE_DESCRIPTOR[TABLE_KEY]))

        mock_connection_transact_get = mocker.patch.object(connection, 'transact_get_items')

        with TransactGet(connection=connection) as t:
            t.get(MockModel, 1, 2)

        mock_connection_transact_get.assert_called_once_with(
            get_items=[{'Key': {'mock_hash': {'N': '1'}, 'mock_range': {'N': '2'}}, 'TableName': 'mock'}],
            return_consumed_capacity=None
        )


class TestTransactWrite:

    def test_condition_check__no_condition(self):
        with pytest.raises(TypeError):
            with TransactWrite(connection=Connection()) as transaction:
                transaction.condition_check(MockModel, hash_key=1, condition=None)

    def test_commit(self, mocker):
        connection = Connection()
        mock_connection_transact_write = mocker.patch.object(connection, 'transact_write_items')
        with TransactWrite(connection=connection) as t:
            t.condition_check(MockModel, 1, 3, condition=(MockModel.mock_hash.does_not_exist()))
            t.delete(MockModel(2, 4))
            t.save(MockModel(3, 5))
            t.update(MockModel(4, 6), actions=[MockModel.mock_toot.set('hello')], return_values='ALL_OLD')

        expected_condition_checks = [{
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_hash'},
            'Key': {'mock_hash': {'N': '1'}, 'mock_range': {'N': '3'}},
            'TableName': 'mock'}
        ]
        expected_deletes = [{
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_version'},
            'Key': {'mock_hash': {'N': '2'}, 'mock_range': {'N': '4'}},
            'TableName': 'mock'
        }]
        expected_puts = [{
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_version'},
            'Item': {'mock_hash': {'N': '3'}, 'mock_range': {'N': '5'}, 'mock_version': {'N': '1'}},
            'TableName': 'mock'
        }]
        expected_updates = [{
            'ConditionExpression': 'attribute_not_exists (#0)',
            'TableName': 'mock',
            'Key': {'mock_hash': {'N': '4'}, 'mock_range': {'N': '6'}},
            'ReturnValuesOnConditionCheckFailure': 'ALL_OLD',
            'UpdateExpression': 'SET #1 = :0, #0 = :1',
            'ExpressionAttributeNames': {'#0': 'mock_version', '#1': 'mock_toot'},
            'ExpressionAttributeValues': {':0': {'S': 'hello'}, ':1': {'N': '1'}}
        }]
        mock_connection_transact_write.assert_called_once_with(
            condition_check_items=expected_condition_checks,
            delete_items=expected_deletes,
            put_items=expected_puts,
            update_items=expected_updates,
            client_request_token=None,
            return_consumed_capacity=None,
            return_item_collection_metrics=None
        )
