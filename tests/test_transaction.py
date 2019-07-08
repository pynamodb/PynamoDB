import pytest
import six
from pynamodb.attributes import NumberAttribute

from pynamodb.connection import Connection
from pynamodb.connection.transactions import Transaction, TransactGet, TransactWrite
from pynamodb.models import Model
from tests.test_base_connection import PATCH_METHOD

if six.PY3:
    from unittest.mock import patch
else:
    from mock import patch


class MockModel(Model):
    class Meta:
        table_name = 'mock'

    mock_hash = NumberAttribute(hash_key=True)
    mock_range = NumberAttribute(range_key=True)


MOCK_TABLE_DESCRIPTOR = {
    "Table": {
        "TableName": "Mock",
        "KeySchema": [
            {
                "AttributeName": "MockHash",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "MockRange",
                "KeyType": "RANGE"
            }
        ],
        "AttributeDefinitions": [
            {
                "AttributeName": "MockHash",
                "AttributeType": "N"
            },
            {
                "AttributeName": "MockRange",
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
        mock_connection_transact_get = mocker.patch.object(connection, 'transact_get_items')

        with patch(PATCH_METHOD) as req:
            req.return_value = MOCK_TABLE_DESCRIPTOR
            with TransactGet(connection=connection) as t:
                t.get(MockModel, 1, 2)

        mock_connection_transact_get.assert_called_once_with(
            get_items=[{'Key': {'MockHash': {'N': '1'}, 'MockRange': {'N': '2'}}, 'TableName': 'mock'}],
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
        with patch(PATCH_METHOD) as req:
            req.return_value = MOCK_TABLE_DESCRIPTOR
            with TransactWrite(connection=connection) as t:
                t._condition_check_items = [{}]
                t._delete_items = [{}]
                t._put_items = [{}]
                t._update_items = [{}]

        mock_connection_transact_write.assert_called_once_with(
            condition_check_items=[{}],
            delete_items=[{}],
            put_items=[{}],
            update_items=[{}],
            client_request_token=None,
            return_consumed_capacity=None,
            return_item_collection_metrics=None
        )
