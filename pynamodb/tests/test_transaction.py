import pytest
from botocore.exceptions import BotoCoreError

from pynamodb.connection import transaction
from pynamodb.connection.transaction import Transaction, TRANSACT_ITEM_LIMIT, TransactGet, TransactWrite
from pynamodb.exceptions import GetError, UpdateError


class TestTransaction:

    def setup(self):
        self.transaction = Transaction()

    def test_initialize(self, mocker):
        mock_connection = mocker.patch.object(transaction, 'Connection')

        t = Transaction()
        mock_connection.assert_called_with()
        assert t._operation_kwargs == {}

        t = Transaction(return_consumed_capacity='TOTAL')
        mock_connection.assert_called_with()
        assert t._operation_kwargs == {'ReturnConsumedCapacity': 'TOTAL'}

        t = Transaction(region='us-east-1')
        mock_connection.assert_called_with(region='us-east-1')
        assert t._operation_kwargs == {}

    def test__len__(self):
        self.transaction._transact_items = []
        assert len(self.transaction) == 0

        self.transaction._transact_items = [{}, {}, {}]
        assert len(self.transaction) == 3

    def test_format_item(self):
        method = 'Foo'
        valid_parameters = ['a', 'c', 'e', 'ReturnValuesOnConditionCheckFailure']
        operation_kwargs = {'ReturnValues': 'ALL_OLD'}

        item = self.transaction.format_item(method, valid_parameters, operation_kwargs)
        assert item == {'Foo': {'ReturnValuesOnConditionCheckFailure': 'ALL_OLD'}}

        item = self.transaction.format_item(method, valid_parameters, {'a': 1, 'b': 2, 'c': 3})
        assert item == {'Foo': {'a': 1, 'c': 3}}

    def test_add_item(self):
        self.transaction.add_item({})
        assert len(self.transaction) == 1

        for _ in range(TRANSACT_ITEM_LIMIT - 1):
            self.transaction.add_item({})
        assert len(self.transaction) == TRANSACT_ITEM_LIMIT

        with pytest.raises(ValueError):
            self.transaction.add_item({})


class TestTransactGet:

    def test_commit(self, mocker):
        mock_dispatch = mocker.patch.object(transaction.Connection, 'dispatch')
        t = TransactGet()
        t.add_get_item({})

        t.commit()
        mock_dispatch.assert_called_once_with(
            'TransactGetItems', {
                'TransactItems': [
                    {'Get': {}}
                ]
            }
        )


class TestTransactWrite:

    def test_commit(self, mocker):
        mock_dispatch = mocker.patch.object(transaction.Connection, 'dispatch', return_value={
            'Responses': [{'Item': {}}]
        })
        t = TransactWrite()

        t.add_condition_check_item({})
        t.add_delete_item({})
        t.add_save_item({})
        t.add_update_item({})

        t.commit()
        mock_dispatch.assert_called_once_with(
            'TransactWriteItems', {
                'TransactItems': [
                    {'ConditionCheck': {}},
                    {'Delete': {}},
                    {'Put': {}},
                    {'Update': {}}
                ]
            }
        )
