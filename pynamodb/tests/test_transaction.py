import pytest
from pynamodb.models import Model

from pynamodb.connection import transaction
from pynamodb.connection.transaction import Transaction, TRANSACT_ITEM_LIMIT, TransactGet, TransactWrite


class TestTransaction:

    def setup(self):
        self.transaction = Transaction()

    def test_initialize(self, mocker):
        mock_connection = mocker.spy(transaction, 'Connection')

        t = Transaction()
        mock_connection.assert_called_with()
        assert t._operation_kwargs == {'TransactItems': []}

        t = Transaction(return_consumed_capacity='TOTAL')
        mock_connection.assert_called_with()
        assert t._operation_kwargs == {'ReturnConsumedCapacity': 'TOTAL', 'TransactItems': []}

        t = Transaction(region='us-east-1')
        mock_connection.assert_called_with(region='us-east-1')
        assert t._operation_kwargs == {'TransactItems': []}

    def test__len__(self):
        self.transaction._operation_kwargs['TransactItems'] = []
        assert len(self.transaction) == 0

        self.transaction._operation_kwargs['TransactItems'] = [{}, {}, {}]
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

    def test_add_item_class(self, mocker):
        t = TransactGet()
        assert t._models is None

        t._add_item_class(mocker.MagicMock(spec=Model))
        assert t._models is not None
        assert len(t._models) == 1

    def test_commit(self, mocker):
        mock_transaction = mocker.patch.object(transaction.Connection, 'transact_get_items', return_value={
            'Responses': [{'Item': {}}]
        })

        t = TransactGet()
        t.add_get_item(mocker.MagicMock(spec=Model), {})
        next(t.commit())

        mock_transaction.assert_called_once_with({'TransactItems': [{'Get': {}}]})


class TestTransactWrite:

    def test_initialize(self):
        t = TransactWrite(client_request_token='foo', return_item_collection_metrics='NONE')
        assert t._operation_kwargs == {
            'ClientRequestToken': 'foo',
            'ReturnItemCollectionMetrics': 'NONE',
            'TransactItems': [],
        }

    def test_commit(self, mocker):
        mock_transaction = mocker.patch.object(transaction.Connection, 'transact_write_items')
        t = TransactWrite()

        t.add_condition_check_item({})
        t.add_delete_item({})
        t.add_save_item({})
        t.add_update_item({})

        t.commit()
        mock_transaction.assert_called_once_with({
            'TransactItems': [{'ConditionCheck': {}}, {'Delete': {}}, {'Put': {}}, {'Update': {}}]
        })
