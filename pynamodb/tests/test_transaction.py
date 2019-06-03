import pytest
import six

from pynamodb.connection import transactions
from pynamodb.connection.transactions import Transaction, TransactGet, TransactWrite
from pynamodb.constants import TRANSACT_ITEMS_LIMIT
from pynamodb.exceptions import GetError

if six.PY3:
    from unittest.mock import MagicMock
else:
    from mock import MagicMock


class TestTransaction:

    def setup(self):
        self.transaction = Transaction()

    def test_initialize(self, mocker):
        mock_connection = mocker.spy(transactions, '_get_connection')

        t = Transaction()
        mock_connection.assert_called_with(override_connection=False)
        assert t._operation_kwargs == {'TransactItems': []}

        t = Transaction(return_consumed_capacity='TOTAL')
        mock_connection.assert_called_with(override_connection=False)
        assert t._operation_kwargs == {'ReturnConsumedCapacity': 'TOTAL', 'TransactItems': []}

        t = Transaction(region='us-east-1')
        mock_connection.assert_called_with(override_connection=False, region='us-east-1')
        assert t._operation_kwargs == {'TransactItems': []}

    def test_commit__not_implemented(self):
        t = Transaction()
        with pytest.raises(NotImplementedError):
            t.commit()

    def test_format_item(self):
        method = 'Foo'
        valid_parameters = ['a', 'c', 'e', 'ReturnValuesOnConditionCheckFailure']
        operation_kwargs = {'ReturnValues': 'ALL_OLD'}

        item = self.transaction.format_item(method, valid_parameters, operation_kwargs)
        assert item == {'Foo': {'ReturnValuesOnConditionCheckFailure': 'ALL_OLD'}}

        item = self.transaction.format_item(method, valid_parameters, {'a': 1, 'b': 2, 'c': 3})
        assert item == {'Foo': {'a': 1, 'c': 3}}

    def test_hash_model__duplicate(self, mocker):
        mock_model = mocker.MagicMock()
        mock_model.__class__.__name__ = 'Mock'
        t = Transaction()
        t._hash_model(mock_model, 1, 2)
        with pytest.raises(ValueError):
            t._hash_model(mock_model, 1, 2)

    def test_add_item(self):
        self.transaction.add_item({})
        assert len(self.transaction.transact_items) == 1

        for _ in range(TRANSACT_ITEMS_LIMIT - 1):
            self.transaction.add_item({})
        assert len(self.transaction.transact_items) == TRANSACT_ITEMS_LIMIT

        # value error for hitting limit
        with pytest.raises(ValueError):
            self.transaction.add_item({})


class TestTransactGet:

    def setup(self):
        self.mock_model_cls = MagicMock(__name__='MockModel')

    def test_get_results_in_order__get_error(self):
        t = TransactGet()
        with pytest.raises(GetError):
            t.get_results_in_order()

        t._results = []
        assert t.get_results_in_order() == []

    def test_commit(self, mocker):
        mock_transaction = mocker.patch.object(transactions.Connection, 'transact_get_items', return_value={
            'Responses': [{'Item': {}}]
        })

        t = TransactGet()
        t.add_get_item(self.mock_model_cls, 1, 2, {})
        t.commit()

        mock_transaction.assert_called_once_with({'TransactItems': [{'Get': {}}]})


class TestTransactWrite:

    def test_initialize(self):
        t = TransactWrite(client_request_token='foo', return_item_collection_metrics='NONE')
        assert t._operation_kwargs == {
            'ClientRequestToken': 'foo',
            'ReturnItemCollectionMetrics': 'NONE',
            'TransactItems': [],
        }

    def test_validate_client_request_token(self):
        t = TransactWrite()
        with pytest.raises(ValueError):
            t._validate_client_request_token(123)

        with pytest.raises(ValueError):
            too_long = 'i' * 40
            assert len(too_long) == 40
            t._validate_client_request_token(too_long)

    def test_update_proxy_models(self, mocker):
        t = TransactWrite()
        mock_model = mocker.MagicMock()
        t._proxy_models = [mock_model for _ in range(5)]
        t._update_proxy_models()
        assert mock_model.refresh.call_count == 5

    def test_commit(self, mocker):
        mock_transaction = mocker.patch.object(transactions.Connection, 'transact_write_items')
        t = TransactWrite()

        t.add_condition_check_item(mocker.MagicMock(), 1, 2, {})
        t.add_delete_item(mocker.MagicMock(), {})
        t.add_save_item(mocker.MagicMock(), {})
        t.add_update_item(mocker.MagicMock(), {})

        t.commit()
        mock_transaction.assert_called_once_with({
            'TransactItems': [{'ConditionCheck': {}}, {'Delete': {}}, {'Put': {}}, {'Update': {}}]
        })


class TestConnection:

    def test_get_connection(self):
        transactions._CONNECTION = None
        assert transactions._CONNECTION is None
        conn = transactions._get_connection(host='foo', region='bar')
        assert conn.host == 'foo'
        assert conn.region == 'bar'
        assert transactions._CONNECTION is not None
        assert transactions._get_connection() == conn
