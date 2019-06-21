import pytest
import six

from pynamodb.connection import Connection
from pynamodb.connection.transactions import Transaction, TransactGet, TransactWrite

if six.PY3:
    from unittest.mock import MagicMock
else:
    from mock import MagicMock


class TestTransaction:

    def test_commit__not_implemented(self, mocker):
        t = Transaction(connection=mocker.MagicMock())
        with pytest.raises(NotImplementedError):
            t._commit()

    def test_format_request_parameters(self, mocker):
        transaction = Transaction(connection=mocker.MagicMock())
        valid_parameters = {'a', 'c', 'e', 'ReturnValuesOnConditionCheckFailure'}
        operation_kwargs = {'ReturnValues': 'ALL_OLD'}

        item = transaction._format_request_parameters(valid_parameters, operation_kwargs)
        assert item == {'ReturnValuesOnConditionCheckFailure': 'ALL_OLD'}

        item = transaction._format_request_parameters(valid_parameters, {'a': 1, 'b': 2, 'c': 3})
        assert item == {'a': 1, 'c': 3}


class TestTransactGet:

    def setup(self):
        self.mock_model_cls = MagicMock(__name__='MockModel')

    def test_commit(self, mocker):
        mock_connection = mocker.MagicMock(spec=Connection)
        mock_connection.transact_get_items.return_value = {
            'Responses': [{'Item': {}}]
        }

        with TransactGet(connection=mock_connection) as t:
            t.get(self.mock_model_cls, 1, 2)

        mock_connection.transact_get_items.assert_called_once_with(get_items=[{}], return_consumed_capacity=None)


class TestTransactWrite:

    def test_commit(self, mocker):
        mock_connection = mocker.MagicMock(spec=Connection)
        with TransactWrite(connection=mock_connection) as t:
            t.add_condition_check_item({})
            t.add_delete_item({})
            t.add_save_item({})
            t.add_update_item({})

        mock_connection.transact_write_items.assert_called_once_with(
            condition_check_items=[{}],
            delete_items=[{}],
            put_items=[{}],
            update_items=[{}],
            client_request_token=None,
            return_consumed_capacity=None,
            return_item_collection_metrics=None
        )
