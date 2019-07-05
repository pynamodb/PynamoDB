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


class TestTransactGet:

    def setup(self):
        self.mock_model_cls = MagicMock(__name__='MockModel')
        self.mock_model_cls.get_operation_kwargs_from_class.return_value = {}

    def test_commit(self, mocker):
        mock_connection = mocker.MagicMock(spec=Connection)
        mock_connection.transact_get_items.return_value = {
            'Responses': [{'Item': {}}]
        }

        with TransactGet(connection=mock_connection) as t:
            t.get(self.mock_model_cls, 1, 2)

        mock_connection.transact_get_items.assert_called_once_with(get_items=[{}], return_consumed_capacity=None)


class TestTransactWrite:

    def test_condition_check__no_condition(self):
        with pytest.raises(TypeError):
            with TransactWrite(connection=MagicMock()) as transaction:
                transaction.condition_check(MagicMock(__name__='MockModel'), hash_key=1, condition=None)

    def test_commit(self, mocker):
        mock_connection = mocker.MagicMock(spec=Connection)
        with TransactWrite(connection=mock_connection) as t:
            t._condition_check_items = [{}]
            t._delete_items = [{}]
            t._put_items = [{}]
            t._update_items = [{}]

        mock_connection.transact_write_items.assert_called_once_with(
            condition_check_items=[{}],
            delete_items=[{}],
            put_items=[{}],
            update_items=[{}],
            client_request_token=None,
            return_consumed_capacity=None,
            return_item_collection_metrics=None
        )
