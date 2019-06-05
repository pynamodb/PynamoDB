from pynamodb.exceptions import GetError

from pynamodb.constants import (
    RETURN_VALUES_ON_CONDITION_FAILURE, ITEM, RETURN_VALUES, RESPONSES,
    TRANSACTION_CONDITION_CHECK_REQUEST_PARAMETERS, TRANSACTION_DELETE_REQUEST_PARAMETERS,
    TRANSACTION_GET_REQUEST_PARAMETERS, TRANSACTION_PUT_REQUEST_PARAMETERS, TRANSACTION_UPDATE_REQUEST_PARAMETERS
)
from pynamodb.models import _ModelPromise


class Transaction(object):

    _connection = None
    _hashed_models = None
    _results = None
    _return_consumed_capacity = None

    def __init__(self, connection, return_consumed_capacity=None):
        self._connection = connection
        self._hashed_models = set()
        self._return_consumed_capacity = return_consumed_capacity

    def _commit(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._commit()

    def _hash_model(self, model, hash_key, range_key=None):
        key = (model.__class__, hash_key, range_key)
        if key in self._hashed_models:
            raise ValueError("Can't perform operation on the same entry multiple times in one transaction")
        self._hashed_models.add(key)

    @staticmethod
    def _format_request_parameters(valid_parameters, operation_kwargs):
        if RETURN_VALUES in operation_kwargs.keys():
            operation_kwargs[RETURN_VALUES_ON_CONDITION_FAILURE] = operation_kwargs.pop(RETURN_VALUES)
        return {
            key: value for key, value in operation_kwargs.items() if key in valid_parameters
        }


class TransactGet(Transaction):
    _get_items = None
    _proxy_models = None

    def __init__(self, *args, **kwargs):
        super(TransactGet, self).__init__(*args, **kwargs)
        self._get_items = []
        self._proxy_models = []

    def add_get_item(self, model_cls, hash_key, range_key, operation_kwargs):
        get_item = self._format_request_parameters(TRANSACTION_GET_REQUEST_PARAMETERS, operation_kwargs)
        proxy_model = _ModelPromise(model_cls)
        self._hash_model(model_cls, hash_key, range_key)
        self._proxy_models.append(proxy_model)
        self._get_items.append(get_item)
        return proxy_model

    def get_results_in_order(self):
        if self._results is None:
            raise GetError('Attempting to access item before committing the transaction')
        return self._proxy_models

    def _update_proxy_models(self):
        for model, data in zip(self._proxy_models, self._results):
            model.update_with_raw_data(data[ITEM])

    def _commit(self):
        response = self._connection.transact_get_items(
            get_items=self._get_items,
            return_consumed_capacity=self._return_consumed_capacity
        )
        self._results = response[RESPONSES]
        self._update_proxy_models()


class TransactWrite(Transaction):
    _condition_check_items = None
    _delete_items = None
    _put_items = None
    _update_items = None
    _client_request_token = None
    _return_item_collection_metrics = None

    def __init__(self, client_request_token=None, return_item_collection_metrics=None, **kwargs):
        super(TransactWrite, self).__init__(**kwargs)
        self._client_request_token = client_request_token
        self._return_item_collection_metrics = return_item_collection_metrics
        self._condition_check_items = []
        self._delete_items = []
        self._put_items = []
        self._update_items = []

    def add_condition_check_item(self, model_cls, hash_key, range_key, operation_kwargs):
        condition_item = self._format_request_parameters(TRANSACTION_CONDITION_CHECK_REQUEST_PARAMETERS, operation_kwargs)
        self._hash_model(model_cls(), hash_key, range_key)
        self._condition_check_items.append(condition_item)

    def add_delete_item(self, model, operation_kwargs):
        delete_item = self._format_request_parameters(TRANSACTION_DELETE_REQUEST_PARAMETERS, operation_kwargs)
        self._hash_model(model, model.get_hash_key(), model.get_range_key())
        self._delete_items.append(delete_item)

    def add_save_item(self, model, operation_kwargs):
        put_item = self._format_request_parameters(TRANSACTION_PUT_REQUEST_PARAMETERS, operation_kwargs)
        self._hash_model(model, model.get_hash_key(), model.get_range_key())
        self._put_items.append(put_item)

    def add_update_item(self, model, operation_kwargs):
        update_item = self._format_request_parameters(TRANSACTION_UPDATE_REQUEST_PARAMETERS, operation_kwargs)
        self._hash_model(model, model.get_hash_key(), model.get_range_key())
        self._update_items.append(update_item)

    def _commit(self):
        self._connection.transact_write_items(
            condition_check_items=self._condition_check_items,
            delete_items=self._delete_items,
            put_items=self._put_items,
            update_items=self._update_items,
            client_request_token=self._client_request_token,
            return_consumed_capacity=self._return_consumed_capacity,
            return_item_collection_metrics=self._return_item_collection_metrics,
        )
