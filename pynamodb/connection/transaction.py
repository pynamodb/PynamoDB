from pynamodb.connection.base import BOTOCORE_EXCEPTIONS, Connection
from pynamodb.constants import (
    TABLE_NAME, PROJECTION_EXPRESSION, GET, PUT, UPDATE, DELETE, RETURN_CONSUMED_CAPACITY,
    TRANSACT_GET_ITEMS, TRANSACT_WRITE_ITEMS, TRANSACT_ITEMS, CLIENT_REQUEST_TOKEN, CONDITION_CHECK,
    RETURN_ITEM_COLL_METRICS, CONDITION_EXPRESSION, EXPRESSION_ATTRIBUTE_NAMES, EXPRESSION_ATTRIBUTE_VALUES,
    UPDATE_EXPRESSION, RETURN_VALUES_ON_CONDITION_FAILURE, ITEM, KEY,
    RETURN_VALUES)
from pynamodb.exceptions import GetError
from pynamodb.expressions.update import Update

TRANSACT_ITEM_LIMIT = 10

CONDITION_CHECK_REQUEST_PARAMETERS = {
    CONDITION_EXPRESSION,
    EXPRESSION_ATTRIBUTE_NAMES,
    EXPRESSION_ATTRIBUTE_VALUES,
    KEY,
    RETURN_VALUES_ON_CONDITION_FAILURE,
    TABLE_NAME,
}

DELETE_REQUEST_PARAMETERS = {
    CONDITION_EXPRESSION,
    EXPRESSION_ATTRIBUTE_NAMES,
    EXPRESSION_ATTRIBUTE_VALUES,
    KEY,
    RETURN_VALUES_ON_CONDITION_FAILURE,
    TABLE_NAME,
}

GET_REQUEST_PARAMETERS = {
    KEY,
    TABLE_NAME,
    EXPRESSION_ATTRIBUTE_NAMES,
    EXPRESSION_ATTRIBUTE_VALUES,
    PROJECTION_EXPRESSION,
}

PUT_REQUEST_PARAMETERS = {
    CONDITION_EXPRESSION,
    EXPRESSION_ATTRIBUTE_NAMES,
    EXPRESSION_ATTRIBUTE_VALUES,
    ITEM,
    RETURN_VALUES_ON_CONDITION_FAILURE,
    TABLE_NAME,
}

UPDATE_REQUEST_PARAMETERS = {
    CONDITION_EXPRESSION,
    EXPRESSION_ATTRIBUTE_NAMES,
    EXPRESSION_ATTRIBUTE_VALUES,
    KEY,
    RETURN_VALUES_ON_CONDITION_FAILURE,
    TABLE_NAME,
    UPDATE_EXPRESSION,
}


class Transaction:

    _item_limit = TRANSACT_ITEM_LIMIT
    _method = None
    _operation_kwargs = {}
    _transact_items = []

    def __init__(self, return_consumed_capacity=None, **connection_kwargs):
        if return_consumed_capacity is not None:
            self._operation_kwargs[RETURN_CONSUMED_CAPACITY] = return_consumed_capacity
        self._connection = Connection(**connection_kwargs)

    def __len__(self):
        return len(self._transact_items)

    @property
    def transact_items(self):
        return self._transact_items

    @staticmethod
    def format_item(method, valid_parameters, operation_kwargs):
        request_params = {
            key: value for key, value in operation_kwargs if key in valid_parameters
        }
        if RETURN_VALUES in operation_kwargs.keys() and RETURN_VALUES_ON_CONDITION_FAILURE in valid_parameters:
            request_params[RETURN_VALUES_ON_CONDITION_FAILURE] = operation_kwargs.pop(RETURN_VALUES)
        return {method: request_params}

    def add_item(self, item):
        if len(self) >= self._item_limit:
            raise ValueError("Transaction can't support more than {0} items".format(self._item_limit))
        self._transact_items.append(item)

    def commit(self):
        self._operation_kwargs[TRANSACT_ITEMS] = self.transact_items
        try:
            return self._connection.dispatch(self._method, self._operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise GetError("Failed to batch get items: {0}".format(e), e)


class TransactGet(Transaction):

    _method = TRANSACT_GET_ITEMS

    def add_get_item(self, operation_kwargs):
        get_item = self.format_item(GET, GET_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(get_item)


class TransactWrite(Transaction):

    _method = TRANSACT_WRITE_ITEMS

    def __init__(self, client_request_token=None, return_item_collection_metrics=None, **kwargs):
        if client_request_token is not None:
            self._operation_kwargs[CLIENT_REQUEST_TOKEN] = client_request_token
        if return_item_collection_metrics is not None:
            self._operation_kwargs[RETURN_ITEM_COLL_METRICS] = return_item_collection_metrics
        super().__init__(**kwargs)

    def add_save_item(self, operation_kwargs):
        put_item = self.format_item(PUT, PUT_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(put_item)

    def add_update_item(self, operation_kwargs):
        update_item = self.format_item(UPDATE, UPDATE_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(update_item)

    def add_delete_item(self, operation_kwargs):
        delete_item = self.format_item(DELETE, DELETE_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(delete_item)

    def add_condition_check_item(self, operation_kwargs):
        condition_item = self.format_item(CONDITION_CHECK, CONDITION_CHECK_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(condition_item)
