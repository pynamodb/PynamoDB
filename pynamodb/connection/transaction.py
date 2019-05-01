from pynamodb.connection.base import Connection
from pynamodb.constants import (
    TABLE_NAME, PROJECTION_EXPRESSION, RETURN_CONSUMED_CAPACITY, UPDATE, GET, PUT, DELETE,
    TRANSACT_GET_ITEMS, TRANSACT_WRITE_ITEMS, TRANSACT_ITEMS, CLIENT_REQUEST_TOKEN, CONDITION_CHECK,
    RETURN_ITEM_COLL_METRICS, CONDITION_EXPRESSION, EXPRESSION_ATTRIBUTE_NAMES, EXPRESSION_ATTRIBUTE_VALUES,
    UPDATE_EXPRESSION, RETURN_VALUES_ON_CONDITION_FAILURE, ITEM, KEY,
    RETURN_VALUES, RESPONSES
)

PUT = PUT.lower().capitalize()
DELETE = DELETE.lower().capitalize()

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


class Transaction(object):

    _connection = None
    _item_limit = TRANSACT_ITEM_LIMIT
    _method = None
    _operation_kwargs = None

    def __init__(self, return_consumed_capacity=None, **connection_kwargs):
        self._operation_kwargs = {
            TRANSACT_ITEMS: [],
        }
        self._connection = Connection(**connection_kwargs)
        if return_consumed_capacity is not None:
            self._operation_kwargs.update(self._connection.get_consumed_capacity_map(return_consumed_capacity))

    def __len__(self):
        return len(self.transact_items)

    @property
    def transact_items(self):
        return self._operation_kwargs[TRANSACT_ITEMS]

    @staticmethod
    def format_item(method, valid_parameters, operation_kwargs):
        if RETURN_VALUES in operation_kwargs.keys():
            operation_kwargs[RETURN_VALUES_ON_CONDITION_FAILURE] = operation_kwargs.pop(RETURN_VALUES)
        request_params = {
            key: value for key, value in operation_kwargs.items() if key in valid_parameters
        }
        return {method: request_params}

    def add_item(self, item):
        if len(self) >= self._item_limit:
            raise ValueError("Transaction can't support more than {0} items".format(self._item_limit))
        self.transact_items.append(item)


class TransactGet(Transaction):

    _models = None

    def add_get_item(self, model_cls, operation_kwargs):
        get_item = self.format_item(GET, GET_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(get_item)
        self._add_item_class(model_cls)

    def _add_item_class(self, model_cls):
        if self._models is None:
            self._models = []
        self._models.append(model_cls)

    def commit(self):  # why isnt this hitting
        items = self._connection.transact_get_items(self._operation_kwargs)[RESPONSES]
        # the items are returned in the same order as the original transact_items request list
        for model, item in zip(self._models, items):
            yield model.from_raw_data(item[ITEM])


class TransactWrite(Transaction):

    def __init__(self, client_request_token=None, return_item_collection_metrics=None, **kwargs):
        super(TransactWrite, self).__init__(**kwargs)
        if client_request_token is not None:
            self._operation_kwargs[CLIENT_REQUEST_TOKEN] = client_request_token
        if return_item_collection_metrics is not None:
            self._operation_kwargs.update(self._connection.get_item_collection_map(return_item_collection_metrics))

    def add_condition_check_item(self, operation_kwargs):
        condition_item = self.format_item(CONDITION_CHECK, CONDITION_CHECK_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(condition_item)

    def add_delete_item(self, operation_kwargs):
        delete_item = self.format_item(DELETE, DELETE_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(delete_item)

    def add_save_item(self, operation_kwargs):
        put_item = self.format_item(PUT, PUT_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(put_item)

    def add_update_item(self, operation_kwargs):
        update_item = self.format_item(UPDATE, UPDATE_REQUEST_PARAMETERS, operation_kwargs)
        self.add_item(update_item)

    def commit(self):
        return self._connection.transact_write_items(self._operation_kwargs)
