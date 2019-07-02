from pynamodb.constants import ITEM, RESPONSES
from pynamodb.exceptions import TransactGetError
from pynamodb.models import _ModelFuture


class Transaction(object):

    """
    Base class for a type of transaction operation
    """

    _results = None

    def __init__(self, connection, return_consumed_capacity=None):
        self._connection = connection
        self._return_consumed_capacity = return_consumed_capacity

    @staticmethod
    def _get_error_code(error):
        return error.cause.response['Error'].get('Code')

    def _commit(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(exc_type, exc_val)
        if exc_type is None and exc_val is None and exc_tb is None:
            self._commit()


class TransactGet(Transaction):

    def __init__(self, *args, **kwargs):
        super(TransactGet, self).__init__(*args, **kwargs)
        self._get_items = []
        self._futures = []

    def get(self, model_cls, hash_key, range_key=None):
        """
        Adds the operation arguments for an item to list of models to get
        returns a _ModelFuture object as a placeholder

        :param model_cls:
        :param hash_key:
        :param range_key:
        :return:
        """
        operation_kwargs = model_cls.get_operation_kwargs_from_class(hash_key, range_key=range_key)
        model_future = _ModelFuture(model_cls)
        self._futures.append(model_future)
        self._get_items.append(operation_kwargs)
        return model_future

    def _update_futures(self):
        for model, data in zip(self._futures, self._results):
            model.update_with_raw_data(data[ITEM])

    def _cancel_futures(self):
        for future in self._futures:
            future._cancelled = True

    def _commit(self):
        try:
            response = self._connection.transact_get_items(
                get_items=self._get_items,
                return_consumed_capacity=self._return_consumed_capacity
            )
            self._results = response[RESPONSES]
            self._update_futures()
        except TransactGetError as exc:
            if self._get_error_code(exc) == 'TransactionCanceledException':
                self._cancel_futures()
            raise
        return response


class TransactWrite(Transaction):

    def __init__(self, client_request_token=None, return_item_collection_metrics=None, **kwargs):
        super(TransactWrite, self).__init__(**kwargs)
        self._client_request_token = client_request_token
        self._return_item_collection_metrics = return_item_collection_metrics
        self._condition_check_items = []
        self._delete_items = []
        self._put_items = []
        self._update_items = []
        self.response = None

    def condition_check(self, model_cls, hash_key, range_key=None, condition=None):
        if condition is None:
            raise TypeError('`condition` cannot be None')
        operation_kwargs = model_cls.get_operation_kwargs_from_class(
            hash_key,
            range_key=range_key,
            condition=condition
        )
        self._condition_check_items.append(operation_kwargs)

    def delete(self, model, condition=None):
        operation_kwargs = model.get_operation_kwargs_from_instance(condition=condition)
        self._delete_items.append(operation_kwargs)

    def save(self, model, condition=None, return_values=None):
        operation_kwargs = model.get_operation_kwargs_from_instance(
            key=ITEM,
            condition=condition,
            return_values_on_condition_failure=return_values
        )
        self._put_items.append(operation_kwargs)

    def update(self, model, actions, condition=None, return_values=None):
        operation_kwargs = model.get_operation_kwargs_from_instance(
            actions=actions,
            condition=condition,
            return_values_on_condition_failure=return_values
        )
        self._update_items.append(operation_kwargs)

    def _commit(self):
        return self._connection.transact_write_items(
            condition_check_items=self._condition_check_items,
            delete_items=self._delete_items,
            put_items=self._put_items,
            update_items=self._update_items,
            client_request_token=self._client_request_token,
            return_consumed_capacity=self._return_consumed_capacity,
            return_item_collection_metrics=self._return_item_collection_metrics,
        )
