from typing import Tuple, TypeVar, Type, Any, List, Optional, Dict, Union, Text

from pynamodb.expressions.condition import Condition
from pynamodb.models import Model, _ModelFuture

from pynamodb.connection import Connection


KeyType = Union[Text, bytes, float, int, Tuple]
ModelType = TypeVar('ModelType', bound=Model)

class Transaction:
    _connection: Connection
    _return_consumed_capacity: Optional[Any]

    def __enter__(self) -> Transaction: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> Any: ...
    def __init__(self, connection: Connection, return_consumed_capacity: Optional[Any] = ...) -> None: ...
    def _commit(self) -> Any: ...

class TransactGet(Transaction):
    _get_items: List[Dict]
    _futures = List[_ModelFuture]
    _results: List[Dict]

    def _update_futures(self) -> None: ...
    def get(self, model_cls: Type[ModelType], hash_key: KeyType, range_key: Optional[KeyType] = ...) -> ModelType: ...

class TransactWrite(Transaction):
    _condition_check_items: List[Dict]
    _delete_items: List[Dict]
    _put_items: List[Dict]
    _update_items: List[Dict]
    _client_request_token: Optional[str]
    _return_item_collection_metrics: Optional[Any]

    def __int__(self, connection: Connection, return_consumed_capacity: Optional[Any] = ..., client_request_token: Optional[Text] = ..., return_item_collection_metrics: Optional[Text] = ...) -> None: ...
    def condition_check(self, model_cls: Type[ModelType], hash_key: KeyType, range_key: Optional[KeyType] = ..., condition: Condition = ...) -> None: ...
    def delete(self, model: ModelType, condition: Optional[Condition] = ...) -> None: ...
    def save(self, model: ModelType, condition: Optional[Condition] = ..., return_values: Optional[Any] = ...) -> None: ...
    def update(self, model: ModelType, actions: List[Any], condition: Optional[Condition] = ..., return_values: Optional[Any] = ...) -> None: ...
