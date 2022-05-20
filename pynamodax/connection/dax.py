from typing import Dict, List

OP_WRITE = {
    'PutItem': 'put_item',
    'DeleteItem': 'delete_item',
    'UpdateItem': 'update_item',
    'BatchWriteItem': 'batch_write_item',
}

OP_READ = {
    'GetItem': 'get_item',
    'Scan': 'scan',
    'BatchGetItem': 'batch_get_item',
    'Query': 'query',
}

OP_NAME_TO_METHOD = OP_WRITE.copy()
OP_NAME_TO_METHOD.update(OP_READ)


class DaxClient(object):

    def __init__(self, endpoints: List[str], region_name: str):
        from amazondax import AmazonDaxClient

        self.connection = AmazonDaxClient(
            endpoints=endpoints,
            region_name=region_name
        )

    def dispatch(self, operation_name: str, operation_kwargs: Dict):
        method = getattr(self.connection, OP_NAME_TO_METHOD[operation_name])
        return method(**operation_kwargs)
