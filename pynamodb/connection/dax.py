from amazondax import AmazonDaxClient


OP_WRITE = {
    'PutItem': 'put_item',
    'DeleteItem': 'delete_item',
    'UpdateItem': 'update_item',
    'BatchWriteItem': 'batch_write_item',
    'TransactWriteItems': 'transact_write_items',

}

OP_READ = {
    'GetItem': 'get_item',
    'BatchGetItem': 'batch_get_item',
    # query and scan has a serious consistency issue
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.consistency.html#DAX.consistency.query-cache
    # 'Query': 'query',
    # 'Scan': 'scan',
    'TransactGetItems': 'transact_get_items',
}

OP_NAME_TO_METHOD = OP_WRITE.copy()
OP_NAME_TO_METHOD.update(OP_READ)


class DaxClient(object):

    def __init__(self, endpoints, region_name):
        self.connection = AmazonDaxClient(
            endpoints=endpoints,
            region_name=region_name
        )

    def dispatch(self, operation_name, kwargs):
        method = getattr(self.connection, OP_NAME_TO_METHOD[operation_name])
        return method(**kwargs)
