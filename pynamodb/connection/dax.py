# coding: utf-8
from amazondax import AmazonDaxClient


class DaxClient(object):

    OP_NAME_TO_METHOD = {
        'GetItem': 'get_item',
        'PutItem': 'put_item',
        'DeleteItem': 'delete_item',
        'UpdateItem': 'update_item',
        'Query': 'query',
        'Scan': 'scan',
        'BatchGetItem': 'batch_get_item',
        'BatchWriteItem': 'batch_write_item',
    }

    def __init__(self, session, endpoints):
        self.connection = AmazonDaxClient(session, endpoints=endpoints)

    def dispatch(self, operation_name, kwargs):
        method = getattr(self.connection, self.OP_NAME_TO_METHOD[operation_name])
        return method(**kwargs)
