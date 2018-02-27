# coding: utf-8
from hashlib import sha256

from amazondax import AmazonDaxClient


def get_connection_hash(endpoints):
    return sha256(''.join(endpoints).encode('utf-8')).hexdigest()


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

    _connections = {}

    def __init__(self, session, endpoints):
        self.session = session
        self.endpoints = endpoints

    @classmethod
    def _get_connection(cls, session, endpoints):
        connection_hash = get_connection_hash(endpoints)

        if connection_hash not in cls._connections.keys():
            cls._connections[connection_hash] = AmazonDaxClient(session, endpoints=endpoints)

        return cls._connections[connection_hash]

    def dispatch(self, operation_name, kwargs):
        method = getattr(self._get_connection(self.session, self.endpoints), self.OP_NAME_TO_METHOD[operation_name])
        return method(**kwargs)
