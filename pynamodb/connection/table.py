"""
Connection classes
"""
from .base import Connection


class TableConnection(object):
    """
    A higher level abstraction over botocore
    """

    def __init__(self, table_name, region=None):
        self._hash_keyname = None
        self._range_keyname = None
        self.table_name = table_name
        self.connection = Connection(region=region)

    def delete_item(self, hash_key,
                    range_key=None,
                    expected=None,
                    return_values=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=None):
        """
        Performs the DeleteItem operation and returns the result
        """
        return self.connection.delete_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            expected=expected,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    def update_item(self,
                    hash_key,
                    range_key=None,
                    attribute_updates=None,
                    expected=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=None,
                    return_values=None
                    ):
        """
        Performs the UpdateItem operation
        """
        return self.connection.update_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            attribute_updates=attribute_updates,
            expected=expected,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values=return_values)

    def put_item(self, hash_key,
                 range_key=None,
                 attributes=None,
                 expected=None,
                 return_values=None,
                 return_consumed_capacity=None,
                 return_item_collection_metrics=None):
        """
        Performs the PutItem operation and returns the result
        """
        return self.connection.put_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            attributes=attributes,
            expected=expected,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    def batch_write_item(self,
                         put_items=None,
                         delete_items=None,
                         return_consumed_capacity=None,
                         return_item_collection_metrics=None):
        """
        Performs the batch_write_item operation
        """
        return self.connection.batch_write_item(
            self.table_name,
            put_items=put_items,
            delete_items=delete_items,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    def batch_get_item(self, keys, consistent_read=None, return_consumed_capacity=None, attributes_to_get=None):
        """
        Performs the batch get item operation
        """
        return self.connection.batch_get_item(
            self.table_name,
            keys,
            consistent_read=consistent_read,
            return_consumed_capacity=return_consumed_capacity,
            attributes_to_get=attributes_to_get)

    def get_item(self, hash_key, range_key=None, consistent_read=False, attributes_to_get=None):
        """
        Performs the GetItem operation and returns the result
        """
        return self.connection.get_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get)

    def scan(self,
             attributes_to_get=None,
             limit=None,
             scan_filter=None,
             return_consumed_capacity=None,
             exclusive_start_key=None):
        """
        Performs the scan operation
        """
        return self.connection.scan(
            self.table_name,
            attributes_to_get=attributes_to_get,
            limit=limit,
            scan_filter=scan_filter,
            return_consumed_capacity=return_consumed_capacity,
            exclusive_start_key=exclusive_start_key)

    def query(self,
              hash_key,
              attributes=None,
              consistent_read=False,
              exclusive_start_key=None,
              index_name=None,
              key_conditions=None,
              limit=None,
              return_consumed_capacity=None,
              scan_index_forward=None,
              select=None
              ):
        """
        Performs the Query operation and returns the result
        """
        return self.connection.query(
            self.table_name,
            hash_key,
            attributes=attributes,
            consistent_read=consistent_read,
            exclusive_start_key=exclusive_start_key,
            index_name=index_name,
            key_conditions=key_conditions,
            limit=limit,
            return_consumed_capacity=return_consumed_capacity,
            scan_index_forward=scan_index_forward,
            select=select)
