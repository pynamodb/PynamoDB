"""
PynamoDB Connection classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
from pynamodb.connection.base import Connection


class TableConnection(object):
    """
    A higher level abstraction over botocore
    """

    def __init__(self, table_name, region=None, host=None, session_cls=None,):
        self._hash_keyname = None
        self._range_keyname = None
        self.table_name = table_name
        self.connection = Connection(region=region, host=host, session_cls=session_cls,)

    def delete_item(self, hash_key,
                    range_key=None,
                    expected=None,
                    conditional_operator=None,
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
            conditional_operator=conditional_operator,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    def update_item(self,
                    hash_key,
                    range_key=None,
                    attribute_updates=None,
                    expected=None,
                    conditional_operator=None,
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
            conditional_operator=conditional_operator,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values=return_values)

    def put_item(self, hash_key,
                 range_key=None,
                 attributes=None,
                 expected=None,
                 conditional_operator=None,
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
            conditional_operator=conditional_operator,
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
             conditional_operator=None,
             scan_filter=None,
             return_consumed_capacity=None,
             segment=None,
             total_segments=None,
             exclusive_start_key=None):
        """
        Performs the scan operation
        """
        return self.connection.scan(
            self.table_name,
            attributes_to_get=attributes_to_get,
            limit=limit,
            conditional_operator=conditional_operator,
            scan_filter=scan_filter,
            return_consumed_capacity=return_consumed_capacity,
            segment=segment,
            total_segments=total_segments,
            exclusive_start_key=exclusive_start_key)

    def query(self,
              hash_key,
              attributes_to_get=None,
              consistent_read=False,
              exclusive_start_key=None,
              index_name=None,
              key_conditions=None,
              query_filters=None,
              limit=None,
              return_consumed_capacity=None,
              scan_index_forward=None,
              conditional_operator=None,
              select=None
              ):
        """
        Performs the Query operation and returns the result
        """
        return self.connection.query(
            self.table_name,
            hash_key,
            attributes_to_get=attributes_to_get,
            consistent_read=consistent_read,
            exclusive_start_key=exclusive_start_key,
            index_name=index_name,
            key_conditions=key_conditions,
            query_filters=query_filters,
            limit=limit,
            return_consumed_capacity=return_consumed_capacity,
            scan_index_forward=scan_index_forward,
            conditional_operator=conditional_operator,
            select=select)

    def describe_table(self):
        """
        Performs the DescribeTable operation and returns the result
        """
        return self.connection.describe_table(self.table_name)

    def delete_table(self):
        """
        Performs the DeleteTable operation and returns the result
        """
        return self.connection.delete_table(self.table_name)

    def update_table(self,
                     read_capacity_units=None,
                     write_capacity_units=None,
                     global_secondary_index_updates=None):
        """
        Performs the UpdateTable operation and returns the result
        """
        return self.connection.update_table(
            self.table_name,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            global_secondary_index_updates=global_secondary_index_updates)

    def create_table(self,
                     attribute_definitions=None,
                     key_schema=None,
                     read_capacity_units=None,
                     write_capacity_units=None,
                     global_secondary_indexes=None,
                     local_secondary_indexes=None,
                     stream_specification=None):
        """
        Performs the CreateTable operation and returns the result
        """
        return self.connection.create_table(
            self.table_name,
            attribute_definitions=attribute_definitions,
            key_schema=key_schema,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            global_secondary_indexes=global_secondary_indexes,
            local_secondary_indexes=local_secondary_indexes,
            stream_specification=stream_specification
        )
