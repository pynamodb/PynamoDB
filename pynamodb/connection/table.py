"""
PynamoDB Connection classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
from pynamodb.connection.base import Connection
from pynamodb.constants import DEFAULT_BILLING_MODE, KEY


class TableConnection(object):
    """
    A higher level abstraction over botocore
    """

    def __init__(self,
                 table_name,
                 region=None,
                 host=None,
                 connect_timeout_seconds=None,
                 read_timeout_seconds=None,
                 max_retry_attempts=None,
                 base_backoff_ms=None,
                 max_pool_connections=None,
                 extra_headers=None,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None):
        self._hash_keyname = None
        self._range_keyname = None
        self.table_name = table_name
        self.connection = Connection(region=region,
                                     host=host,
                                     connect_timeout_seconds=connect_timeout_seconds,
                                     read_timeout_seconds=read_timeout_seconds,
                                     max_retry_attempts=max_retry_attempts,
                                     base_backoff_ms=base_backoff_ms,
                                     max_pool_connections=max_pool_connections,
                                     extra_headers=extra_headers)

        if aws_access_key_id and aws_secret_access_key:
            self.connection.session.set_credentials(aws_access_key_id,
                                                    aws_secret_access_key,
                                                    aws_session_token)

    def get_meta_table(self, refresh=False):
        """
        Returns a MetaTable
        """
        return self.connection.get_meta_table(self.table_name, refresh=refresh)

    def get_operation_kwargs(self,
                             hash_key,
                             range_key=None,
                             key=KEY,
                             attributes=None,
                             attributes_to_get=None,
                             actions=None,
                             condition=None,
                             consistent_read=None,
                             return_values=None,
                             return_consumed_capacity=None,
                             return_item_collection_metrics=None,
                             return_values_on_condition_failure=None):
        return self.connection.get_operation_kwargs(
            self.table_name,
            hash_key,
            range_key=range_key,
            key=key,
            attributes=attributes,
            attributes_to_get=attributes_to_get,
            actions=actions,
            condition=condition,
            consistent_read=consistent_read,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values_on_condition_failure=return_values_on_condition_failure
        )

    def delete_item(self,
                    hash_key,
                    range_key=None,
                    condition=None,
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
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    def update_item(self,
                    hash_key,
                    range_key=None,
                    actions=None,
                    condition=None,
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
            actions=actions,
            condition=condition,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values=return_values)

    def put_item(self,
                 hash_key,
                 range_key=None,
                 attributes=None,
                 condition=None,
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
            condition=condition,
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
             filter_condition=None,
             attributes_to_get=None,
             limit=None,
             return_consumed_capacity=None,
             segment=None,
             total_segments=None,
             exclusive_start_key=None,
             consistent_read=None,
             index_name=None):
        """
        Performs the scan operation
        """
        return self.connection.scan(
            self.table_name,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get,
            limit=limit,
            return_consumed_capacity=return_consumed_capacity,
            segment=segment,
            total_segments=total_segments,
            exclusive_start_key=exclusive_start_key,
            consistent_read=consistent_read,
            index_name=index_name)

    def query(self,
              hash_key,
              range_key_condition=None,
              filter_condition=None,
              attributes_to_get=None,
              consistent_read=False,
              exclusive_start_key=None,
              index_name=None,
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
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get,
            consistent_read=consistent_read,
            exclusive_start_key=exclusive_start_key,
            index_name=index_name,
            limit=limit,
            return_consumed_capacity=return_consumed_capacity,
            scan_index_forward=scan_index_forward,
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

    def update_time_to_live(self, ttl_attr_name):
        """
        Performs the UpdateTimeToLive operation and returns the result
        """
        return self.connection.update_time_to_live(self.table_name, ttl_attr_name)

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
                     stream_specification=None,
                     billing_mode=DEFAULT_BILLING_MODE):
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
            stream_specification=stream_specification,
            billing_mode=billing_mode
        )
