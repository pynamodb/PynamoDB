"""
PynamoDB Connection classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
from pynamodb.connection.base import Connection


class TableConnection(object):
    """
    A higher level abstraction over botocore
    """

    def __init__(self,
                 table_name,
                 region=None,
                 host=None,
                 session_cls=None,
                 request_timeout_seconds=None,
                 max_retry_attempts=None,
                 base_backoff_ms=None,
                 aws_access_key_id=None,
                 aws_secret_access_key=None):
        self._hash_keyname = None
        self._range_keyname = None
        self.table_name = table_name
        self.connection = Connection(region=region,
                                     host=host,
                                     session_cls=session_cls,
                                     request_timeout_seconds=request_timeout_seconds,
                                     max_retry_attempts=max_retry_attempts,
                                     base_backoff_ms=base_backoff_ms)

        if aws_access_key_id and aws_secret_access_key:
            self.connection.session.set_credentials(aws_access_key_id,
                                                    aws_secret_access_key)

    def get_meta_table(self, refresh=False):
        """
        Returns a MetaTable
        """
        return self.connection.get_meta_table(self.table_name, refresh=refresh)

    def delete_item(self, hash_key,
                    range_key=None,
                    condition=None,
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
            condition=condition,
            expected=expected,
            conditional_operator=conditional_operator,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    def update_item(self,
                    hash_key,
                    range_key=None,
                    actions=None,
                    attribute_updates=None,
                    condition=None,
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
            actions=actions,
            attribute_updates=attribute_updates,
            condition=condition,
            expected=expected,
            conditional_operator=conditional_operator,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values=return_values)

    def put_item(self, hash_key,
                 range_key=None,
                 attributes=None,
                 condition=None,
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
            condition=condition,
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

    def rate_limited_scan(
             self,
             filter_condition=None,
             attributes_to_get=None,
             page_size=None,
             limit=None,
             conditional_operator=None,
             scan_filter=None,
             segment=None,
             total_segments=None,
             exclusive_start_key=None,
             timeout_seconds=None,
             read_capacity_to_consume_per_second=None,
             allow_rate_limited_scan_without_consumed_capacity=None,
             max_sleep_between_retry=None,
             max_consecutive_exceptions=None,
             consistent_read=None,
             index_name=None):
        """
        Performs the scan operation with rate limited
        """
        return self.connection.rate_limited_scan(
            self.table_name,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get,
            page_size=page_size,
            limit=limit,
            conditional_operator=conditional_operator,
            scan_filter=scan_filter,
            segment=segment,
            total_segments=total_segments,
            exclusive_start_key=exclusive_start_key,
            timeout_seconds=timeout_seconds,
            read_capacity_to_consume_per_second=read_capacity_to_consume_per_second,
            allow_rate_limited_scan_without_consumed_capacity=allow_rate_limited_scan_without_consumed_capacity,
            max_sleep_between_retry=max_sleep_between_retry,
            max_consecutive_exceptions=max_consecutive_exceptions,
            consistent_read=consistent_read,
            index_name=index_name)

    def scan(self,
             filter_condition=None,
             attributes_to_get=None,
             limit=None,
             conditional_operator=None,
             scan_filter=None,
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
            conditional_operator=conditional_operator,
            scan_filter=scan_filter,
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
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
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
