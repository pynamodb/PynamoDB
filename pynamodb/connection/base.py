"""
Lowest level connection
"""
from base64 import b64decode
import logging

import six
from six.moves import range
from botocore.session import get_session
from botocore.exceptions import BotoCoreError
from botocore.client import ClientError
from botocore.vendored import requests

from pynamodb.connection.util import pythonic
from pynamodb.types import HASH, RANGE
from pynamodb.compat import NullHandler
from pynamodb.exceptions import (
    TableError, QueryError, PutError, DeleteError, UpdateError, GetError, ScanError, TableDoesNotExist,
    VerboseClientError
)
from pynamodb.constants import (
    RETURN_CONSUMED_CAPACITY_VALUES, RETURN_ITEM_COLL_METRICS_VALUES, COMPARISON_OPERATOR_VALUES,
    RETURN_ITEM_COLL_METRICS, RETURN_CONSUMED_CAPACITY, RETURN_VALUES_VALUES, ATTR_UPDATE_ACTIONS,
    COMPARISON_OPERATOR, EXCLUSIVE_START_KEY, SCAN_INDEX_FORWARD, SCAN_FILTER_VALUES, ATTR_DEFINITIONS,
    BATCH_WRITE_ITEM, CONSISTENT_READ, ATTR_VALUE_LIST, DESCRIBE_TABLE, DEFAULT_REGION, KEY_CONDITIONS,
    BATCH_GET_ITEM, DELETE_REQUEST, SELECT_VALUES, RETURN_VALUES, REQUEST_ITEMS, ATTR_UPDATES,
    ATTRS_TO_GET, SERVICE_NAME, DELETE_ITEM, PUT_REQUEST, UPDATE_ITEM, SCAN_FILTER, TABLE_NAME,
    INDEX_NAME, KEY_SCHEMA, ATTR_NAME, ATTR_TYPE, TABLE_KEY, EXPECTED, KEY_TYPE, GET_ITEM, UPDATE,
    PUT_ITEM, SELECT, ACTION, EXISTS, VALUE, LIMIT, QUERY, SCAN, ITEM, LOCAL_SECONDARY_INDEXES,
    KEYS, KEY, EQ, SEGMENT, TOTAL_SEGMENTS, CREATE_TABLE, PROVISIONED_THROUGHPUT, READ_CAPACITY_UNITS,
    WRITE_CAPACITY_UNITS, GLOBAL_SECONDARY_INDEXES, PROJECTION, EXCLUSIVE_START_TABLE_NAME, TOTAL,
    DELETE_TABLE, UPDATE_TABLE, LIST_TABLES, GLOBAL_SECONDARY_INDEX_UPDATES,
    CONSUMED_CAPACITY, CAPACITY_UNITS, QUERY_FILTER, QUERY_FILTER_VALUES, CONDITIONAL_OPERATOR,
    CONDITIONAL_OPERATORS, NULL, NOT_NULL, SHORT_ATTR_TYPES, DELETE,
    ITEMS, DEFAULT_ENCODING, BINARY_SHORT, BINARY_SET_SHORT, LAST_EVALUATED_KEY, RESPONSES, UNPROCESSED_KEYS,
    UNPROCESSED_ITEMS, STREAM_SPECIFICATION, STREAM_VIEW_TYPE, STREAM_ENABLED)

BOTOCORE_EXCEPTIONS = (BotoCoreError, ClientError)

# retry parameters
DEFAULT_TIMEOUT = 60  # matches legacy retry timeout from botocore
DEFAULT_MAX_RETRY_ATTEMPTS_EXCEPTION = 3

log = logging.getLogger(__name__)
log.addHandler(NullHandler())


class MetaTable(object):
    """
    A pythonic wrapper around table metadata
    """

    def __init__(self, data):
        self.data = data or {}
        self._range_keyname = None
        self._hash_keyname = None

    def __repr__(self):
        if self.data:
            return six.u("MetaTable<{0}>".format(self.data.get(TABLE_NAME)))

    @property
    def range_keyname(self):
        """
        Returns the name of this table's range key
        """
        if self._range_keyname is None:
            for attr in self.data.get(KEY_SCHEMA):
                if attr.get(KEY_TYPE) == RANGE:
                    self._range_keyname = attr.get(ATTR_NAME)
        return self._range_keyname

    @property
    def hash_keyname(self):
        """
        Returns the name of this table's hash key
        """
        if self._hash_keyname is None:
            for attr in self.data.get(KEY_SCHEMA):
                if attr.get(KEY_TYPE) == HASH:
                    self._hash_keyname = attr.get(ATTR_NAME)
                    break
        return self._hash_keyname

    def get_index_hash_keyname(self, index_name):
        """
        Returns the name of the hash key for a given index
        """
        global_indexes = self.data.get(GLOBAL_SECONDARY_INDEXES)
        local_indexes = self.data.get(LOCAL_SECONDARY_INDEXES)
        indexes = []
        if local_indexes:
            indexes += local_indexes
        if global_indexes:
            indexes += global_indexes
        for index in indexes:
            if index.get(INDEX_NAME) == index_name:
                for schema_key in index.get(KEY_SCHEMA):
                    if schema_key.get(KEY_TYPE) == HASH:
                        return schema_key.get(ATTR_NAME)

    def get_item_attribute_map(self, attributes, item_key=ITEM, pythonic_key=True):
        """
        Builds up a dynamodb compatible AttributeValue map
        """
        if pythonic_key:
            item_key = item_key
        attr_map = {
            item_key: {}
        }
        for key, value in attributes.items():
            # In this case, the user provided a mapping
            # {'key': {'S': 'value'}}
            if isinstance(value, dict):
                attr_map[item_key][key] = value
            else:
                attr_map[item_key][key] = {
                    self.get_attribute_type(key): value
                }
        return attr_map

    def get_attribute_type(self, attribute_name, value=None):
        """
        Returns the proper attribute type for a given attribute name
        """
        for attr in self.data.get(ATTR_DEFINITIONS):
            if attr.get(ATTR_NAME) == attribute_name:
                return attr.get(ATTR_TYPE)
        attr_names = [attr.get(ATTR_NAME) for attr in self.data.get(ATTR_DEFINITIONS)]
        if value is not None and isinstance(value, dict):
            for key in SHORT_ATTR_TYPES:
                if key in value:
                    return key
        raise ValueError("No attribute {0} in {1}".format(attribute_name, attr_names))

    def get_identifier_map(self, hash_key, range_key=None, key=KEY):
        """
        Builds the identifier map that is common to several operations
        """
        kwargs = {
            key: {
                self.hash_keyname: {
                    self.get_attribute_type(self.hash_keyname): hash_key
                }
            }
        }
        if range_key is not None:
            kwargs[key][self.range_keyname] = {
                self.get_attribute_type(self.range_keyname): range_key
            }
        return kwargs

    def get_exclusive_start_key_map(self, exclusive_start_key):
        """
        Builds the exclusive start key attribute map
        """
        if isinstance(exclusive_start_key, dict) and self.hash_keyname in exclusive_start_key:
            # This is useful when paginating results, as the LastEvaluatedKey returned is already
            # structured properly
            return {
                EXCLUSIVE_START_KEY: exclusive_start_key
            }
        else:
            return {
                EXCLUSIVE_START_KEY: {
                    self.hash_keyname: {
                        self.get_attribute_type(self.hash_keyname): exclusive_start_key
                    }
                }
            }


class Connection(object):
    """
    A higher level abstraction over botocore
    """

    def __init__(self, region=None, host=None, session_cls=None):
        self._tables = {}
        self.host = host
        self._session = None
        self._requests_session = None
        self._client = None
        if region:
            self.region = region
        else:
            self.region = DEFAULT_REGION

        # TODO: provide configurability of retry parameters via arguments
        self._request_timeout_seconds = DEFAULT_TIMEOUT
        self._max_retry_attempts_exception = DEFAULT_MAX_RETRY_ATTEMPTS_EXCEPTION

        if session_cls:
            self.session_cls = session_cls
        else:
            self.session_cls = requests.Session

    def __repr__(self):
        return six.u("Connection<{0}>".format(self.client.meta.endpoint_url))

    def _log_debug(self, operation, kwargs):
        """
        Sends a debug message to the logger
        """
        log.debug("Calling %s with arguments %s", operation, kwargs)

    def _log_debug_response(self, operation, response):
        """
        Sends a debug message to the logger about a response
        """
        log.debug("%s response: %s", operation, response)

    def _log_error(self, operation, response):
        """
        Sends an error message to the logger
        """
        log.error("%s failed with status: %s, message: %s",
                  operation, response.status_code,response.content)

    def dispatch(self, operation_name, operation_kwargs):
        """
        Dispatches `operation_name` with arguments `operation_kwargs`

        Raises TableDoesNotExist if the specified table does not exist
        """
        if operation_name not in [DESCRIBE_TABLE, LIST_TABLES, UPDATE_TABLE, DELETE_TABLE, CREATE_TABLE]:
            if RETURN_CONSUMED_CAPACITY not in operation_kwargs:
                operation_kwargs.update(self.get_consumed_capacity_map(TOTAL))
        self._log_debug(operation_name, operation_kwargs)

        data = self._make_api_call(operation_name, operation_kwargs)

        if data and CONSUMED_CAPACITY in data:
            capacity = data.get(CONSUMED_CAPACITY)
            if isinstance(capacity, dict) and CAPACITY_UNITS in capacity:
                capacity = capacity.get(CAPACITY_UNITS)
            log.debug("%s %s consumed %s units",  data.get(TABLE_NAME, ''), operation_name, capacity)
        return data

    def _make_api_call(self, operation_name, operation_kwargs):
        """
        This private method is here for two reasons:
        1. It's faster to avoid using botocore's response parsing
        2. It provides a place to monkey patch requests for unit testing
        """
        operation_model = self.client._service_model.operation_model(operation_name)
        request_dict = self.client._convert_to_request_dict(
            operation_kwargs,
            operation_model
        )
        prepared_request = self.client._endpoint.create_request(request_dict, operation_model)

        for attempt_number in range(1, self._max_retry_attempts_exception + 1):
            is_last_attempt_for_exceptions = attempt_number == self._max_retry_attempts_exception

            try:
                response = self.requests_session.send(
                    prepared_request,
                    timeout=self._request_timeout_seconds,
                    proxies=self.client._endpoint.proxies,
                )
            except requests.RequestException as e:
                if is_last_attempt_for_exceptions:
                    log.debug('Reached the maximum number of retry attempts: %s', attempt_number)
                    raise
                else:
                    log.debug(
                        'Retry needed for (%s) after attempt %s, retryable RequestException caught: %s',
                        operation_name,
                        attempt_number,
                        e
                    )
                    continue

            try:
                data = response.json()
            except ValueError as e:
                if is_last_attempt_for_exceptions:
                    log.debug('Reached the maximum number of retry attempts: %s', attempt_number)
                    raise
                else:
                    log.debug(
                        'Retry needed for (%s) after attempt %s, retryable JSONDecodeError caught: %s',
                        operation_name,
                        attempt_number,
                        e
                    )
                    continue

            # TODO: retry with backoff based on status code, error code, operation
            if response.status_code >= 300:
                # Extract error code from __type
                code = data.get('__type', '')
                if '#' in code:
                    code = code.rsplit('#', 1)[1]
                botocore_expected_format = {'Error': {'Message': data.get('message', ''), 'Code': code}}
                verbose_properties = {
                    'request_id': response.headers.get('x-amzn-RequestId')
                }

                if 'RequestItems' in operation_kwargs:
                    # Batch operations can hit multiple tables, report them comma separated
                    verbose_properties['table_name'] = ','.join(operation_kwargs['RequestItems'])
                else:
                    verbose_properties['table_name'] = operation_kwargs.get('TableName')

                raise VerboseClientError(botocore_expected_format, operation_name, verbose_properties)

            # Simulate botocore's binary attribute handling
            if ITEM in data:
                for attr in six.itervalues(data[ITEM]):
                    _convert_binary(attr)
            if ITEMS in data:
                for item in data[ITEMS]:
                    for attr in six.itervalues(item):
                        _convert_binary(attr)
            if RESPONSES in data:
                for item_list in six.itervalues(data[RESPONSES]):
                    for item in item_list:
                        for attr in six.itervalues(item):
                            _convert_binary(attr)
            if LAST_EVALUATED_KEY in data:
                for attr in six.itervalues(data[LAST_EVALUATED_KEY]):
                    _convert_binary(attr)
            if UNPROCESSED_KEYS in data:
                for item_list in six.itervalues(data[UNPROCESSED_KEYS]):
                    for item in item_list:
                        for attr in six.itervalues(item):
                            _convert_binary(attr)
            if UNPROCESSED_ITEMS in data:
                for item_mapping in six.itervalues(data[UNPROCESSED_ITEMS]):
                    for item in item_mapping:
                        for attr in six.itervalues(item):
                            _convert_binary(attr)

            return data

    @property
    def session(self):
        """
        Returns a valid botocore session
        """
        if self._session is None:
            self._session = get_session()
        return self._session

    @property
    def requests_session(self):
        """
        Return a requests session to execute prepared requests using the same pool
        """
        if self._requests_session is None:
            self._requests_session = self.session_cls()
        return self._requests_session

    @property
    def client(self):
        """
        Returns a botocore dynamodb client
        """
        # botocore has a known issue where it will cache empty credentials
        # https://github.com/boto/botocore/blob/4d55c9b4142/botocore/credentials.py#L1016-L1021
        # if the client does not have credentials, we create a new client
        # otherwise the client is permanently poisoned in the case of metadata service flakiness when using IAM roles
        if not self._client or (self._client._request_signer and not self._client._request_signer._credentials):
            self._client = self.session.create_client(SERVICE_NAME, self.region, endpoint_url=self.host)
        return self._client

    def get_meta_table(self, table_name, refresh=False):
        """
        Returns a MetaTable
        """
        if table_name not in self._tables or refresh:
            operation_kwargs = {
                TABLE_NAME: table_name
            }
            try:
                data = self.dispatch(DESCRIBE_TABLE, operation_kwargs)
                self._tables[table_name] = MetaTable(data.get(TABLE_KEY))
            except BotoCoreError as e:
                raise TableError("Unable to describe table: {0}".format(e), e)
            except ClientError as e:
                if 'ResourceNotFound' in e.response['Error']['Code']:
                    raise TableDoesNotExist(e.response['Error']['Message'])
                else:
                    raise
        return self._tables[table_name]

    def create_table(self,
                     table_name,
                     attribute_definitions=None,
                     key_schema=None,
                     read_capacity_units=None,
                     write_capacity_units=None,
                     global_secondary_indexes=None,
                     local_secondary_indexes=None,
                     stream_specification=None):
        """
        Performs the CreateTable operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name,
            PROVISIONED_THROUGHPUT: {
                READ_CAPACITY_UNITS: read_capacity_units,
                WRITE_CAPACITY_UNITS: write_capacity_units
            }
        }
        attrs_list = []
        if attribute_definitions is None:
            raise ValueError("attribute_definitions argument is required")
        for attr in attribute_definitions:
            attrs_list.append({
                ATTR_NAME: attr.get(pythonic(ATTR_NAME)),
                ATTR_TYPE: attr.get(pythonic(ATTR_TYPE))
            })
        operation_kwargs[ATTR_DEFINITIONS] = attrs_list

        if global_secondary_indexes:
            global_secondary_indexes_list = []
            for index in global_secondary_indexes:
                global_secondary_indexes_list.append({
                    INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                    KEY_SCHEMA: sorted(index.get(pythonic(KEY_SCHEMA)), key=lambda x: x.get(KEY_TYPE)),
                    PROJECTION: index.get(pythonic(PROJECTION)),
                    PROVISIONED_THROUGHPUT: index.get(pythonic(PROVISIONED_THROUGHPUT))
                })
            operation_kwargs[GLOBAL_SECONDARY_INDEXES] = global_secondary_indexes_list

        if key_schema is None:
            raise ValueError("key_schema is required")
        key_schema_list = []
        for item in key_schema:
            key_schema_list.append({
                ATTR_NAME: item.get(pythonic(ATTR_NAME)),
                KEY_TYPE: str(item.get(pythonic(KEY_TYPE))).upper()
            })
        operation_kwargs[KEY_SCHEMA] = sorted(key_schema_list, key=lambda x: x.get(KEY_TYPE))

        local_secondary_indexes_list = []
        if local_secondary_indexes:
            for index in local_secondary_indexes:
                local_secondary_indexes_list.append({
                    INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                    KEY_SCHEMA: sorted(index.get(pythonic(KEY_SCHEMA)), key=lambda x: x.get(KEY_TYPE)),
                    PROJECTION: index.get(pythonic(PROJECTION)),
                })
            operation_kwargs[LOCAL_SECONDARY_INDEXES] = local_secondary_indexes_list

        if stream_specification:
            operation_kwargs[STREAM_SPECIFICATION] = {
                STREAM_ENABLED: stream_specification[pythonic(STREAM_ENABLED)],
                STREAM_VIEW_TYPE: stream_specification[pythonic(STREAM_VIEW_TYPE)]
            }

        try:
            data = self.dispatch(CREATE_TABLE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Failed to create table: {0}".format(e), e)
        return data

    def delete_table(self, table_name):
        """
        Performs the DeleteTable operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name
        }
        try:
            data = self.dispatch(DELETE_TABLE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Failed to delete table: {0}".format(e), e)
        return data

    def update_table(self,
                     table_name,
                     read_capacity_units=None,
                     write_capacity_units=None,
                     global_secondary_index_updates=None):
        """
        Performs the UpdateTable operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name
        }
        if read_capacity_units and not write_capacity_units or write_capacity_units and not read_capacity_units:
            raise ValueError("read_capacity_units and write_capacity_units are required together")
        if read_capacity_units and write_capacity_units:
            operation_kwargs[PROVISIONED_THROUGHPUT] = {
                READ_CAPACITY_UNITS: read_capacity_units,
                WRITE_CAPACITY_UNITS: write_capacity_units
            }
        if global_secondary_index_updates:
            global_secondary_indexes_list = []
            for index in global_secondary_index_updates:
                global_secondary_indexes_list.append({
                    UPDATE: {
                        INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                        PROVISIONED_THROUGHPUT: {
                            READ_CAPACITY_UNITS: index.get(pythonic(READ_CAPACITY_UNITS)),
                            WRITE_CAPACITY_UNITS: index.get(pythonic(WRITE_CAPACITY_UNITS))
                        }
                    }
                })
            operation_kwargs[GLOBAL_SECONDARY_INDEX_UPDATES] = global_secondary_indexes_list
        try:
            return self.dispatch(UPDATE_TABLE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Failed to update table: {0}".format(e), e)

    def list_tables(self, exclusive_start_table_name=None, limit=None):
        """
        Performs the ListTables operation
        """
        operation_kwargs = {}
        if exclusive_start_table_name:
            operation_kwargs.update({
                EXCLUSIVE_START_TABLE_NAME: exclusive_start_table_name
            })
        if limit is not None:
            operation_kwargs.update({
                LIMIT: limit
            })
        try:
            return self.dispatch(LIST_TABLES, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Unable to list tables: {0}".format(e), e)

    def describe_table(self, table_name):
        """
        Performs the DescribeTable operation
        """
        tbl = self.get_meta_table(table_name, refresh=True)
        if tbl:
            return tbl.data
        else:
            raise TableDoesNotExist(table_name)

    def get_conditional_operator(self, operator):
        """
        Returns a dictionary containing the correct conditional operator,
        validating it first.
        """
        operator = operator.upper()
        if operator not in CONDITIONAL_OPERATORS:
            raise ValueError(
                "The {0} must be one of {1}".format(
                    CONDITIONAL_OPERATOR,
                    CONDITIONAL_OPERATORS
                )
            )
        return {
            CONDITIONAL_OPERATOR: operator
        }

    def get_item_attribute_map(self, table_name, attributes, item_key=ITEM, pythonic_key=True):
        """
        Builds up a dynamodb compatible AttributeValue map
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {0}".format(table_name))
        return tbl.get_item_attribute_map(
            attributes,
            item_key=item_key,
            pythonic_key=pythonic_key)

    def get_expected_map(self, table_name, expected):
        """
        Builds the expected map that is common to several operations
        """
        kwargs = {EXPECTED: {}}
        for key, condition in expected.items():
            if EXISTS in condition:
                kwargs[EXPECTED][key] = {
                    EXISTS: condition.get(EXISTS)
                }
            elif VALUE in condition:
                kwargs[EXPECTED][key] = {
                    VALUE: {
                        self.get_attribute_type(table_name, key): condition.get(VALUE)
                    }
                }
            elif COMPARISON_OPERATOR in condition:
                kwargs[EXPECTED][key] = {
                    COMPARISON_OPERATOR: condition.get(COMPARISON_OPERATOR),
                }
                values = []
                for value in condition.get(ATTR_VALUE_LIST, []):
                    attr_type = self.get_attribute_type(table_name, key, value)
                    values.append({attr_type: self.parse_attribute(value)})
                if condition.get(COMPARISON_OPERATOR) not in [NULL, NOT_NULL]:
                    kwargs[EXPECTED][key][ATTR_VALUE_LIST] = values
        return kwargs

    def parse_attribute(self, attribute, return_type=False):
        """
        Returns the attribute value, where the attribute can be
        a raw attribute value, or a dictionary containing the type:
        {'S': 'String value'}
        """
        if isinstance(attribute, dict):
            for key in SHORT_ATTR_TYPES:
                if key in attribute:
                    if return_type:
                        return key, attribute.get(key)
                    return attribute.get(key)
            raise ValueError("Invalid attribute supplied: {0}".format(attribute))
        else:
            if return_type:
                return None, attribute
            return attribute

    def get_attribute_type(self, table_name, attribute_name, value=None):
        """
        Returns the proper attribute type for a given attribute name
        :param value: The attribute value an be supplied just in case the type is already included
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {0}".format(table_name))
        return tbl.get_attribute_type(attribute_name, value=value)

    def get_identifier_map(self, table_name, hash_key, range_key=None, key=KEY):
        """
        Builds the identifier map that is common to several operations
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {0}".format(table_name))
        return tbl.get_identifier_map(hash_key, range_key=range_key, key=key)

    def get_query_filter_map(self, table_name, query_filters):
        """
        Builds the QueryFilter object needed for the Query operation
        """
        kwargs = {
            QUERY_FILTER: {}
        }
        for key, condition in query_filters.items():
            operator = condition.get(COMPARISON_OPERATOR)
            if operator not in QUERY_FILTER_VALUES:
                raise ValueError("{0} must be one of {1}".format(COMPARISON_OPERATOR, QUERY_FILTER_VALUES))
            attr_value_list = []
            for value in condition.get(ATTR_VALUE_LIST, []):
                attr_value_list.append({
                    self.get_attribute_type(table_name, key, value): self.parse_attribute(value)
                })
            kwargs[QUERY_FILTER][key] = {
                COMPARISON_OPERATOR: operator
            }
            if len(attr_value_list):
                kwargs[QUERY_FILTER][key][ATTR_VALUE_LIST] = attr_value_list
        return kwargs

    def get_consumed_capacity_map(self, return_consumed_capacity):
        """
        Builds the consumed capacity map that is common to several operations
        """
        if return_consumed_capacity.upper() not in RETURN_CONSUMED_CAPACITY_VALUES:
            raise ValueError("{0} must be one of {1}".format(RETURN_ITEM_COLL_METRICS, RETURN_CONSUMED_CAPACITY_VALUES))
        return {
            RETURN_CONSUMED_CAPACITY: str(return_consumed_capacity).upper()
        }

    def get_return_values_map(self, return_values):
        """
        Builds the return values map that is common to several operations
        """
        if return_values.upper() not in RETURN_VALUES_VALUES:
            raise ValueError("{0} must be one of {1}".format(RETURN_VALUES, RETURN_VALUES_VALUES))
        return {
            RETURN_VALUES: str(return_values).upper()
        }

    def get_item_collection_map(self, return_item_collection_metrics):
        """
        Builds the item collection map
        """
        if return_item_collection_metrics.upper() not in RETURN_ITEM_COLL_METRICS_VALUES:
            raise ValueError("{0} must be one of {1}".format(RETURN_ITEM_COLL_METRICS, RETURN_ITEM_COLL_METRICS_VALUES))
        return {
            RETURN_ITEM_COLL_METRICS: str(return_item_collection_metrics).upper()
        }

    def get_exclusive_start_key_map(self, table_name, exclusive_start_key):
        """
        Builds the exclusive start key attribute map
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {0}".format(table_name))
        return tbl.get_exclusive_start_key_map(exclusive_start_key)

    def delete_item(self,
                    table_name,
                    hash_key,
                    range_key=None,
                    expected=None,
                    conditional_operator=None,
                    return_values=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=None):
        """
        Performs the DeleteItem operation and returns the result
        """
        operation_kwargs = {TABLE_NAME: table_name}
        operation_kwargs.update(self.get_identifier_map(table_name, hash_key, range_key))

        if expected:
            operation_kwargs.update(self.get_expected_map(table_name, expected))
        if return_values:
            operation_kwargs.update(self.get_return_values_map(return_values))
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics:
            operation_kwargs.update(self.get_item_collection_map(return_item_collection_metrics))
        if conditional_operator:
            operation_kwargs.update(self.get_conditional_operator(conditional_operator))
        try:
            return self.dispatch(DELETE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise DeleteError("Failed to delete item: {0}".format(e), e)

    def update_item(self,
                    table_name,
                    hash_key,
                    range_key=None,
                    attribute_updates=None,
                    expected=None,
                    return_consumed_capacity=None,
                    conditional_operator=None,
                    return_item_collection_metrics=None,
                    return_values=None):
        """
        Performs the UpdateItem operation
        """
        operation_kwargs = {TABLE_NAME: table_name}
        operation_kwargs.update(self.get_identifier_map(table_name, hash_key, range_key))
        if expected:
            operation_kwargs.update(self.get_expected_map(table_name, expected))
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics:
            operation_kwargs.update(self.get_item_collection_map(return_item_collection_metrics))
        if return_values:
            operation_kwargs.update(self.get_return_values_map(return_values))
        if conditional_operator:
            operation_kwargs.update(self.get_conditional_operator(conditional_operator))
        if not attribute_updates:
            raise ValueError("{0} cannot be empty".format(ATTR_UPDATES))

        operation_kwargs[ATTR_UPDATES] = {}
        for key, update in attribute_updates.items():
            value = update.get(VALUE)
            attr_type, value = self.parse_attribute(value, return_type=True)
            action = update.get(ACTION)
            if attr_type is None and action is not None and action.upper() != DELETE:
                attr_type = self.get_attribute_type(table_name, key, value)
            if action not in ATTR_UPDATE_ACTIONS:
                raise ValueError("{0} must be one of {1}".format(ACTION, ATTR_UPDATE_ACTIONS))
            operation_kwargs[ATTR_UPDATES][key] = {
                ACTION: action,
            }
            if action.upper() != DELETE:
                operation_kwargs[ATTR_UPDATES][key][VALUE] = {attr_type: value}
        try:
            return self.dispatch(UPDATE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise UpdateError("Failed to update item: {0}".format(e), e)

    def put_item(self,
                 table_name,
                 hash_key,
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
        operation_kwargs = {TABLE_NAME: table_name}
        operation_kwargs.update(self.get_identifier_map(table_name, hash_key, range_key, key=ITEM))
        if attributes:
            attrs = self.get_item_attribute_map(table_name, attributes)
            operation_kwargs[ITEM].update(attrs[ITEM])
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics:
            operation_kwargs.update(self.get_item_collection_map(return_item_collection_metrics))
        if return_values:
            operation_kwargs.update(self.get_return_values_map(return_values))
        if expected:
            operation_kwargs.update(self.get_expected_map(table_name, expected))
        if conditional_operator:
            operation_kwargs.update(self.get_conditional_operator(conditional_operator))
        try:
            return self.dispatch(PUT_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise PutError("Failed to put item: {0}".format(e), e)

    def batch_write_item(self,
                         table_name,
                         put_items=None,
                         delete_items=None,
                         return_consumed_capacity=None,
                         return_item_collection_metrics=None):
        """
        Performs the batch_write_item operation
        """
        if put_items is None and delete_items is None:
            raise ValueError("Either put_items or delete_items must be specified")
        operation_kwargs = {
            REQUEST_ITEMS: {
                table_name: []
            }
        }
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics:
            operation_kwargs.update(self.get_item_collection_map(return_item_collection_metrics))
        put_items_list = []
        if put_items:
            for item in put_items:
                put_items_list.append({
                    PUT_REQUEST: self.get_item_attribute_map(table_name, item, pythonic_key=False)
                })
        delete_items_list = []
        if delete_items:
            for item in delete_items:
                delete_items_list.append({
                    DELETE_REQUEST: self.get_item_attribute_map(table_name, item, item_key=KEY, pythonic_key=False)
                })
        operation_kwargs[REQUEST_ITEMS][table_name] = delete_items_list + put_items_list
        try:
            return self.dispatch(BATCH_WRITE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise PutError("Failed to batch write items: {0}".format(e), e)

    def batch_get_item(self,
                       table_name,
                       keys,
                       consistent_read=None,
                       return_consumed_capacity=None,
                       attributes_to_get=None):
        """
        Performs the batch get item operation
        """
        operation_kwargs = {
            REQUEST_ITEMS: {
                table_name: {}
            }
        }

        args_map = {}
        if consistent_read:
            args_map[CONSISTENT_READ] = consistent_read
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if attributes_to_get is not None:
            args_map[ATTRS_TO_GET] = attributes_to_get
        operation_kwargs[REQUEST_ITEMS][table_name].update(args_map)

        keys_map = {KEYS: []}
        for key in keys:
            keys_map[KEYS].append(
                self.get_item_attribute_map(table_name, key)[ITEM]
            )
        operation_kwargs[REQUEST_ITEMS][table_name].update(keys_map)
        try:
            return self.dispatch(BATCH_GET_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise GetError("Failed to batch get items: {0}".format(e), e)

    def get_item(self,
                 table_name,
                 hash_key,
                 range_key=None,
                 consistent_read=False,
                 attributes_to_get=None):
        """
        Performs the GetItem operation and returns the result
        """
        operation_kwargs = {}
        if attributes_to_get is not None:
            operation_kwargs[ATTRS_TO_GET] = attributes_to_get
        operation_kwargs[CONSISTENT_READ] = consistent_read
        operation_kwargs[TABLE_NAME] = table_name
        operation_kwargs.update(self.get_identifier_map(table_name, hash_key, range_key))
        try:
            return self.dispatch(GET_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise GetError("Failed to get item: {0}".format(e), e)

    def scan(self,
             table_name,
             attributes_to_get=None,
             limit=None,
             conditional_operator=None,
             scan_filter=None,
             return_consumed_capacity=None,
             exclusive_start_key=None,
             segment=None,
             total_segments=None):
        """
        Performs the scan operation
        """
        operation_kwargs = {TABLE_NAME: table_name}
        if attributes_to_get is not None:
            operation_kwargs[ATTRS_TO_GET] = attributes_to_get
        if limit is not None:
            operation_kwargs[LIMIT] = limit
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if exclusive_start_key:
            operation_kwargs.update(self.get_exclusive_start_key_map(table_name, exclusive_start_key))
        if segment is not None:
            operation_kwargs[SEGMENT] = segment
        if total_segments:
            operation_kwargs[TOTAL_SEGMENTS] = total_segments
        if scan_filter:
            operation_kwargs[SCAN_FILTER] = {}
            for key, condition in scan_filter.items():
                operator = condition.get(COMPARISON_OPERATOR)
                if operator not in SCAN_FILTER_VALUES:
                    raise ValueError("{0} must be one of {1}".format(COMPARISON_OPERATOR, SCAN_FILTER_VALUES))
                values = []
                for value in condition.get(ATTR_VALUE_LIST, []):
                    attr_type = self.get_attribute_type(table_name, key, value)
                    values.append({attr_type: self.parse_attribute(value)})
                operation_kwargs[SCAN_FILTER][key] = {
                    COMPARISON_OPERATOR: operator
                }
                if len(values):
                    operation_kwargs[SCAN_FILTER][key][ATTR_VALUE_LIST] = values
            if conditional_operator:
                operation_kwargs.update(self.get_conditional_operator(conditional_operator))
        try:
            return self.dispatch(SCAN, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise ScanError("Failed to scan table: {0}".format(e), e)

    def query(self,
              table_name,
              hash_key,
              attributes_to_get=None,
              consistent_read=False,
              exclusive_start_key=None,
              index_name=None,
              key_conditions=None,
              query_filters=None,
              conditional_operator=None,
              limit=None,
              return_consumed_capacity=None,
              scan_index_forward=None,
              select=None):
        """
        Performs the Query operation and returns the result
        """
        operation_kwargs = {TABLE_NAME: table_name}
        if attributes_to_get:
            operation_kwargs[ATTRS_TO_GET] = attributes_to_get
        if consistent_read:
            operation_kwargs[CONSISTENT_READ] = True
        if exclusive_start_key:
            operation_kwargs.update(self.get_exclusive_start_key_map(table_name, exclusive_start_key))
        if index_name:
            operation_kwargs[INDEX_NAME] = index_name
        if limit is not None:
            operation_kwargs[LIMIT] = limit
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if query_filters:
            operation_kwargs.update(self.get_query_filter_map(table_name, query_filters))
        if conditional_operator:
            operation_kwargs.update(self.get_conditional_operator(conditional_operator))
        if select:
            if select.upper() not in SELECT_VALUES:
                raise ValueError("{0} must be one of {1}".format(SELECT, SELECT_VALUES))
            operation_kwargs[SELECT] = str(select).upper()
        if scan_index_forward is not None:
            operation_kwargs[SCAN_INDEX_FORWARD] = scan_index_forward
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table: {0}".format(table_name))
        if index_name:
            hash_keyname = tbl.get_index_hash_keyname(index_name)
            if not hash_keyname:
                raise ValueError("No hash key attribute for index: {0}".format(index_name))
        else:
            hash_keyname = tbl.hash_keyname
        operation_kwargs[KEY_CONDITIONS] = {
            hash_keyname: {
                ATTR_VALUE_LIST: [
                    {
                        self.get_attribute_type(table_name, hash_keyname): hash_key,
                    }
                ],
                COMPARISON_OPERATOR: EQ
            },
        }
        if key_conditions is not None:
            for key, condition in key_conditions.items():
                attr_type = self.get_attribute_type(table_name, key)
                operator = condition.get(COMPARISON_OPERATOR)
                if operator not in COMPARISON_OPERATOR_VALUES:
                    raise ValueError("{0} must be one of {1}".format(COMPARISON_OPERATOR, COMPARISON_OPERATOR_VALUES))
                operation_kwargs[KEY_CONDITIONS][key] = {
                    ATTR_VALUE_LIST: [
                        {
                            attr_type: self.parse_attribute(value)
                        } for value in condition.get(ATTR_VALUE_LIST)
                    ],
                    COMPARISON_OPERATOR: operator
                }

        try:
            return self.dispatch(QUERY, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise QueryError("Failed to query items: {0}".format(e), e)


def _convert_binary(attr):
    if BINARY_SHORT in attr:
        attr[BINARY_SHORT] = b64decode(attr[BINARY_SHORT].encode(DEFAULT_ENCODING))
    elif BINARY_SET_SHORT in attr:
        value = attr[BINARY_SET_SHORT]
        if value and len(value):
            attr[BINARY_SET_SHORT] = set(b64decode(v.encode(DEFAULT_ENCODING)) for v in value)
