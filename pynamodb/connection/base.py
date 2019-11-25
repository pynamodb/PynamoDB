"""
Lowest level connection
"""
from __future__ import division

import json
import logging
import random
import sys
import time
import uuid
from base64 import b64decode
from threading import local

import six
import botocore.client
import botocore.exceptions
from botocore.awsrequest import create_request_object
from botocore.client import ClientError
from botocore.hooks import first_non_none_response
from botocore.exceptions import BotoCoreError
from botocore.session import get_session
from six.moves import range

from pynamodb.connection.util import pythonic
from pynamodb.constants import (
    RETURN_CONSUMED_CAPACITY_VALUES, RETURN_ITEM_COLL_METRICS_VALUES,
    RETURN_ITEM_COLL_METRICS, RETURN_CONSUMED_CAPACITY, RETURN_VALUES_VALUES,
    EXCLUSIVE_START_KEY, SCAN_INDEX_FORWARD, ATTR_DEFINITIONS,
    BATCH_WRITE_ITEM, CONSISTENT_READ, DESCRIBE_TABLE, KEY_CONDITION_EXPRESSION,
    BATCH_GET_ITEM, DELETE_REQUEST, SELECT_VALUES, RETURN_VALUES, REQUEST_ITEMS,
    PROJECTION_EXPRESSION, SERVICE_NAME, DELETE_ITEM, PUT_REQUEST, UPDATE_ITEM, TABLE_NAME,
    INDEX_NAME, KEY_SCHEMA, ATTR_NAME, ATTR_TYPE, TABLE_KEY, KEY_TYPE, GET_ITEM, UPDATE,
    PUT_ITEM, SELECT, LIMIT, QUERY, SCAN, ITEM, LOCAL_SECONDARY_INDEXES,
    KEYS, KEY, SEGMENT, TOTAL_SEGMENTS, CREATE_TABLE, PROVISIONED_THROUGHPUT, READ_CAPACITY_UNITS,
    WRITE_CAPACITY_UNITS, GLOBAL_SECONDARY_INDEXES, PROJECTION, EXCLUSIVE_START_TABLE_NAME, TOTAL,
    DELETE_TABLE, UPDATE_TABLE, LIST_TABLES, GLOBAL_SECONDARY_INDEX_UPDATES, ATTRIBUTES,
    CONSUMED_CAPACITY, CAPACITY_UNITS,
    SHORT_ATTR_TYPES,
    ITEMS, DEFAULT_ENCODING, BINARY_SHORT, BINARY_SET_SHORT, LAST_EVALUATED_KEY, RESPONSES, UNPROCESSED_KEYS,
    UNPROCESSED_ITEMS, STREAM_SPECIFICATION, STREAM_VIEW_TYPE, STREAM_ENABLED,
    EXPRESSION_ATTRIBUTE_NAMES, EXPRESSION_ATTRIBUTE_VALUES,
    CONDITION_EXPRESSION, FILTER_EXPRESSION,
    TRANSACT_WRITE_ITEMS, TRANSACT_GET_ITEMS, CLIENT_REQUEST_TOKEN, TRANSACT_ITEMS, TRANSACT_CONDITION_CHECK,
    TRANSACT_GET, TRANSACT_PUT, TRANSACT_DELETE, TRANSACT_UPDATE, UPDATE_EXPRESSION,
    RETURN_VALUES_ON_CONDITION_FAILURE_VALUES, RETURN_VALUES_ON_CONDITION_FAILURE,
    AVAILABLE_BILLING_MODES, DEFAULT_BILLING_MODE,  BILLING_MODE, PAY_PER_REQUEST_BILLING_MODE,
    PROVISIONED_BILLING_MODE,
    TIME_TO_LIVE_SPECIFICATION, ENABLED, UPDATE_TIME_TO_LIVE
)
from pynamodb.exceptions import (
    TableError, QueryError, PutError, DeleteError, UpdateError, GetError, ScanError, TableDoesNotExist,
    VerboseClientError,
    TransactGetError, TransactWriteError)
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.operand import Path
from pynamodb.expressions.projection import create_projection_expression
from pynamodb.expressions.update import Update
from pynamodb.settings import get_settings_value
from pynamodb.signals import pre_dynamodb_send, post_dynamodb_send
from pynamodb.types import HASH, RANGE

BOTOCORE_EXCEPTIONS = (BotoCoreError, ClientError)
RATE_LIMITING_ERROR_CODES = ['ProvisionedThroughputExceededException', 'ThrottlingException']

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


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
            return six.u("MetaTable<{}>".format(self.data.get(TABLE_NAME)))

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

    def get_key_names(self, index_name=None):
        """
        Returns the names of the primary key attributes and index key attributes (if index_name is specified)
        """
        key_names = [self.hash_keyname]
        if self.range_keyname:
            key_names.append(self.range_keyname)
        if index_name is not None:
            index_hash_keyname = self.get_index_hash_keyname(index_name)
            if index_hash_keyname not in key_names:
                key_names.append(index_hash_keyname)
            index_range_keyname = self.get_index_range_keyname(index_name)
            if index_range_keyname is not None and index_range_keyname not in key_names:
                key_names.append(index_range_keyname)
        return key_names

    def has_index_name(self, index_name):
        """
        Returns True if the base table has a global or local secondary index with index_name
        """
        global_indexes = self.data.get(GLOBAL_SECONDARY_INDEXES)
        local_indexes = self.data.get(LOCAL_SECONDARY_INDEXES)
        indexes = (global_indexes or []) + (local_indexes or [])
        return any(index.get(INDEX_NAME) == index_name for index in indexes)

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

    def get_index_range_keyname(self, index_name):
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
                    if schema_key.get(KEY_TYPE) == RANGE:
                        return schema_key.get(ATTR_NAME)
        return None

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
        if value is not None and isinstance(value, dict):
            for key in SHORT_ATTR_TYPES:
                if key in value:
                    return key
        attr_names = [attr.get(ATTR_NAME) for attr in self.data.get(ATTR_DEFINITIONS)]
        raise ValueError("No attribute {} in {}".format(attribute_name, attr_names))

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

    def __init__(self, region=None, host=None,
                 read_timeout_seconds=None, connect_timeout_seconds=None,
                 max_retry_attempts=None, base_backoff_ms=None,
                 max_pool_connections=None, extra_headers=None):
        self._tables = {}
        self.host = host
        self._local = local()
        self._client = None
        if region:
            self.region = region
        else:
            self.region = get_settings_value('region')

        if connect_timeout_seconds is not None:
            self._connect_timeout_seconds = connect_timeout_seconds
        else:
            self._connect_timeout_seconds = get_settings_value('connect_timeout_seconds')

        if read_timeout_seconds is not None:
            self._read_timeout_seconds = read_timeout_seconds
        else:
            self._read_timeout_seconds = get_settings_value('read_timeout_seconds')

        if max_retry_attempts is not None:
            self._max_retry_attempts_exception = max_retry_attempts
        else:
            self._max_retry_attempts_exception = get_settings_value('max_retry_attempts')

        if base_backoff_ms is not None:
            self._base_backoff_ms = base_backoff_ms
        else:
            self._base_backoff_ms = get_settings_value('base_backoff_ms')

        if max_pool_connections is not None:
            self._max_pool_connections = max_pool_connections
        else:
            self._max_pool_connections = get_settings_value('max_pool_connections')

        if extra_headers is not None:
            self._extra_headers = extra_headers
        else:
            self._extra_headers = get_settings_value('extra_headers')

    def __repr__(self):
        return six.u("Connection<{}>".format(self.client.meta.endpoint_url))

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

    def _sign_request(self, request):
        auth = self.client._request_signer.get_auth_instance(
            self.client._request_signer.signing_name,
            self.client._request_signer.region_name,
            self.client._request_signer.signature_version)
        auth.add_auth(request)

    def _create_prepared_request(self, params, operation_model):
        request = create_request_object(params)
        self._sign_request(request)
        prepared_request = self.client._endpoint.prepare_request(request)
        if self._extra_headers is not None:
            prepared_request.headers.update(self._extra_headers)
        return prepared_request

    def dispatch(self, operation_name, operation_kwargs):
        """
        Dispatches `operation_name` with arguments `operation_kwargs`

        Raises TableDoesNotExist if the specified table does not exist
        """
        if operation_name not in [DESCRIBE_TABLE, LIST_TABLES, UPDATE_TABLE, UPDATE_TIME_TO_LIVE, DELETE_TABLE, CREATE_TABLE]:
            if RETURN_CONSUMED_CAPACITY not in operation_kwargs:
                operation_kwargs.update(self.get_consumed_capacity_map(TOTAL))
        self._log_debug(operation_name, operation_kwargs)

        table_name = operation_kwargs.get(TABLE_NAME)
        req_uuid = uuid.uuid4()

        self.send_pre_boto_callback(operation_name, req_uuid, table_name)
        data = self._make_api_call(operation_name, operation_kwargs)
        self.send_post_boto_callback(operation_name, req_uuid, table_name)

        if data and CONSUMED_CAPACITY in data:
            capacity = data.get(CONSUMED_CAPACITY)
            if isinstance(capacity, dict) and CAPACITY_UNITS in capacity:
                capacity = capacity.get(CAPACITY_UNITS)
            log.debug("%s %s consumed %s units",  data.get(TABLE_NAME, ''), operation_name, capacity)
        return data

    def send_post_boto_callback(self, operation_name, req_uuid, table_name):
        try:
            post_dynamodb_send.send(self, operation_name=operation_name, table_name=table_name, req_uuid=req_uuid)
        except Exception as e:
            log.exception("post_boto callback threw an exception.")

    def send_pre_boto_callback(self, operation_name, req_uuid, table_name):
        try:
            pre_dynamodb_send.send(self, operation_name=operation_name, table_name=table_name, req_uuid=req_uuid)
        except Exception as e:
            log.exception("pre_boto callback threw an exception.")

    def _make_api_call(self, operation_name, operation_kwargs):
        """
        This private method is here for two reasons:
        1. It's faster to avoid using botocore's response parsing
        2. It provides a place to monkey patch HTTP requests for unit testing
        """
        operation_model = self.client._service_model.operation_model(operation_name)
        request_dict = self.client._convert_to_request_dict(
            operation_kwargs,
            operation_model,
        )

        for i in range(0, self._max_retry_attempts_exception + 1):
            attempt_number = i + 1
            is_last_attempt_for_exceptions = i == self._max_retry_attempts_exception

            http_response = None
            prepared_request = None
            try:
                if prepared_request is not None:
                    # If there is a stream associated with the request, we need
                    # to reset it before attempting to send the request again.
                    # This will ensure that we resend the entire contents of the
                    # body.
                    prepared_request.reset_stream()

                # Create a new request for each retry (including a new signature).
                prepared_request = self._create_prepared_request(request_dict, operation_model)

                # Implement the before-send event from botocore
                event_name = 'before-send.dynamodb.{}'.format(operation_model.name)
                event_responses = self.client._endpoint._event_emitter.emit(event_name, request=prepared_request)
                event_response = first_non_none_response(event_responses)

                if event_response is None:
                    http_response = self.client._endpoint.http_session.send(prepared_request)
                else:
                    http_response = event_response
                    is_last_attempt_for_exceptions = True  # don't retry if we have an event response

                # json.loads accepts bytes in >= 3.6.0
                if sys.version_info < (3, 6, 0):
                    data = json.loads(http_response.text)
                else:
                    data = json.loads(http_response.content)
            except (ValueError, botocore.exceptions.HTTPClientError, botocore.exceptions.ConnectionError) as e:
                if is_last_attempt_for_exceptions:
                    log.debug('Reached the maximum number of retry attempts: %s', attempt_number)
                    if http_response:
                        e.args += (http_response.text,)
                    raise
                else:
                    # No backoff for fast-fail exceptions that likely failed at the frontend
                    log.debug(
                        'Retry needed for (%s) after attempt %s, retryable %s caught: %s',
                        operation_name,
                        attempt_number,
                        e.__class__.__name__,
                        e
                    )
                    continue

            status_code = http_response.status_code
            headers = http_response.headers
            if status_code >= 300:
                # Extract error code from __type
                code = data.get('__type', '')
                if '#' in code:
                    code = code.rsplit('#', 1)[1]
                botocore_expected_format = {'Error': {'Message': data.get('message', ''), 'Code': code}}
                verbose_properties = {
                    'request_id': headers.get('x-amzn-RequestId')
                }

                if 'RequestItems' in operation_kwargs:
                    # Batch operations can hit multiple tables, report them comma separated
                    verbose_properties['table_name'] = ','.join(operation_kwargs['RequestItems'])
                else:
                    verbose_properties['table_name'] = operation_kwargs.get('TableName')

                try:
                    raise VerboseClientError(botocore_expected_format, operation_name, verbose_properties)
                except VerboseClientError as e:
                    if is_last_attempt_for_exceptions:
                        log.debug('Reached the maximum number of retry attempts: %s', attempt_number)
                        raise
                    elif status_code < 500 and code not in RATE_LIMITING_ERROR_CODES:
                        # We don't retry on a ConditionalCheckFailedException or other 4xx (except for
                        # throughput related errors) because we assume they will fail in perpetuity.
                        # Retrying when there is already contention could cause other problems
                        # in part due to unnecessary consumption of throughput.
                        raise
                    else:
                        # We use fully-jittered exponentially-backed-off retries:
                        #  https://www.awsarchitectureblog.com/2015/03/backoff.html
                        sleep_time_ms = random.randint(0, self._base_backoff_ms * (2 ** i))
                        log.debug(
                            'Retry with backoff needed for (%s) after attempt %s,'
                            'sleeping for %s milliseconds, retryable %s caught: %s',
                            operation_name,
                            attempt_number,
                            sleep_time_ms,
                            e.__class__.__name__,
                            e
                        )
                        time.sleep(sleep_time_ms / 1000.0)
                        continue

            return self._handle_binary_attributes(data)

    @staticmethod
    def _handle_binary_attributes(data):
        """ Simulate botocore's binary attribute handling """
        if ITEM in data:
            for attr in six.itervalues(data[ITEM]):
                _convert_binary(attr)
        if ITEMS in data:
            for item in data[ITEMS]:
                for attr in six.itervalues(item):
                    _convert_binary(attr)
        if RESPONSES in data:
            if isinstance(data[RESPONSES], list):
                for item in data[RESPONSES]:
                    for attr in six.itervalues(item):
                        _convert_binary(attr)
            else:
                for item_list in six.itervalues(data[RESPONSES]):
                    for item in item_list:
                        for attr in six.itervalues(item):
                            _convert_binary(attr)
        if LAST_EVALUATED_KEY in data:
            for attr in six.itervalues(data[LAST_EVALUATED_KEY]):
                _convert_binary(attr)
        if UNPROCESSED_KEYS in data:
            for table_data in six.itervalues(data[UNPROCESSED_KEYS]):
                for item in table_data[KEYS]:
                    for attr in six.itervalues(item):
                        _convert_binary(attr)
        if UNPROCESSED_ITEMS in data:
            for table_unprocessed_requests in six.itervalues(data[UNPROCESSED_ITEMS]):
                for request in table_unprocessed_requests:
                    for item_mapping in six.itervalues(request):
                        for item in six.itervalues(item_mapping):
                            for attr in six.itervalues(item):
                                _convert_binary(attr)
        if ATTRIBUTES in data:
            for attr in six.itervalues(data[ATTRIBUTES]):
                _convert_binary(attr)
        return data

    @property
    def session(self):
        """
        Returns a valid botocore session
        """
        # botocore client creation is not thread safe as of v1.2.5+ (see issue #153)
        if getattr(self._local, 'session', None) is None:
            self._local.session = get_session()
        return self._local.session

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
            config = botocore.client.Config(
                parameter_validation=False,  # Disable unnecessary validation for performance
                connect_timeout=self._connect_timeout_seconds,
                read_timeout=self._read_timeout_seconds,
                max_pool_connections=self._max_pool_connections)
            self._client = self.session.create_client(SERVICE_NAME, self.region, endpoint_url=self.host, config=config)
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
                six.raise_from(TableError("Unable to describe table: {}".format(e), e), None)
            except ClientError as e:
                if 'ResourceNotFound' in e.response['Error']['Code']:
                    six.raise_from(TableDoesNotExist(e.response['Error']['Message']), None)
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
                     stream_specification=None,
                     billing_mode=DEFAULT_BILLING_MODE):
        """
        Performs the CreateTable operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name,
            BILLING_MODE: billing_mode,
            PROVISIONED_THROUGHPUT: {
                READ_CAPACITY_UNITS: read_capacity_units,
                WRITE_CAPACITY_UNITS: write_capacity_units,
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

        if billing_mode not in AVAILABLE_BILLING_MODES:
            raise ValueError("incorrect value for billing_mode, available modes: {}".format(AVAILABLE_BILLING_MODES))
        if billing_mode == PAY_PER_REQUEST_BILLING_MODE:
            del operation_kwargs[PROVISIONED_THROUGHPUT]
        elif billing_mode == PROVISIONED_BILLING_MODE:
            del operation_kwargs[BILLING_MODE]

        if global_secondary_indexes:
            global_secondary_indexes_list = []
            for index in global_secondary_indexes:
                index_kwargs = {
                    INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                    KEY_SCHEMA: sorted(index.get(pythonic(KEY_SCHEMA)), key=lambda x: x.get(KEY_TYPE)),
                    PROJECTION: index.get(pythonic(PROJECTION)),
                    PROVISIONED_THROUGHPUT: index.get(pythonic(PROVISIONED_THROUGHPUT))
                }
                if billing_mode == PAY_PER_REQUEST_BILLING_MODE:
                    del index_kwargs[PROVISIONED_THROUGHPUT]
                global_secondary_indexes_list.append(index_kwargs)
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
            six.raise_from(TableError("Failed to create table: {}".format(e), e), None)
        return data

    def update_time_to_live(self, table_name, ttl_attribute_name):
        """
        Performs the UpdateTimeToLive operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name,
            TIME_TO_LIVE_SPECIFICATION: {
                ATTR_NAME: ttl_attribute_name,
                ENABLED: True,
            }
        }
        try:
            return self.dispatch(UPDATE_TIME_TO_LIVE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(TableError("Failed to update TTL on table: {}".format(e), e), None)

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
            six.raise_from(TableError("Failed to delete table: {}".format(e), e), None)
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
            six.raise_from(TableError("Failed to update table: {}".format(e), e), None)

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
            six.raise_from(TableError("Unable to list tables: {}".format(e), e), None)

    def describe_table(self, table_name):
        """
        Performs the DescribeTable operation
        """
        try:
            tbl = self.get_meta_table(table_name, refresh=True)
            if tbl:
                return tbl.data
        except ValueError:
            pass
        raise TableDoesNotExist(table_name)

    def get_item_attribute_map(self, table_name, attributes, item_key=ITEM, pythonic_key=True):
        """
        Builds up a dynamodb compatible AttributeValue map
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_item_attribute_map(
            attributes,
            item_key=item_key,
            pythonic_key=pythonic_key)

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
            raise ValueError("Invalid attribute supplied: {}".format(attribute))
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
            raise TableError("No such table {}".format(table_name))
        return tbl.get_attribute_type(attribute_name, value=value)

    def get_identifier_map(self, table_name, hash_key, range_key=None, key=KEY):
        """
        Builds the identifier map that is common to several operations
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_identifier_map(hash_key, range_key=range_key, key=key)

    def get_consumed_capacity_map(self, return_consumed_capacity):
        """
        Builds the consumed capacity map that is common to several operations
        """
        if return_consumed_capacity.upper() not in RETURN_CONSUMED_CAPACITY_VALUES:
            raise ValueError("{} must be one of {}".format(RETURN_ITEM_COLL_METRICS, RETURN_CONSUMED_CAPACITY_VALUES))
        return {
            RETURN_CONSUMED_CAPACITY: str(return_consumed_capacity).upper()
        }

    def get_return_values_map(self, return_values):
        """
        Builds the return values map that is common to several operations
        """
        if return_values.upper() not in RETURN_VALUES_VALUES:
            raise ValueError("{} must be one of {}".format(RETURN_VALUES, RETURN_VALUES_VALUES))
        return {
            RETURN_VALUES: str(return_values).upper()
        }

    def get_return_values_on_condition_failure_map(self, return_values_on_condition_failure):
        """
        Builds the return values map that is common to several operations
        """
        if return_values_on_condition_failure.upper() not in RETURN_VALUES_VALUES:
            raise ValueError("{} must be one of {}".format(
                RETURN_VALUES_ON_CONDITION_FAILURE,
                RETURN_VALUES_ON_CONDITION_FAILURE_VALUES
            ))
        return {
            RETURN_VALUES_ON_CONDITION_FAILURE: str(return_values_on_condition_failure).upper()
        }

    def get_item_collection_map(self, return_item_collection_metrics):
        """
        Builds the item collection map
        """
        if return_item_collection_metrics.upper() not in RETURN_ITEM_COLL_METRICS_VALUES:
            raise ValueError("{} must be one of {}".format(RETURN_ITEM_COLL_METRICS, RETURN_ITEM_COLL_METRICS_VALUES))
        return {
            RETURN_ITEM_COLL_METRICS: str(return_item_collection_metrics).upper()
        }

    def get_exclusive_start_key_map(self, table_name, exclusive_start_key):
        """
        Builds the exclusive start key attribute map
        """
        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_exclusive_start_key_map(exclusive_start_key)

    def get_operation_kwargs(self,
                             table_name,
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
        self._check_condition('condition', condition)

        operation_kwargs = {}
        name_placeholders = {}
        expression_attribute_values = {}

        operation_kwargs[TABLE_NAME] = table_name
        operation_kwargs.update(self.get_identifier_map(table_name, hash_key, range_key, key=key))
        if attributes and operation_kwargs.get(ITEM) is not None:
            attrs = self.get_item_attribute_map(table_name, attributes)
            operation_kwargs[ITEM].update(attrs[ITEM])
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(attributes_to_get, name_placeholders)
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
        if condition is not None:
            condition_expression = condition.serialize(name_placeholders, expression_attribute_values)
            operation_kwargs[CONDITION_EXPRESSION] = condition_expression
        if consistent_read is not None:
            operation_kwargs[CONSISTENT_READ] = consistent_read
        if return_values is not None:
            operation_kwargs.update(self.get_return_values_map(return_values))
        if return_values_on_condition_failure is not None:
            operation_kwargs.update(self.get_return_values_on_condition_failure_map(return_values_on_condition_failure))
        if return_consumed_capacity is not None:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics is not None:
            operation_kwargs.update(self.get_item_collection_map(return_item_collection_metrics))
        if actions is not None:
            update_expression = Update(*actions)
            operation_kwargs[UPDATE_EXPRESSION] = update_expression.serialize(
                name_placeholders,
                expression_attribute_values
            )
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values
        return operation_kwargs

    def delete_item(self,
                    table_name,
                    hash_key,
                    range_key=None,
                    condition=None,
                    return_values=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=None):
        """
        Performs the DeleteItem operation and returns the result
        """
        operation_kwargs = self.get_operation_kwargs(
            table_name,
            hash_key,
            range_key=range_key,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        try:
            return self.dispatch(DELETE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(DeleteError("Failed to delete item: {}".format(e), e), None)

    def update_item(self,
                    table_name,
                    hash_key,
                    range_key=None,
                    actions=None,
                    condition=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=None,
                    return_values=None):
        """
        Performs the UpdateItem operation
        """
        if not actions:
            raise ValueError("'actions' cannot be empty")

        operation_kwargs = self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            actions=actions,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        try:
            return self.dispatch(UPDATE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(UpdateError("Failed to update item: {}".format(e), e), None)

    def put_item(self,
                 table_name,
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
        operation_kwargs = self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            key=ITEM,
            attributes=attributes,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        try:
            return self.dispatch(PUT_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(PutError("Failed to put item: {}".format(e), e), None)

    def _get_transact_operation_kwargs(self,
                                       client_request_token=None,
                                       return_consumed_capacity=None,
                                       return_item_collection_metrics=None):
        operation_kwargs = {}
        if client_request_token is not None:
            operation_kwargs[CLIENT_REQUEST_TOKEN] = client_request_token
        if return_consumed_capacity is not None:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics is not None:
            operation_kwargs.update(self.get_item_collection_map(return_item_collection_metrics))

        return operation_kwargs

    def transact_write_items(self,
                             condition_check_items,
                             delete_items,
                             put_items,
                             update_items,
                             client_request_token=None,
                             return_consumed_capacity=None,
                             return_item_collection_metrics=None):
        """
        Performs the TransactWrite operation and returns the result
        """
        transact_items = []
        transact_items.extend(
            {TRANSACT_CONDITION_CHECK: item} for item in condition_check_items
        )
        transact_items.extend(
            {TRANSACT_DELETE: item} for item in delete_items
        )
        transact_items.extend(
            {TRANSACT_PUT: item} for item in put_items
        )
        transact_items.extend(
            {TRANSACT_UPDATE: item} for item in update_items
        )

        operation_kwargs = self._get_transact_operation_kwargs(
            client_request_token=client_request_token,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        operation_kwargs[TRANSACT_ITEMS] = transact_items

        try:
            return self.dispatch(TRANSACT_WRITE_ITEMS, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(TransactWriteError("Failed to write transaction items", e), None)

    def transact_get_items(self, get_items, return_consumed_capacity=None):
        """
        Performs the TransactGet operation and returns the result
        """
        operation_kwargs = self._get_transact_operation_kwargs(return_consumed_capacity=return_consumed_capacity)
        operation_kwargs[TRANSACT_ITEMS] = [
            {TRANSACT_GET: item} for item in get_items
        ]

        try:
            return self.dispatch(TRANSACT_GET_ITEMS, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(TransactGetError("Failed to get transaction items", e), None)

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
            six.raise_from(PutError("Failed to batch write items: {}".format(e), e), None)

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
        name_placeholders = {}
        if consistent_read:
            args_map[CONSISTENT_READ] = consistent_read
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(attributes_to_get, name_placeholders)
            args_map[PROJECTION_EXPRESSION] = projection_expression
        if name_placeholders:
            args_map[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
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
            six.raise_from(GetError("Failed to batch get items: {}".format(e), e), None)

    def get_item(self,
                 table_name,
                 hash_key,
                 range_key=None,
                 consistent_read=False,
                 attributes_to_get=None):
        """
        Performs the GetItem operation and returns the result
        """
        operation_kwargs = self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get
        )
        try:
            return self.dispatch(GET_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(GetError("Failed to get item: {}".format(e), e), None)

    def scan(self,
             table_name,
             filter_condition=None,
             attributes_to_get=None,
             limit=None,
             return_consumed_capacity=None,
             exclusive_start_key=None,
             segment=None,
             total_segments=None,
             consistent_read=None,
             index_name=None):
        """
        Performs the scan operation
        """
        self._check_condition('filter_condition', filter_condition)

        operation_kwargs = {TABLE_NAME: table_name}
        name_placeholders = {}
        expression_attribute_values = {}

        if filter_condition is not None:
            filter_expression = filter_condition.serialize(name_placeholders, expression_attribute_values)
            operation_kwargs[FILTER_EXPRESSION] = filter_expression
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(attributes_to_get, name_placeholders)
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
        if index_name:
            operation_kwargs[INDEX_NAME] = index_name
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
        if consistent_read:
            operation_kwargs[CONSISTENT_READ] = consistent_read
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values

        try:
            return self.dispatch(SCAN, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(ScanError("Failed to scan table: {}".format(e), e), None)

    def query(self,
              table_name,
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
              select=None):
        """
        Performs the Query operation and returns the result
        """
        self._check_condition('range_key_condition', range_key_condition)
        self._check_condition('filter_condition', filter_condition)

        operation_kwargs = {TABLE_NAME: table_name}
        name_placeholders = {}
        expression_attribute_values = {}

        tbl = self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table: {}".format(table_name))
        if index_name:
            if not tbl.has_index_name(index_name):
                raise ValueError("Table {} has no index: {}".format(table_name, index_name))
            hash_keyname = tbl.get_index_hash_keyname(index_name)
            if not hash_keyname:
                raise ValueError("No hash key attribute for index: {}".format(index_name))
            range_keyname = tbl.get_index_range_keyname(index_name)
        else:
            hash_keyname = tbl.hash_keyname
            range_keyname = tbl.range_keyname

        hash_condition_value = {self.get_attribute_type(table_name, hash_keyname, hash_key): self.parse_attribute(hash_key)}
        key_condition = getattr(Path([hash_keyname]), '__eq__')(hash_condition_value)

        if range_key_condition is not None:
            if range_key_condition.is_valid_range_key_condition(range_keyname):
                key_condition = key_condition & range_key_condition
            elif filter_condition is None:
                # Try to gracefully handle the case where a user passed in a filter as a range key condition
                (filter_condition, range_key_condition) = (range_key_condition, None)
            else:
                raise ValueError("{} is not a valid range key condition".format(range_key_condition))

        operation_kwargs[KEY_CONDITION_EXPRESSION] = key_condition.serialize(
            name_placeholders, expression_attribute_values)
        if filter_condition is not None:
            filter_expression = filter_condition.serialize(name_placeholders, expression_attribute_values)
            # FilterExpression does not allow key attributes. Check for hash and range key name placeholders
            hash_key_placeholder = name_placeholders.get(hash_keyname)
            range_key_placeholder = range_keyname and name_placeholders.get(range_keyname)
            if (
                hash_key_placeholder in filter_expression or
                (range_key_placeholder and range_key_placeholder in filter_expression)
            ):
                raise ValueError("'filter_condition' cannot contain key attributes")
            operation_kwargs[FILTER_EXPRESSION] = filter_expression
        if attributes_to_get:
            projection_expression = create_projection_expression(attributes_to_get, name_placeholders)
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
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
        if select:
            if select.upper() not in SELECT_VALUES:
                raise ValueError("{} must be one of {}".format(SELECT, SELECT_VALUES))
            operation_kwargs[SELECT] = str(select).upper()
        if scan_index_forward is not None:
            operation_kwargs[SCAN_INDEX_FORWARD] = scan_index_forward
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values

        try:
            return self.dispatch(QUERY, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            six.raise_from(QueryError("Failed to query items: {}".format(e), e), None)

    def _check_condition(self, name, condition):
        if condition is not None:
            if not isinstance(condition, Condition):
                raise ValueError("'{}' must be an instance of Condition".format(name))

    @staticmethod
    def _reverse_dict(d):
        return {v: k for k, v in six.iteritems(d)}


def _convert_binary(attr):
    if BINARY_SHORT in attr:
        attr[BINARY_SHORT] = b64decode(attr[BINARY_SHORT].encode(DEFAULT_ENCODING))
    elif BINARY_SET_SHORT in attr:
        value = attr[BINARY_SET_SHORT]
        if value and len(value):
            attr[BINARY_SET_SHORT] = {b64decode(v.encode(DEFAULT_ENCODING)) for v in value}
