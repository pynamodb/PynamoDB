"""
Pynamodb constants
"""

# Operations
BATCH_WRITE_ITEM = 'BatchWriteItem'
DESCRIBE_TABLE = 'DescribeTable'
BATCH_GET_ITEM = 'BatchGetItem'
CREATE_TABLE = 'CreateTable'
UPDATE_TABLE = 'UpdateTable'
DELETE_TABLE = 'DeleteTable'
LIST_TABLES = 'ListTables'
UPDATE_ITEM = 'UpdateItem'
DELETE_ITEM = 'DeleteItem'
GET_ITEM = 'GetItem'
PUT_ITEM = 'PutItem'
QUERY = 'Query'
SCAN = 'Scan'

# Request Parameters
GLOBAL_SECONDARY_INDEX_UPDATES = 'GlobalSecondaryIndexUpdates'
RETURN_ITEM_COLL_METRICS = 'ReturnItemCollectionMetrics'
EXCLUSIVE_START_TABLE_NAME = 'ExclusiveStartTableName'
RETURN_CONSUMED_CAPACITY = 'ReturnConsumedCapacity'
COMPARISON_OPERATOR = 'ComparisonOperator'
SCAN_INDEX_FORWARD = 'ScanIndexForward'
ATTR_DEFINITIONS = 'AttributeDefinitions'
ATTR_VALUE_LIST = 'AttributeValueList'
TABLE_DESCRIPTION = 'TableDescription'
UNPROCESSED_KEYS = 'UnprocessedKeys'
UNPROCESSED_ITEMS = 'UnprocessedItems'
CONSISTENT_READ = 'ConsistentRead'
DELETE_REQUEST = 'DeleteRequest'
RETURN_VALUES = 'ReturnValues'
REQUEST_ITEMS = 'RequestItems'
ATTRS_TO_GET = 'AttributesToGet'
ATTR_UPDATES = 'AttributeUpdates'
TABLE_STATUS = 'TableStatus'
SCAN_FILTER = 'ScanFilter'
TABLE_NAME = 'TableName'
KEY_SCHEMA = 'KeySchema'
ATTR_NAME = 'AttributeName'
ATTR_TYPE = 'AttributeType'
ITEM_COUNT = 'ItemCount'
CAMEL_COUNT = 'Count'
PUT_REQUEST = 'PutRequest'
INDEX_NAME = 'IndexName'
ATTRIBUTES = 'Attributes'
TABLE_KEY = 'Table'
RESPONSES = 'Responses'
RANGE_KEY = 'RangeKey'
KEY_TYPE = 'KeyType'
ACTION = 'Action'
UPDATE = 'Update'
EXISTS = 'Exists'
SELECT = 'Select'
ACTIVE = 'ACTIVE'
LIMIT = 'Limit'
ITEMS = 'Items'
ITEM = 'Item'
KEYS = 'Keys'
UTC = 'UTC'
KEY = 'Key'

# Defaults
DEFAULT_ENCODING = 'utf-8'
DEFAULT_REGION = 'us-east-1'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'
SERVICE_NAME = 'dynamodb'
HTTP_OK = 200
HTTP_BAD_REQUEST = 400

# Create Table arguments
PROVISIONED_THROUGHPUT = 'ProvisionedThroughput'
READ_CAPACITY_UNITS = 'ReadCapacityUnits'
WRITE_CAPACITY_UNITS = 'WriteCapacityUnits'

STRING_SHORT = 'S'
STRING_SET_SHORT = 'SS'
NUMBER_SHORT = 'N'
NUMBER_SET_SHORT = 'NS'
BINARY_SHORT = 'B'
BINARY_SET_SHORT = 'BS'
STRING = 'String'
STRING_SET = 'StringSet'
NUMBER = 'Number'
NUMBER_SET = 'NumberSet'
BINARY = 'Binary'
BINARY_SET = 'BinarySet'
SHORT_ATTR_TYPES = [STRING_SHORT, STRING_SET_SHORT, NUMBER_SHORT, NUMBER_SET_SHORT, BINARY_SHORT, BINARY_SET_SHORT]
ATTR_TYPE_MAP = {
    STRING: STRING_SHORT,
    STRING_SET: STRING_SET_SHORT,
    NUMBER: NUMBER_SHORT,
    NUMBER_SET: NUMBER_SET_SHORT,
    BINARY: BINARY_SHORT,
    BINARY_SET: BINARY_SET_SHORT,
    STRING_SHORT: STRING,
    STRING_SET_SHORT: STRING_SET,
    NUMBER_SHORT: NUMBER,
    NUMBER_SET_SHORT: NUMBER_SET,
    BINARY_SHORT: BINARY,
    BINARY_SET_SHORT: BINARY_SET
}
# Constants needed for creating indexes
LOCAL_SECONDARY_INDEX = 'LocalSecondaryIndex'
LOCAL_SECONDARY_INDEXES = 'LocalSecondaryIndexes'
GLOBAL_SECONDARY_INDEX = 'GlobalSecondaryIndex'
GLOBAL_SECONDARY_INDEXES = 'GlobalSecondaryIndexes'
PROJECTION = 'Projection'
PROJECTION_TYPE = 'ProjectionType'
NON_KEY_ATTRIBUTES = 'NonKeyAttributes'
KEYS_ONLY = 'KEYS_ONLY'
ALL = 'ALL'
INCLUDE = 'INCLUDE'

# Constants for Dynamodb Streams
STREAM_VIEW_TYPE = 'StreamViewType'
STREAM_SPECIFICATION = 'StreamSpecification'
STREAM_ENABLED = 'StreamEnabled'
STREAM_NEW_IMAGE = 'NEW_IMAGE'
STREAM_OLD_IMAGE = 'OLD_IMAGE'
STREAM_NEW_AND_OLD_IMAGE = 'NEW_AND_OLD_IMAGES'
STREAM_KEYS_ONLY = 'KEYS_ONLY'

# These are constants used in the KeyConditions parameter
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditions
EXCLUSIVE_START_KEY = 'ExclusiveStartKey'
LAST_EVALUATED_KEY = 'LastEvaluatedKey'
QUERY_FILTER = 'QueryFilter'
BEGINS_WITH = 'BEGINS_WITH'
BETWEEN = 'BETWEEN'
EQ = 'EQ'
NE = 'NE'
LE = 'LE'
LT = 'LT'
GE = 'GE'
GT = 'GT'
IN = 'IN'
KEY_CONDITIONS = 'KeyConditions'
COMPARISON_OPERATOR_VALUES = [EQ, LE, LT, GE, GT, BEGINS_WITH, BETWEEN]
QUERY_OPERATOR_MAP = {
    'eq': EQ,
    'le': LE,
    'lt': LT,
    'ge': GE,
    'gt': GT,
    'begins_with': BEGINS_WITH,
    'between': BETWEEN
}

# These are the valid select values for the Scan operation
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-Select
NOT_NULL = 'NOT_NULL'
NULL = 'NULL'
CONTAINS = 'CONTAINS'
NOT_CONTAINS = 'NOT_CONTAINS'
ALL_ATTRIBUTES = 'ALL_ATTRIBUTES'
ALL_PROJECTED_ATTRIBUTES = 'ALL_PROJECTED_ATTRIBUTES'
SPECIFIC_ATTRIBUTES = 'SPECIFIC_ATTRIBUTES'
COUNT = 'COUNT'
SELECT_VALUES = [ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT]
SCAN_OPERATOR_MAP = {
    'eq': EQ,
    'ne': NE,
    'le': LE,
    'lt': LT,
    'ge': GE,
    'gt': GT,
    'not_null': NOT_NULL,
    'null': NULL,
    'contains': CONTAINS,
    'not_contains': NOT_CONTAINS,
    'begins_with': BEGINS_WITH,
    'in': IN,
    'between': BETWEEN
}
QUERY_FILTER_OPERATOR_MAP = SCAN_OPERATOR_MAP
DELETE_FILTER_OPERATOR_MAP = SCAN_OPERATOR_MAP
UPDATE_FILTER_OPERATOR_MAP = SCAN_OPERATOR_MAP
PUT_FILTER_OPERATOR_MAP = SCAN_OPERATOR_MAP


# These are the valid comparison operators for the Scan operation
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-ScanFilter
SEGMENT = 'Segment'
TOTAL_SEGMENTS = 'TotalSegments'
SCAN_FILTER_VALUES = [EQ, NE, LE, LT, GE, GT, NOT_NULL, NULL, CONTAINS, NOT_CONTAINS, BEGINS_WITH, IN, BETWEEN]
QUERY_FILTER_VALUES = SCAN_FILTER_VALUES
DELETE_FILTER_VALUES = SCAN_FILTER_VALUES


# These are constants used in the expected condition for PutItem
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-Expected
VALUE = 'Value'
EXPECTED = 'Expected'

# These are the valid ReturnConsumedCapacity values used in multiple operations
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#DDB-BatchGetItem-request-ReturnConsumedCapacity
CONSUMED_CAPACITY = 'ConsumedCapacity'
CAPACITY_UNITS = 'CapacityUnits'
INDEXES = 'INDEXES'
TOTAL = 'TOTAL'
NONE = 'NONE'
RETURN_CONSUMED_CAPACITY_VALUES = [INDEXES, TOTAL, NONE]

# These are the valid ReturnItemCollectionMetrics values used in multiple operations
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#DDB-BatchWriteItem-request-ReturnItemCollectionMetrics
SIZE = 'SIZE'
RETURN_ITEM_COLL_METRICS_VALUES = [SIZE, NONE]

# These are the valid ReturnValues values used in the PutItem operation
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ReturnValues
ALL_OLD = 'ALL_OLD'
UPDATED_OLD = 'UPDATED_OLD'
ALL_NEW = 'ALL_NEW'
UPDATED_NEW = 'UPDATED_NEW'
RETURN_VALUES_VALUES = [NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW]

# These are constants used in the AttributeUpdates parameter for UpdateItem
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-AttributeUpdates
PUT = 'PUT'
DELETE = 'DELETE'
ADD = 'ADD'
ATTR_UPDATE_ACTIONS = [PUT, DELETE, ADD]
BATCH_GET_PAGE_LIMIT = 100
BATCH_WRITE_PAGE_LIMIT = 25

META_CLASS_NAME = "Meta"
REGION = "region"
HOST = "host"

# The constants are needed for the ConditionalOperator argument used
# UpdateItem, PutItem and DeleteItem
CONDITIONAL_OPERATOR = 'ConditionalOperator'
AND = 'AND'
OR = 'OR'
CONDITIONAL_OPERATORS = [AND, OR]
