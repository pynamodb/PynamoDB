"""
Pynamodb constants
"""

# Operations
TRANSACT_WRITE_ITEMS = 'TransactWriteItems'
TRANSACT_GET_ITEMS = 'TransactGetItems'
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
RETURN_VALUES_ON_CONDITION_FAILURE = 'ReturnValuesOnConditionCheckFailure'
GLOBAL_SECONDARY_INDEX_UPDATES = 'GlobalSecondaryIndexUpdates'
RETURN_ITEM_COLL_METRICS = 'ReturnItemCollectionMetrics'
EXCLUSIVE_START_TABLE_NAME = 'ExclusiveStartTableName'
RETURN_CONSUMED_CAPACITY = 'ReturnConsumedCapacity'
CLIENT_REQUEST_TOKEN = 'ClientRequestToken'
COMPARISON_OPERATOR = 'ComparisonOperator'
SCAN_INDEX_FORWARD = 'ScanIndexForward'
ATTR_DEFINITIONS = 'AttributeDefinitions'
TABLE_DESCRIPTION = 'TableDescription'
UNPROCESSED_KEYS = 'UnprocessedKeys'
UNPROCESSED_ITEMS = 'UnprocessedItems'
CONSISTENT_READ = 'ConsistentRead'
DELETE_REQUEST = 'DeleteRequest'
TRANSACT_ITEMS = 'TransactItems'
RETURN_VALUES = 'ReturnValues'
REQUEST_ITEMS = 'RequestItems'
ATTRS_TO_GET = 'AttributesToGet'
TABLE_STATUS = 'TableStatus'
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
UPDATE = 'Update'
SELECT = 'Select'
ACTIVE = 'ACTIVE'
LIMIT = 'Limit'
ITEMS = 'Items'
ITEM = 'Item'
TAGS = 'Tags'
KEYS = 'Keys'
UTC = 'UTC'
KEY = 'Key'
GET = 'Get'

# transaction operators
TRANSACT_CONDITION_CHECK = 'ConditionCheck'
TRANSACT_DELETE = 'Delete'
TRANSACT_GET = 'Get'
TRANSACT_PUT = 'Put'
TRANSACT_UPDATE = 'Update'

ACTION = 'Action'

# Response Parameters
SCANNED_COUNT = 'ScannedCount'

# Expression Parameters
CONDITION_EXPRESSION = 'ConditionExpression'
EXPRESSION_ATTRIBUTE_NAMES = 'ExpressionAttributeNames'
EXPRESSION_ATTRIBUTE_VALUES = 'ExpressionAttributeValues'
FILTER_EXPRESSION = 'FilterExpression'
KEY_CONDITION_EXPRESSION = 'KeyConditionExpression'
PROJECTION_EXPRESSION = 'ProjectionExpression'
UPDATE_EXPRESSION = 'UpdateExpression'

# Billing Modes
PAY_PER_REQUEST_BILLING_MODE = 'PAY_PER_REQUEST'
PROVISIONED_BILLING_MODE = 'PROVISIONED'
AVAILABLE_BILLING_MODES = [PROVISIONED_BILLING_MODE, PAY_PER_REQUEST_BILLING_MODE]

# Defaults
DEFAULT_ENCODING = 'utf-8'
DEFAULT_REGION = 'us-east-1'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'
SERVICE_NAME = 'dynamodb'
HTTP_OK = 200
HTTP_BAD_REQUEST = 400
DEFAULT_BILLING_MODE = PROVISIONED_BILLING_MODE

# Create Table arguments
PROVISIONED_THROUGHPUT = 'ProvisionedThroughput'
READ_CAPACITY_UNITS = 'ReadCapacityUnits'
WRITE_CAPACITY_UNITS = 'WriteCapacityUnits'
BILLING_MODE = 'BillingMode'

# Attribute Types
BINARY = 'B'
BINARY_SET = 'BS'
BOOLEAN = 'BOOL'
LIST = 'L'
MAP = 'M'
NULL = 'NULL'
NUMBER = 'N'
NUMBER_SET = 'NS'
STRING = 'S'
STRING_SET = 'SS'

ATTRIBUTE_TYPES = [BINARY, BINARY_SET, BOOLEAN, LIST, MAP, NULL, NUMBER, NUMBER_SET, STRING, STRING_SET]

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

# Constants for updating a table's TTL
UPDATE_TIME_TO_LIVE = 'UpdateTimeToLive'
TIME_TO_LIVE_SPECIFICATION = 'TimeToLiveSpecification'
ENABLED = 'Enabled'

# These are constants used in the KeyConditionExpression parameter
# http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
EXCLUSIVE_START_KEY = 'ExclusiveStartKey'
LAST_EVALUATED_KEY = 'LastEvaluatedKey'

# These are the valid select values for the Scan operation
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-Select
ALL_ATTRIBUTES = 'ALL_ATTRIBUTES'
ALL_PROJECTED_ATTRIBUTES = 'ALL_PROJECTED_ATTRIBUTES'
SPECIFIC_ATTRIBUTES = 'SPECIFIC_ATTRIBUTES'
COUNT = 'COUNT'
SELECT_VALUES = [ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT]

# These are the valid comparison operators for the Scan operation
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-ScanFilter
SEGMENT = 'Segment'
TOTAL_SEGMENTS = 'TotalSegments'

# These are constants used in the expected condition for PutItem
# See:
# http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-Expected
VALUE = 'Value'

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
RETURN_VALUES_ON_CONDITION_FAILURE_VALUES = [NONE, ALL_OLD]

# These are constants used in the AttributeUpdates parameter for UpdateItem
# See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-AttributeUpdates
PUT = 'PUT'
DELETE = 'DELETE'
ADD = 'ADD'
BATCH_GET_PAGE_LIMIT = 100
BATCH_WRITE_PAGE_LIMIT = 25

META_CLASS_NAME = "Meta"
REGION = "region"
HOST = "host"
