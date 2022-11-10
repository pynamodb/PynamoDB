from typing import Dict
from typing import List

from typing_extensions import NotRequired
from typing_extensions import TypedDict


class SchemaAttrDefinition(TypedDict):
    AttributeName: str
    AttributeType: str


class KeySchema(TypedDict):
    AttributeName: str
    KeyType: str


class Projection(TypedDict):
    ProjectionType: str
    NonKeyAttributes: NotRequired[List[str]]


class IndexSchema(TypedDict):
    index_name: str
    key_schema: List[Dict[str, str]]
    projection: Dict[str, str]
    attribute_definitions: List[SchemaAttrDefinition]


class ProvisionedThroughput(TypedDict, total=False):
    ReadCapacityUnits: int
    WriteCapacityUnits: int


class GlobalSecondaryIndexSchema(IndexSchema):
    provisioned_throughput: ProvisionedThroughput


class ModelSchema(TypedDict):
    attribute_definitions: List[SchemaAttrDefinition]
    key_schema: List[KeySchema]
    global_secondary_indexes: List[GlobalSecondaryIndexSchema]
    local_secondary_indexes: List[IndexSchema]
