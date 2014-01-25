"""
Examples using DynamoDB indexes
"""
from datetime import datetime
from pynamodb.models import Model
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.attributes import UTCDateTimeAttribute, UnicodeAttribute, NumberAttribute


class ViewIndex(GlobalSecondaryIndex):
    """
    This class represents a global secondary index
    """
    read_capacity_units = 2
    write_capacity_units = 1
    # All attributes are projected
    projection = AllProjection()

    # This attribute is the hash key for the index
    # Note that this attribute must also exist
    # in the model
    view = NumberAttribute(default=0, hash_key=True)


class TestModel(Model):
    """
    A test model that uses a global secondary index
    """
    table_name = 'TestModel'
    forum = UnicodeAttribute(hash_key=True)
    thread = UnicodeAttribute(range_key=True)
    view_index = ViewIndex()
    view = NumberAttribute(default=0)


# Indexes can be queried easily using the index's hash key
for item in TestModel.view_index.query(1):
    print("Item queried from index: {0}".format(item))
