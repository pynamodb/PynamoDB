"""
Examples using DynamoDB indexes
"""
import datetime
from pynamodb.models import Model
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection, LocalSecondaryIndex
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute


class ViewIndex(GlobalSecondaryIndex):
    """
    This class represents a global secondary index
    """
    class Meta:
        # You can override the index name by setting it below
        index_name = "viewIdx"
        read_capacity_units = 1
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
    class Meta:
        table_name = "TestModel"
        # Set host for using DynamoDB Local
        host = "http://localhost:8000"
    forum = UnicodeAttribute(hash_key=True)
    thread = UnicodeAttribute(range_key=True)
    view_index = ViewIndex()
    view = NumberAttribute(default=0)

if not TestModel.exists():
    TestModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

# Create an item
item = TestModel('forum-example', 'thread-example')
item.view = 1
item.save()

# Indexes can be queried easily using the index's hash key
for item in TestModel.view_index.query(1):
    print("Item queried from index: {0}".format(item))


class GamePlayerOpponentIndex(LocalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GamePlayerOpponentIndex"
        host = "http://localhost:8000"
        projection = AllProjection()
    player_id = UnicodeAttribute(hash_key=True)
    winner_id = UnicodeAttribute(range_key=True)


class GameOpponentTimeIndex(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameOpponentTimeIndex"
        host = "http://localhost:8000"
        projection = AllProjection()
    winner_id = UnicodeAttribute(hash_key=True)
    created_time = UnicodeAttribute(range_key=True)


class GameModel(Model):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameModel"
        host = "http://localhost:8000"
    player_id = UnicodeAttribute(hash_key=True)
    created_time = UTCDateTimeAttribute(range_key=True)
    winner_id = UnicodeAttribute()
    loser_id = UnicodeAttribute(null=True)

    player_opponent_index = GamePlayerOpponentIndex()
    opponent_time_index = GameOpponentTimeIndex()

if not GameModel.exists():
    GameModel.create_table(wait=True)

# Create an item
item = GameModel('1234', datetime.datetime.utcnow())
item.winner_id = '5678'
item.save()

# Indexes can be queried easily using the index's hash key
for item in GameModel.player_opponent_index.query('1234'):
    print("Item queried from index: {0}".format(item))

# Count on an index
print(GameModel.player_opponent_index.count('1234'))
