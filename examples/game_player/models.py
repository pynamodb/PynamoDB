from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, DiscriminatorAttribute, NumberAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection


# Step 1: Define Global Secondary Indexes (GSIs)

class InvertedIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'InvertedIndex'
        # Projection determines which attributes are copied to the index.
        projection = AllProjection()
        # Define read/write capacity for the GSI if using provisioned mode.
        read_capacity_units = 5
        write_capacity_units = 5

    SK = UnicodeAttribute(hash_key=True)
    PK = UnicodeAttribute(range_key=True)


class SparseOpenGamesIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'OpenGamesIndex'
        projection = AllProjection()
        read_capacity_units = 5
        write_capacity_units = 5

    # The key schema for the sparse index.
    map_name = UnicodeAttribute(hash_key=True)
    open_timestamp = UnicodeAttribute(range_key=True)
    


# Step 2: Define the Base Model

# This base class defines all the key attributes that are shared across
# all entities in our single table. It serves as the single source of truth for
# our table's key schema.

class GameEntityModel(Model):
    """
    An abstract base model for all entities in our Game table.
    """
    class Meta:
        # The table_name is defined here, and all subclasses will use it.
        table_name = "GamePlayerTable"
        # It's good practice to specify the region.
        region = 'us-east-1'
        # For local development with DynamoDB Local. Comment out for production.
        host = "http://localhost:8009"
        aws_access_key_id = 'fake'
        aws_secret_access_key = 'fake'
        

    # --- Primary Key ---
    # Generic PK and SK attributes. All entities will have these.
    PK = UnicodeAttribute(hash_key=True)
    SK = UnicodeAttribute(range_key=True)

    # --- Discriminator for Polymorphism ---
    # This attribute stores the class name, allowing PynamoDB to deserialize
    # items into the correct Python objects. We use `attr_name` to give it a
    # more descriptive name in the DynamoDB table itself.
    Type = DiscriminatorAttribute(attr_name='Type')

    # We need to define the indexes here so that they can be used in the model.
    inverted_index = InvertedIndex()
    sparse_index = SparseOpenGamesIndex()

    # We also need to define the attributes of our indexes here
    # Since PK and SK are already defined, we can use them directly in the indexes.
    map_name = UnicodeAttribute(null=True)  
    open_timestamp = UnicodeAttribute(null=True)  


# Step 3: Define Concrete Entity Models 

class UserModel(GameEntityModel, discriminator='User'):
    """
    Represents a User profile item.
    PK: USER#<user_id>
    SK: #METADATA#<user_id>
    """
    username = UnicodeAttribute()
    name = UnicodeAttribute()
    email = UnicodeAttribute()
    birthdate = UnicodeAttribute()
    address = UnicodeAttribute() 
    

class GameModel(GameEntityModel, discriminator='Game'):
    """
    Represents a Game session item.
    PK: GAME#<game_id>
    SK: #METADATA#<game_id>
    """
    game_id = UnicodeAttribute()
    map_name = UnicodeAttribute()
    creator = UnicodeAttribute()
    create_time = UnicodeAttribute()
    people = NumberAttribute(default=0)  # Number of players in the game
    start_time = UnicodeAttribute(null=True)  # Nullable for games that haven't started yet
    end_time = UnicodeAttribute(null=True)    # Nullable for games that haven't ended yet


class UserGameMappingModel(GameEntityModel, discriminator='UserGameMapping'):
    """
    Represents the "join" entity connecting a User to a Game.
    PK: GAME#<game_id>
    SK: USER#<user_id>
    """
    game_id = UnicodeAttribute()  
    username = UnicodeAttribute()  
    place = UnicodeAttribute(null=True)  