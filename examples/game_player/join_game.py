from pynamodb.connection import Connection
from pynamodb.transactions import TransactWrite
from models import  UserGameMappingModel, GameModel
from utils import create_table, delete_table, seed_table

GAME_ID = "c6f38a6a-d1c5-4bdf-8468-24692ccc4646"
USERNAME = 'vlopez'


delete_table()
create_table()
seed_table()

connection = Connection(region="us-east-1", host="http://localhost:8009", aws_access_key_id='fake', aws_secret_access_key='fake')

with TransactWrite(connection=connection) as transaction:
    transaction.save(
        UserGameMappingModel(
            hash_key=f"GAME#{GAME_ID}",
            range_key=f"USER#{USERNAME}",
            game_id=GAME_ID,
            username=USERNAME
        ),
        condition=(
            UserGameMappingModel.SK.does_not_exist()
        )
    )

    transaction.update(
        GameModel(
            hash_key=f"GAME#{GAME_ID}",
            range_key=f"#METADATA#{GAME_ID}"
        ),
        actions=[
            GameModel.people.set(GameModel.people + 1)
        ],
        condition=(
            GameModel.people < 50
        )
    )

# verify by fetching the game
game = GameModel.get(f"GAME#{GAME_ID}", f"#METADATA#{GAME_ID}")
print(f"Game [{game.game_id}] {game.map_name} has {game.people} players.")