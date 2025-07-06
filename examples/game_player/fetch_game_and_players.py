from models import GameEntityModel
from utils import create_table, delete_table, seed_table

GAME_ID = "3d4285f0-e52b-401a-a59b-112b38c4a26b"

GAME_PK = f"GAME#{GAME_ID}"
GAME_SK = f"#METADATA#{GAME_ID}"

delete_table()
create_table()
seed_table()

results = GameEntityModel.query(
    GAME_PK,
    GameEntityModel.SK.between(GAME_SK, "USER$")
)

for r in results:
    print(f"Found Game: {r}")
