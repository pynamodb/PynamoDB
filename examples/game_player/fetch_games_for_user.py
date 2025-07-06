# This show case inverted index
# find all past games for a user

from models import GameEntityModel
from utils import create_table, delete_table, seed_table

delete_table()
create_table()
seed_table()

USERNAME = "carrpatrick"

QUERY_PK = f"USER#{USERNAME}"

results = GameEntityModel.inverted_index.query(
    QUERY_PK
)

for index, r in enumerate(results):
    if index == 0:
        print(f"Games for `{r.username}` are ...")

    print(f"Game: {r.game_id}")

