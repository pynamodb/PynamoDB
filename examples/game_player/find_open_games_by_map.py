# This show cases sparse  index
# make targeted queries to find open games.
from models import GameEntityModel
from utils import create_table, delete_table, seed_table

delete_table()
create_table()
seed_table()


MAP_NAME = "Green Grasslands"


results = GameEntityModel.sparse_index.query(
    MAP_NAME
)

for index, r in enumerate(results):
    print(f"Game: {r}")

