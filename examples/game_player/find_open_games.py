# how to find any open game in the application, regardless of the type of map

from models import GameEntityModel
from utils import create_table, delete_table, seed_table

delete_table()
create_table()
seed_table()


results = GameEntityModel.sparse_index.scan()

for index, r in enumerate(results):
    print(f"Game: {r.map_name}")

