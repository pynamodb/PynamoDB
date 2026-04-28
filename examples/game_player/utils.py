from pathlib import Path
import json
from models import UserModel, GameModel, UserGameMappingModel, GameEntityModel


def create_table():
    """
    Checks if the table exists and creates it if it doesn't.
    """
    if not GameEntityModel.exists():
        print("Table 'GamePlayerTable' does not exist. Creating...")
        # PynamoDB handles the creation of the table and all its GSIs.
        GameEntityModel.create_table(
            wait=True,  # Wait until the table is active
            read_capacity_units=1,
            write_capacity_units=1
        )
        print("Table 'GamePlayerTable' created successfully.")
    else:
        print("Table 'GamePlayerTable' already exists.")


def delete_table():
    """
    Deletes the table if it exists.
    """
    if GameEntityModel.exists():
        print("Table 'GamePlayerTable' exists. Deleting...")
        GameEntityModel.delete_table(wait=True)
        print("Table 'GamePlayerTable' deleted successfully.")
    else:
        print("Table 'GamePlayerTable' does not exist. No action taken.")


def seed_table():
    """
    Seeds the table with initial data.
    """
    items_path = Path(__file__).parent / "items.json"

    with open(items_path, "r") as f:
        items = f.readlines()

    rows = []
    for row in items:
        item = json.loads(row)
        if item["PK"].startswith("USER#") and item["SK"].startswith("#METADATA"):
            user = UserModel(**item)
            user.save()
        elif item["PK"].startswith("GAME#") and item["SK"].startswith("#METADATA"):
            # replace "map" with "map_name" in item if it exists
            if "map" in item:
                item["map_name"] = item.pop("map")
            if "gold" in item:
                item.pop("gold")
            if "silver" in item:
                item.pop("silver")
            if "bronze" in item:
                item.pop("bronze")

            game = GameModel(**item)
            game.save()
        elif item["PK"].startswith("GAME#") and item["SK"].startswith("USER#"):
            mapping = UserGameMappingModel(**item)
            mapping.save()
        else:
            print(f"Unknown item type: {item['PK']}")
        rows.append(item)
