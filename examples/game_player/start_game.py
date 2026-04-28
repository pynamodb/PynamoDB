from models import GameModel
import datetime

# Note -
# You must run this script after 'join_game.py' script

GAME_ID = "c6f38a6a-d1c5-4bdf-8468-24692ccc4646"
CREATOR = "gstanley"
START_TIME = datetime.datetime(2019, 4, 16, 10, 15, 35)


game = GameModel.get(f"GAME#{GAME_ID}", f"#METADATA#{GAME_ID}")
print("Before update:")
print(game)

game.update(
    actions=[
        GameModel.start_time.set(START_TIME.isoformat()),
        GameModel.open_timestamp.remove()
    ],
    condition=(
        GameModel.start_time.does_not_exist() & 
        (GameModel.creator == CREATOR) &
        (GameModel.people == 50)
    )
)

print("After update:")
print(game)