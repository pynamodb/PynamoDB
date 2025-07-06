====================================
A SingleTable Design for Game Player
====================================

This example demonstrates a single-table design for a game player application using PynamoDB. 

This is based on a workshop at AWS https://catalog.workshops.aws/dynamodb-labs/en-US/game-player-data

At the above workshop you will find 6 sections that go over the details of the design and how to implement it. It is 
recommended to go through the workshop first before looking at the code here.

This examples show how to use polymorophism in PynamoDB to achieve the single-table design.

Fetch Game and Players
========================

Make sure to read below page -
https://catalog.workshops.aws/dynamodb-labs/en-US/game-player-data/core-usage/step4

The corresponding code (written using pynamodb) for this section is in the file `fetch_game_and_players.py`.

```bash
# Note the script will delete, create & seed the data in the DynamoDB table.
python fetch_game_and_players.py
```

Query the sparse GSI
========================

Make sure to read below page -
https://catalog.workshops.aws/dynamodb-labs/en-US/game-player-data/open-games/step3

The corresponding code (written using pynamodb) for this section is in the file `find_open_games_by_map.py`.

```bash
# Note the script will delete, create & seed the data in the DynamoDB table.
python find_open_games_by_map.py
```

Scan the sparse GSI
===================

Make sure to read below page -
https://catalog.workshops.aws/dynamodb-labs/en-US/game-player-data/open-games/step4

The corresponding code (written using pynamodb) for this section is in the file `find_open_games.py`.

```bash
# Note the script will delete, create & seed the data in the DynamoDB table.
python find_open_games.py
```

Add user to a game
==================

Make sure to read below page -
https://catalog.workshops.aws/dynamodb-labs/en-US/game-player-data/join-games/step1

The corresponding code (written using pynamodb) for this section is in the file `join_game.py`.

```bash
# Note the script will delete, create & seed the data in the DynamoDB table.
python join_game.py
```

Start a game
============

Make sure to read below page -
https://catalog.workshops.aws/dynamodb-labs/en-US/game-player-data/join-games/step2

The corresponding code (written using pynamodb) for this section is in the file `start_game.py`.

```bash
# You MUST run join_game.py first to add a user to the game.
# and then run this script to start the game.
python start_game.py
```

Retrieve games for a user (Inverted Index usecase)
==================================================

Make sure to read below page -
https://catalog.workshops.aws/dynamodb-labs/en-US/game-player-data/past-games/step2

The corresponding code (written using pynamodb) for this section is in the file `find_games_for_user.py`.

```bash
# Note the script will delete, create & seed the data in the DynamoDB table.
python find_games_for_user.py
```