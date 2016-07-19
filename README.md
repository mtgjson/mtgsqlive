# MTGSQLive

[![Join the chat at https://gitter.im/mtgjson/mtgsqlive](https://badges.gitter.im/mtgjson/mtgsqlive.svg)](https://gitter.im/mtgjson/mtgsqlive)

The goal of this project is to create a SQLite database with all Magic: The Gathering card data that is supported by Gatherer and [MTGJSON](https://mtgjson.com).

We don't like being dependent on Gatherer for card data, as their update time is always delayed and there are sometimes obvious and egregious mistakes that we can correct in real time.

The database starts as a copy of the MTGJSON source of AllSets-x.json, then we manually go in to correct any errors, as MTGJSON is a direct parse of Gatherer. When spoiler season comes around, we can manually update this (with a tool to be developed later) so we will always have a database that's up to date. Projects can pull in our complete database for their projects in order to have a full Magic: the Gathering card database!

### How to Operate

To turn your JSON file into a SQLite file:
`./json_to_sql.py create_new_db db_location json_file_location`

Where:
* create_new_db is 0 (no) or 1 (yes)
* db_location is location where the database is OR where you want to store the newly created database
* json_file_location is the location of the JSON file you want to import to your database

To turn your SQLite file into a JSON file:
`./sql_to_json.py db_location output_file_location sets_or_cards`

Where:
* db_location is the location where the database is
* output_file_location is where you want to store the newly created JSON file
* sets_or_cards is "sets" (AllSets-x.json will be generated) or "cards" (AllCards-x.json will be generated)

You can test to make sure your initial JSON is the same as your output JSON via `testing_mac.sh`