# MTGSQLive

[![Join the chat at https://gitter.im/mtgjson/mtgsqlive](https://badges.gitter.im/mtgjson/mtgsqlive.svg)](https://gitter.im/mtgjson/mtgsqlive)

### Goals
The goals of this project include creating a downloadable SQLite database file containing all cards from the game Magic: The Gathering. We are looking for a way to stop being dependent on Gatherer for card data due to their delayed update times and egregious errors that slip by quality control, which we can correct in real time.

While we want to claim we are completely independent from Gatherer, we must first start with their data and then manually edit it to be correct. Once that is accomplished, we are then no longer dependent on them for card data, except for rullings.


### How We Did It

For the database we have hosted on GitHub, we began with downloading and parsing card data from [MTGJSON](https://mtgjson.com), a highly reputable source for Magic: The Gathering card and set information, through their `AllSets-x.json` file.

Once we have completed this import, a repo manager can go in and manually edit imperfections in the data, as sometimes MTGJSON's data is directly based on Gatherer, which in itself usually has a few mistakes.

When "spoiler season" (a few times a year when Wizards of the Coast spoils new cards from an upcoming set) begins, a repo manager can manually update the database (with a tool to be developed later). With the past card data already in the database and spoiled cards input manually, this database will always be up to date, far ahead of the update schedule of Gatherer and MTGJSON. 

Projects can pull in our complete database for their projects in order to have a full Magic: the Gathering card database at all times. There is a special table, `lastUpdated`, which contains all of the times that the database was updated, either manually or via an import from MTGJSON (once the set officially is released on Gatherer).

**NOTE:** 

### How to Operate

If you would like to use our pre-compiled database for your project, simply download the database and read the `lastUpdated` table to know when the database was last updated. If you would rather create your own database, we have included the tools to do such.

To turn your AllSets JSON file into a SQLite database, run the following command: <br> `./json_to_sql.py create_new_db db_location json_file_location`

Where:
* `create_new_db` is "0" (do not create a new database) or "1" (create a new database)
* `db_location` is location where the database is stored OR where you want to store the newly created database
* `json_file_location` is the location of the JSON file you want to import to your database

If your project is already dependent on a JSON format, such as if you pull in the data from MTGJSON and parse it internally, you can create a JSON file from our database with our conversion tool!


To turn a SQLite database file into a JSON file, run the following command: <br> `./sql_to_json.py db_location output_file_location sets_or_cards`

Where:
* `db_location` is the location where the database is stored
* `output_file_location` is where you want to store the newly created JSON output file
* `sets_or_cards` is "sets" ("AllSets-x.json" will be generated) or "cards" ("AllCards-x.json" will be generated)

### Confirmation Testing
If you would like to make sure the input file is the same as the output file, making sure no data is lost in conversion, you can run the `testing_mac.sh` command. *NOTE:* this program will only work on unix-style machines.