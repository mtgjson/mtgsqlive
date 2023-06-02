# MTGSQLive
A project to ETL MTGJSON data into multiple other consumer formats

# Connect With Us
Discord via [![Discord](https://img.shields.io/discord/224178957103136779.svg)](https://discord.gg/74GUQDE)

# About Us

MTGJSON and MTGSQLive are open sourced database creation and distribution tool for [*Magic: The Gathering*](https://magic.wizards.com/) cards.

You can find our documentation with all properties [here](https://mtgjson.com/data-models/).

To provide feedback or to report a bug, please [open a ticket](https://github.com/mtgjson/mtgsqlite/issues/new/).

If you would like to join or assist the development of the project, you can [join us on Discord](https://mtgjson.com/discord) to discuss things further.

# Usage
```bash
$ pip install -r requirements.txt

$ python3 -m mtgsqlive [--args]

options:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input-file INPUT_FILE
                        Path to MTGJSON AllPrintings.json
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Where to place translated files

Converters:
  --all                 Run all ETL operations
  --csv                 Compile CSV AllPrinting files
  --mysql               Compile AllPrintings.sql
  --parquet             Compile Parquet AllPrinting files
  --postgresql          Compile AllPrintings.psql
  --sqlite              Compile AllPrintings.sqlite
```