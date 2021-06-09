# [**MTGSQLive**](https://mtgjson.com/)

# Connect With Us
Discord via [![Discord](https://img.shields.io/discord/224178957103136779.svg)](https://discord.gg/74GUQDE)

Gitter via [![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/mtgjson/mtgjson4)


### Goals
The goals of this project are to extend the MTGJSONv5 protocols and give an option for pre-processed SQLite downloads.
lly edit it to be correct. Once that is accomplished, we are then no longer dependent on them for card data, except for rullings.

# About Us

MTGJSON and MTGSQlive are open sourced database creation and distribution tool for [*Magic: The Gathering*](https://magic.wizards.com/) cards, specifically in [JSON](https://json.org/) and [SQLite](https://www.sqlite.org/index.html) format.

You can find our documentation with all properties [here](https://mtgjson.com/data-models/).

To provide feedback and/or bug reports, please [open a ticket](https://github.com/mtgjson/mtgsqlite/issues/new/) as it is the best way for us to communicate with the public.

If you would like to join or assist the development of the project, you can [join us on Discord](https://mtgjson.com/discord) to discuss things further.

# How To Use

This system was built using *Python 3.9*, so we can only guarantee proper functionality with this version.

## Postgres

To produce a Postgres-ready output, ensure the output file has a `.sql` extension.

```sh
# Install dependencies
$ pip install -r requirements.txt 

# usage: mtgsqlive [-h] -i file -o file 
$ python -m mtgsqlive -i /path/to/AllPrintings.json -o /path/to/output.sql
```

### Hasura

The Postgres-ready output uses JSONB fields to capture array values. This allows for complex filtering if using [Hasura](https://hasura.io/) on top of Postgres to generate a GraphQL schema and API.

For example, if imported into a Hasura instance, one could perform the following query to grab all cards which are both Human and Cleric, then display their names.

```graphql
query HumanCleric {
  cards(where: {subtypes: {_contains: ["Human", "Cleric"]}}) {
    name
  }
}

```

## MariaDB/MySQL

If the output file has a `.sql` extension, the destination engine will default to Postgres. The `-e` flag can be set to produce MySQL-conforming output.

```sh
# Install dependencies
$ pip install -r requirements.txt 

# usage: mtgsqlive [-h] -i file -o file 
$ python -m mtgsqlive -i /path/to/AllPrintings.json -o /path/to/output.sql -e mysql
```  

## SQLite

To create a SQLite database, use a `.sqlite` extension for the output file argument.

>**Note:** These are the build directions to compile your own SQLite file.<br>
>If you are looking for the pre-compiled SQLite file, you can download it from [MTGJSON.com](https://mtgjson.com/).

```sh
# Install dependencies
$ pip install -r requirements.txt 

# usage: mtgsqlive [-h] -i file -o file 
$ python -m mtgsqlive -i /path/to/AllPrintings.json -o /path/to/output.sqlite
```  
