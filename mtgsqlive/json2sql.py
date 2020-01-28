"""
Convert MTGJSON v4 -> SQLite
"""
import argparse
import json
import logging
import pathlib
import sqlite3
import time
from typing import Any, Dict, List, Union

LOGGER = logging.getLogger(__name__)
version = "v4.6.2"  # need to automate this


def execute(input_file, output_file) -> None:
    """
    Main function
    """
    if not validate_io_streams(input_file, output_file):
        exit(1)

    # Build the SQLite database/file
    if output_file["path"].suffix == ".sql":
        output_file["handle"] = open(output_file["path"], "w", encoding="utf8")
        output_file["handle"].write(
            "\n".join(
                (
                    "-- MTGSQLive Output File",
                    "-- ({})".format(str(time.strftime("%Y-%m-%d %H:%M:%S"))),
                    "-- MTGJSON Version: {}".format(version),
                    "",
                    "START TRANSACTION;",
                    "SET names 'utf8';",
                    "",
                    "",
                )
            )
        )
    else:
        output_file["handle"] = sqlite3.connect(str(output_file["path"]))
        output_file["handle"].execute("pragma journal_mode=wal;")

    build_sql_schema(output_file)
    parse_and_import_cards(input_file, output_file)
    parse_and_import_extras(input_file, output_file)
    output_file["handle"].close()


def validate_io_streams(input_file: pathlib.Path, output_dir: Dict) -> bool:
    """
    Ensure I/O paths are valid and clean for program
    :param input_file: Input file (JSON)
    :param output_dir: Output dir
    :return: Good to continue status
    """
    output_dir.update(
        {
            "useAllPrices": False,
            "useAllDeckFiles": False,
            "useKeywords": False,
            "useCardTypes": False,
        }
    )
    if input_file.is_file():
        LOGGER.info("Building using AllPrintings.json master file.")
        if input_file.parent.joinpath("AllPrices.json").is_file():
            LOGGER.info("Building with AllPrices.json supplement.")
            output_dir["useAllPrices"] = True
        if input_file.parent.joinpath("AllDeckFiles").is_dir():
            LOGGER.info("Building with AllDeckFiles supplement.")
            output_dir["useAllDeckFiles"] = True
        if input_file.parent.joinpath("Keywords.json").is_file():
            LOGGER.info("Building with Keywords.json supplement.")
            output_dir["useKeywords"] = True
        if input_file.parent.joinpath("CardTypes.json").is_file():
            LOGGER.info("Building with CardTypes.json supplement.")
            output_dir["useCardTypes"] = True
    elif input_file.is_dir():
        LOGGER.info("Building using AllSetFiles directory.")
    else:
        LOGGER.fatal(f"Invalid input file/directory. ({input_file})")
        return False

    output_dir["path"].parent.mkdir(exist_ok=True)
    if output_dir["path"].is_file():
        LOGGER.warning(f"Output path {output_dir['path']} exists already, moving it.")
        output_dir["path"].replace(
            output_dir["path"].parent.joinpath(output_dir["path"].name + ".old")
        )

    return True


def build_sql_schema(output_file: Dict) -> None:
    """
    Create the SQLite DB schema
    :param output_file: Output info dict
    """
    LOGGER.info("Building SQLite Schema")
    if output_file["path"].suffix == ".sql":
        schema = {
            "sets": [
                "CREATE TABLE `sets` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "baseSetSize INTEGER,",
                "block TEXT,",
                "boosterV3 TEXT,",
                "code VARCHAR(8) UNIQUE NOT NULL,",
                "codeV3 TEXT,",
                "isFoilOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isForeignOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isOnlineOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isPartialPreview INTEGER NOT NULL DEFAULT 0,",  # boolean
                "keyruneCode TEXT,",
                "mcmId INTEGER,",
                "mcmName TEXT,",
                "meta TEXT,",
                "mtgoCode TEXT,",
                "name TEXT,",
                "parentCode TEXT,",
                "releaseDate DATE NOT NULL,",
                "tcgplayerGroupId INTEGER,",
                "totalSetSize INTEGER,",
                "type TEXT",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ],
            "cards": [
                "CREATE TABLE `cards` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "artist TEXT,",
                "borderColor TEXT,",
                "colorIdentity TEXT,",
                "colorIndicator TEXT,",
                "colors TEXT,",
                "convertedManaCost FLOAT,",
                "duelDeck TEXT(1),",
                "edhrecRank TEXT,",
                "faceConvertedManaCost FLOAT,",
                "flavorText TEXT,",
                "frameEffect TEXT,",
                "frameEffects TEXT,",
                "frameVersion TEXT,",
                "hand TEXT,",
                "hasFoil INTEGER NOT NULL DEFAULT 0,",  # boolean
                "hasNoDeckLimit INTEGER NOT NULL DEFAULT 0,",  # boolean
                "hasNonFoil INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isAlternative INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isArena INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isBuyABox INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isDateStamped INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isFullArt INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isMtgo INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isOnlineOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isOversized INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isPaper INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isPromo INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isReprint INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isReserved INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isStarter INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isStorySpotlight INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isTextless INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isTimeshifted INTEGER NOT NULL DEFAULT 0,",  # boolean
                "layout TEXT,",
                "leadershipSkills TEXT,",
                "life TEXT,",
                "loyalty TEXT,",
                "manaCost TEXT,",
                "mcmId INTEGER,",
                "mcmMetaId INTEGER,",
                "mcmName TEXT,",
                "mtgArenaId INTEGER,",
                "mtgoFoilId INTEGER,",
                "mtgoId INTEGER,",
                "mtgstocksId INTEGER,",
                "multiverseId INTEGER,",
                "name TEXT,",
                "names TEXT,",
                "number TEXT,",
                "originalText TEXT,",
                "originalType TEXT,",
                "otherFaceIds TEXT,",
                "power TEXT,",
                "printings TEXT,",
                "purchaseUrls TEXT,",
                "rarity TEXT,",
                "scryfallId VARCHAR(36),",
                "scryfallIllustrationId VARCHAR(36),",
                "scryfallOracleId VARCHAR(36),",
                "setCode VARCHAR(8),"
                "INDEX(setCode),"
                "FOREIGN KEY (setCode) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,",
                "side TEXT,",
                "subtypes TEXT,",
                "supertypes TEXT,",
                "tcgplayerProductId INTEGER,",
                "tcgplayerPurchaseUrl TEXT,",
                "text TEXT,",
                "toughness TEXT,",
                "type TEXT,",
                "types TEXT,",
                "uuid VARCHAR(36) UNIQUE NOT NULL,",
                "variations TEXT,",
                "watermark TEXT",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ],
            "tokens": [
                "CREATE TABLE `tokens` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "artist TEXT,",
                "borderColor TEXT,",
                "colorIdentity TEXT,",
                "colorIndicator TEXT,",
                "colors TEXT,",
                "duelDeck TEXT(1),",
                "isOnlineOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "layout TEXT,",
                "loyalty TEXT,",
                "name TEXT,",
                "names TEXT,",
                "number TEXT,",
                "power TEXT,",
                "reverseRelated TEXT,",
                "scryfallId VARCHAR(36),",
                "scryfallIllustrationId VARCHAR(36),",
                "scryfallOracleId VARCHAR(36),",
                "setCode VARCHAR(8),",
                "INDEX(setCode),",
                "FOREIGN KEY (setCode) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,",
                "side TEXT,",
                "subtypes TEXT,",
                "supertypes TEXT,",
                "text TEXT,",
                "toughness TEXT,",
                "type TEXT,",
                "types TEXT,",
                "uuid VARCHAR(36) NOT NULL,",
                "watermark TEXT",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ],
            "set_translations": [
                "CREATE TABLE `set_translations` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "language TEXT,",
                "setCode VARCHAR(8),",
                "INDEX(setCode),",
                "FOREIGN KEY (setCode) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,",
                "translation TEXT",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ],
            "foreign_data": [
                "CREATE TABLE `foreign_data` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "flavorText TEXT,",
                "language TEXT,",
                "multiverseId INTEGER,",
                "name TEXT,",
                "text TEXT,",
                "type TEXT,",
                "uuid VARCHAR(36),",
                "INDEX(uuid),",
                "FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ],
            "legalities": [
                "CREATE TABLE `legalities` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "format TEXT,",
                "status TEXT,",
                "uuid VARCHAR(36),",
                "INDEX(uuid),",
                "FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ],
            "rulings": [
                "CREATE TABLE `rulings` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "date TEXT,",
                "text TEXT,",
                "uuid VARCHAR(36),",
                "INDEX(uuid),",
                "FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ],
            "prices": [
                "CREATE TABLE `prices` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "date TEXT,",
                "price REAL,",
                "type TEXT,",
                "uuid VARCHAR(36),",
                "INDEX(uuid),",
                "FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ],
        }
        if output_file["useAllDeckFiles"]:
            schema["decks"] = [
                "CREATE TABLE `decks` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "code VARCHAR(8),",
                "INDEX(code),",
                "FOREIGN KEY (code) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,",
                "fileName TEXT UNIQUE NOT NULL,",
                "name TEXT NOT NULL,",
                "releaseDate TEXT,",
                "mainBoard TEXT NOT NULL,",
                "sideBoard TEXT,",
                "type TEXT",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ]
        if output_file["useKeywords"]:
            schema["keywords"] = [
                "CREATE TABLE `keywords` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "word TEXT UNIQUE NOT NULL,",
                "type TEXT NOT NULL",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ]
        if output_file["useCardTypes"]:
            schema["types"] = [
                "CREATE TABLE `types` (",
                "id INTEGER PRIMARY KEY AUTO_INCREMENT,",
                "type TEXT UNIQUE NOT NULL,",
                "subTypes TEXT,",
                "superTypes TEXT",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "",
                "",
            ]
        for q in schema.values():
            output_file["handle"].write("\n".join(q))
        output_file["handle"].write("COMMIT;\n\n")
    else:
        schema = {
            "sets": [
                "CREATE TABLE `sets` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "baseSetSize INTEGER,",
                "block TEXT,",
                "boosterV3 TEXT,",
                "code TEXT UNIQUE NOT NULL,",
                "codeV3 TEXT,",
                "isFoilOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isForeignOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isOnlineOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isPartialPreview INTEGER NOT NULL DEFAULT 0,",  # boolean
                "keyruneCode TEXT,",
                "mcmId INTEGER,",
                "mcmName TEXT,",
                "meta TEXT,",
                "mtgoCode TEXT,",
                "name TEXT,",
                "parentCode TEXT,",
                "releaseDate TEXT,",
                "tcgplayerGroupId INTEGER,",
                "totalSetSize INTEGER,",
                "type TEXT",
                ");",
                "",
                "",
            ],
            "cards": [
                "CREATE TABLE `cards` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "artist TEXT,",
                "borderColor TEXT,",
                "colorIdentity TEXT,",
                "colorIndicator TEXT,",
                "colors TEXT,",
                "convertedManaCost FLOAT,",
                "duelDeck TEXT(1),",
                "edhrecRank TEXT,",
                "faceConvertedManaCost FLOAT,",
                "flavorText TEXT,",
                "frameEffect TEXT,",
                "frameEffects TEXT,",
                "frameVersion TEXT,",
                "hand TEXT,",
                "hasFoil INTEGER NOT NULL DEFAULT 0,",  # boolean
                "hasNoDeckLimit INTEGER NOT NULL DEFAULT 0,",  # boolean
                "hasNonFoil INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isAlternative INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isArena INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isBuyABox INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isDateStamped INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isFullArt INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isMtgo INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isOnlineOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isOversized INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isPaper INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isPromo INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isReprint INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isReserved INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isStarter INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isStorySpotlight INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isTextless INTEGER NOT NULL DEFAULT 0,",  # boolean
                "isTimeshifted INTEGER NOT NULL DEFAULT 0,",  # boolean
                "layout TEXT,",
                "leadershipSkills TEXT,",
                "life TEXT,",
                "loyalty TEXT,",
                "manaCost TEXT,",
                "mcmId INTEGER,",
                "mcmMetaId INTEGER,",
                "mcmName TEXT,",
                "mtgArenaId INTEGER,",
                "mtgoFoilId INTEGER,",
                "mtgoId INTEGER,",
                "mtgstocksId INTEGER,",
                "multiverseId INTEGER,",
                "name TEXT,",
                "names TEXT,",
                "number TEXT,",
                "originalText TEXT,",
                "originalType TEXT,",
                "otherFaceIds TEXT,",
                "power TEXT,",
                "printings TEXT,",
                "purchaseUrls TEXT,",
                "rarity TEXT,",
                "scryfallId TEXT(36),",
                "scryfallIllustrationId TEXT(36),",
                "scryfallOracleId TEXT(36),",
                "setCode TEXT REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,",
                "side TEXT,",
                "subtypes TEXT,",
                "supertypes TEXT,",
                "tcgplayerProductId INTEGER,",
                "tcgplayerPurchaseUrl TEXT,",
                "text TEXT,",
                "toughness TEXT,",
                "type TEXT,",
                "types TEXT,",
                "uuid TEXT(36) UNIQUE NOT NULL,",
                "variations TEXT,",
                "watermark TEXT",
                ");",
                "",
                "",
            ],
            "tokens": [
                "CREATE TABLE `tokens` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "artist TEXT,",
                "borderColor TEXT,",
                "colorIdentity TEXT,",
                "colorIndicator TEXT,",
                "colors TEXT,",
                "duelDeck TEXT(1),",
                "isOnlineOnly INTEGER NOT NULL DEFAULT 0,",  # boolean
                "layout TEXT,",
                "loyalty TEXT,",
                "name TEXT,",
                "names TEXT,",
                "number TEXT,",
                "power TEXT,",
                "reverseRelated TEXT,",
                "scryfallId TEXT(36),",
                "scryfallIllustrationId TEXT(36),",
                "scryfallOracleId TEXT(36),",
                "setCode TEXT REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,",
                "side TEXT,",
                "subtypes TEXT,",
                "supertypes TEXT,",
                "text TEXT,",
                "toughness TEXT,",
                "type TEXT,",
                "types TEXT,",
                "uuid TEXT(36) NOT NULL,",
                "watermark TEXT",
                ");",
                "",
                "",
            ],
            "set_translations": [
                "CREATE TABLE `set_translations` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "language TEXT,",
                "setCode TEXT REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,",
                "translation TEXT",
                ");",
                "",
                "",
            ],
            "foreign_data": [
                "CREATE TABLE `foreign_data` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "flavorText TEXT,",
                "language TEXT,",
                "multiverseId INTEGER,",
                "name TEXT,",
                "text TEXT,",
                "type TEXT,",
                "uuid TEXT(36) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE",
                ");",
                "",
                "",
            ],
            "legalities": [
                "CREATE TABLE `legalities` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "format TEXT,",
                "status TEXT,",
                "uuid TEXT(36) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE",
                ");",
                "",
                "",
            ],
            "rulings": [
                "CREATE TABLE `rulings` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "date TEXT,",
                "text TEXT,",
                "uuid TEXT(36) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE",
                ");",
                "",
                "",
            ],
            "prices": [
                "CREATE TABLE `prices` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "date TEXT,",
                "price REAL,",
                "type TEXT,",
                "uuid TEXT(36) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE",
                ");",
                "",
                "",
            ],
        }
        if output_file["useAllDeckFiles"]:
            schema["decks"] = [
                "CREATE TABLE `decks` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "code TEXT REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,",
                "fileName TEXT UNIQUE NOT NULL,",
                "name TEXT NOT NULL,",
                "releaseDate TEXT,",
                "mainBoard TEXT NOT NULL,",
                "sideBoard TEXT,",
                "type TEXT",
                ");",
                "",
                "",
            ]
        if output_file["useKeywords"]:
            schema["keywords"] = [
                "CREATE TABLE `keywords` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "word TEXT UNIQUE NOT NULL,",
                "type TEXT NOT NULL",
                ");",
                "",
                "",
            ]
        if output_file["useCardTypes"]:
            schema["types"] = [
                "CREATE TABLE `types` (",
                "id INTEGER PRIMARY KEY AUTOINCREMENT,",
                "type TEXT UNIQUE NOT NULL,",
                "subTypes TEXT,",
                "superTypes TEXT",
                ");",
                "",
                "",
            ]
        cursor = output_file["handle"].cursor()
        for q in schema.values():
            cursor.execute("".join(q))
        output_file["handle"].commit()


def parse_and_import_cards(input_file: pathlib.Path, output_file: Dict) -> None:
    """
    Parse the JSON cards and input them into the database
    :param input_file: AllSets.json file
    :param output_file: Output info dictionary
    """
    if input_file.is_file():
        LOGGER.info("Loading JSON into memory")
        with input_file.open("r", encoding="utf8") as f:
            json_data = json.load(f)
        LOGGER.info("Building sets")
        for set_code, set_data in json_data.items():
            # Handle set insertion
            LOGGER.info("Inserting set row for {}".format(set_code))
            set_insert_values = handle_set_row_insertion(set_data)
            sql_dict_insert(set_insert_values, "sets", output_file)

            for card in set_data.get("cards"):
                LOGGER.debug("Inserting card row for {}".format(card.get("name")))
                card_attr: Dict[str, Any] = handle_card_row_insertion(card, set_code)
                sql_insert_all_card_fields(card_attr, output_file)

            for token in set_data.get("tokens"):
                LOGGER.debug("Inserting token row for {}".format(token.get("name")))
                token_attr = handle_token_row_insertion(token, set_code)
                sql_dict_insert(token_attr, "tokens", output_file)

            for language, translation in set_data.get("translations", {}).items():
                LOGGER.debug("Inserting set_translation row for {}".format(language))
                set_translation_attr = handle_set_translation_row_insertion(
                    language, translation, set_code
                )
                sql_dict_insert(set_translation_attr, "set_translations", output_file)
    elif input_file.is_dir():
        for setFile in input_file.glob("*.json"):
            LOGGER.info("Loading {} into memory...".format(setFile.name))
            with setFile.open("r", encoding="utf8") as f:
                set_data = json.load(f)
            set_code = setFile.stem
            LOGGER.info("Building set: {}".format(set_code))
            set_insert_values = handle_set_row_insertion(set_data)
            sql_dict_insert(set_insert_values, "sets", output_file)

            for card in set_data.get("cards"):
                LOGGER.debug("Inserting card row for {}".format(card.get("name")))
                card_attr: Dict[str, Any] = handle_card_row_insertion(card, set_code)
                sql_insert_all_card_fields(card_attr, output_file)

            for token in set_data.get("tokens"):
                LOGGER.debug("Inserting token row for {}".format(token.get("name")))
                token_attr = handle_token_row_insertion(token, set_code)
                sql_dict_insert(token_attr, "tokens", output_file)

            for language, translation in set_data.get("translations", {}).items():
                LOGGER.debug("Inserting set_translation row for {}".format(language))
                set_translation_attr = handle_set_translation_row_insertion(
                    language, translation, set_code
                )
                sql_dict_insert(set_translation_attr, "set_translations", output_file)

    if output_file["path"].suffix == ".sql":
        output_file["handle"].write("COMMIT;")
    else:
        output_file["handle"].commit()


def parse_and_import_extras(input_file: pathlib.Path, output_file: Dict) -> None:
    """
    Parse the extra data files and input them into the database
    :param input_file: AllSets.json file
    :param output_file: Output info dictionary
    """
    if output_file["useAllPrices"]:
        LOGGER.info("Inserting AllPrices rows")
        with input_file.parent.joinpath("AllPrices.json").open(
            "r", encoding="utf8"
        ) as f:
            json_data = json.load(f)
        for card_uuid, price_data in json_data.items():
            for price_type, price_dict in price_data["prices"].items():
                if not price_type == "uuid":
                    for price_date, price_value in price_dict.items():
                        if price_value:
                            sql_dict_insert(
                                {
                                    "uuid": card_uuid,
                                    "type": price_type,
                                    "date": price_date,
                                    "price": float(price_value),
                                },
                                "prices",
                                output_file,
                            )

    if output_file["useAllDeckFiles"]:
        LOGGER.info("Inserting Deck rows")
        for deck_file in input_file.parent.joinpath("AllDeckFiles").glob("*.json"):
            with deck_file.open("r", encoding="utf8") as f:
                json_data = json.load(f)
            deck_data = {}
            for key, value in json_data.items():
                if key == "meta":
                    continue
                if key == "mainBoard" or key == "sideBoard":
                    cards = []
                    for card in value:
                        for i in range(0, card["count"]):
                            cards.append(card["uuid"])
                    deck_data[key] = ", ".join(cards)
                else:
                    deck_data[key] = value
            if not "fileName" in deck_data:
                deck_data["fileName"] = deck_file.stem
            sql_dict_insert(deck_data, "decks", output_file)

    if output_file["useKeywords"]:
        LOGGER.info("Inserting Keyword rows")
        with input_file.parent.joinpath("Keywords.json").open(
            "r", encoding="utf8"
        ) as f:
            json_data = json.load(f)
        for keyword_type in json_data:
            if keyword_type == "meta":
                continue
            for keyword in json_data[keyword_type]:
                sql_dict_insert(
                    {"word": keyword, "type": keyword_type}, "keywords", output_file
                )

    if output_file["useCardTypes"]:
        LOGGER.info("Inserting Card Type rows")
        with input_file.parent.joinpath("CardTypes.json").open(
            "r", encoding="utf8"
        ) as f:
            json_data = json.load(f)
        for type in json_data["types"]:
            subtypes = []
            for subtype in json_data["types"][type]["subTypes"]:
                subtypes.append(subtype)
            supertypes = []
            for supertype in json_data["types"][type]["superTypes"]:
                supertypes.append(supertype)
            sql_dict_insert(
                {
                    "type": type,
                    "subTypes": ", ".join(subtypes),
                    "superTypes": ", ".join(supertypes),
                },
                "types",
                output_file,
            )

    if output_file["path"].suffix == ".sql":
        output_file["handle"].write("COMMIT;")
    else:
        output_file["handle"].commit()


def sql_insert_all_card_fields(
    card_attributes: Dict[str, Any], output_file: Dict
) -> None:
    """
    Given all of the card's data, insert the data into the
    appropriate SQLite tables.
    :param card_attributes: Tuple of data
    :param output_file: Output info dictionary
    """
    sql_dict_insert(card_attributes["cards"], "cards", output_file)

    for foreign_val in card_attributes["foreign_data"]:
        sql_dict_insert(foreign_val, "foreign_data", output_file)

    for legal_val in card_attributes["legalities"]:
        sql_dict_insert(legal_val, "legalities", output_file)

    for rule_val in card_attributes["rulings"]:
        sql_dict_insert(rule_val, "rulings", output_file)

    if not output_file["useAllPrices"]:
        for price_val in card_attributes["prices"]:
            sql_dict_insert(price_val, "prices", output_file)


def handle_set_row_insertion(set_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    This method will take the set data and convert it, preparing
    for SQLite insertion
    :param set_data: Data to process
    :return: Dictionary ready for insertion
    """
    set_skip_keys = ["cards", "tokens", "translations"]
    set_insert_values = {}

    for key, value in set_data.items():
        if key in set_skip_keys:
            continue

        if key == "boosterV3":
            set_insert_values[key] = modify_for_sql_insert(str(value))
            continue

        set_insert_values[key] = modify_for_sql_insert(value)

    return set_insert_values


def handle_foreign_rows(
    card_data: Dict[str, Any], card_uuid: str
) -> List[Dict[str, Any]]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts ready for insertion
    """

    foreign_entries = []
    for entry in card_data["foreignData"]:
        foreign_entries.append(
            {
                "uuid": card_uuid,
                "flavorText": entry.get("flavorText", ""),
                "language": entry.get("language", ""),
                "multiverseId": entry.get("multiverseId", None),
                "name": entry.get("name", ""),
                "text": entry.get("text", ""),
                "type": entry.get("type", ""),
            }
        )

    return foreign_entries


def handle_legal_rows(
    card_data: Dict[str, Any], card_uuid: str
) -> List[Dict[str, Any]]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts, ready for insertion
    """
    legalities = []
    for card_format, format_status in card_data["legalities"].items():
        legalities.append(
            {"uuid": card_uuid, "format": card_format, "status": format_status}
        )

    return legalities


def handle_ruling_rows(
    card_data: Dict[str, Any], card_uuid: str
) -> List[Dict[str, Any]]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts, ready for insertion
    """
    rulings = []
    for rule in card_data["rulings"]:
        rulings.append(
            {
                "uuid": card_uuid,
                "date": rule.get("date", ""),
                "text": rule.get("text", ""),
            }
        )
    return rulings


def handle_price_rows(
    card_data: Dict[str, Any], card_uuid: str
) -> List[Dict[str, Any]]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts, ready for insertion
    """
    prices = []
    for price_type in card_data["prices"]:
        if price_type == "MtgjsonpricesobjectParentIsCardObject":
            continue
        if card_data["prices"][price_type] is not None:
            for date, price in card_data["prices"][price_type].items():
                if price:
                    prices.append(
                        {
                            "uuid": card_uuid,
                            "type": price_type,
                            "price": price,
                            "date": date,
                        }
                    )
    return prices


def handle_set_translation_row_insertion(
    language: str, translation: str, set_name: str
) -> Dict[str, Any]:
    """
    This method will take the set translation data and convert it, preparing
    for SQLite insertion
    :param language: The language of the set translation
    :param translation: The set name translated in to the given language
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    set_translation_insert_values: Dict[str, Any] = {
        "language": language,
        "translation": translation,
        "setCode": set_name,
    }

    return set_translation_insert_values


def handle_token_row_insertion(
    token_data: Dict[str, Any], set_name: str
) -> Dict[str, Any]:
    """
    This method will take the token data and convert it, preparing
    for SQLite insertion
    :param token_data: Data to process
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    token_insert_values: Dict[str, Any] = {"setCode": set_name}
    for key, value in token_data.items():
        token_insert_values[key] = modify_for_sql_insert(value)

    return token_insert_values


def handle_card_row_insertion(
    card_data: Dict[str, Any], set_name: str
) -> Dict[str, Any]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    # ORDERING MATTERS HERE
    card_skip_keys = ["foreignData", "legalities", "rulings", "prices"]

    card_insert_values: Dict[str, Any] = {"setCode": set_name}
    for key, value in card_data.items():
        if key in card_skip_keys:
            continue
        card_insert_values[key] = modify_for_sql_insert(value)

    foreign_insert_values: List[Dict[str, Any]] = []
    if card_skip_keys[0] in card_data.keys():
        foreign_insert_values = handle_foreign_rows(card_data, card_data["uuid"])

    legal_insert_values: List[Dict[str, Any]] = []
    if card_skip_keys[1] in card_data.keys():
        legal_insert_values = handle_legal_rows(card_data, card_data["uuid"])

    ruling_insert_values: List[Dict[str, Any]] = []
    if card_skip_keys[2] in card_data.keys():
        ruling_insert_values = handle_ruling_rows(card_data, card_data["uuid"])

    price_insert_values: List[Dict[str, Any]] = []
    if card_skip_keys[3] in card_data.keys():
        price_insert_values = handle_price_rows(card_data, card_data["uuid"])

    return {
        "cards": card_insert_values,
        "foreign_data": foreign_insert_values,
        "legalities": legal_insert_values,
        "rulings": ruling_insert_values,
        "prices": price_insert_values,
    }


def modify_for_sql_insert(data: Any) -> Union[str, int, float, None]:
    """
    Arrays and booleans can't be inserted, so we need to stringify
    :param data: Data to modify
    :return: string value
    """
    if isinstance(data, (str, int, float)):
        return data

    # If the value is empty/null, mark it in SQL as such
    if not data:
        return None

    if isinstance(data, list) and data and isinstance(data[0], str):
        return ",".join(data)

    if isinstance(data, bool):
        return int(data)

    if isinstance(data, dict):
        return str(data)

    return ""

def modify_for_sql_file(data: Dict[str, Any]) -> Dict[str, Any]:
    for key in data.keys():
        if isinstance(data[key], str):
            data[key] = (
                "'" + data[key].replace("'", "''") + "'"
            )
        if str(data[key]) == "False":
            data[key] = 0
        if str(data[key]) == "True":
            data[key] = 1
        if data[key] is None:
            data[key] = "NULL"
    return data

def sql_dict_insert(data: Dict[str, Any], table: str, output_file: Dict) -> None:
    """
    Insert a dictionary into a sqlite table
    :param data: Dict to insert
    :param table: Table to insert to
    :param output_file: Output info dictionary
    """
    try:
        if output_file["path"].suffix == ".sql":
            data = modify_for_sql_file(data)
            query = (
                "INSERT INTO "
                + table
                + " ("
                + ", ".join(data.keys())
                + ") VALUES ({"
                + "}, {".join(data.keys())
                + "});\n"
            )
            query = query.format(**data)
            output_file["handle"].write(query)
        else:
            cursor = output_file["handle"].cursor()
            columns = ", ".join(data.keys())
            placeholders = ":" + ", :".join(data.keys())
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            cursor.execute(query, data)
    except:
        datastr = str(data)
        LOGGER.warning(f"Failed to insert row in {table} with values {datastr}")
