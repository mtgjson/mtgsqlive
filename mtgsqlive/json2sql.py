"""
Convert MTGJSON v4 -> SQLite
"""
import argparse
import json
import logging
import pathlib
import sqlite3
from typing import Any, Dict, List, Union

LOGGER = logging.getLogger(__name__)


def main() -> None:
    """
    Main function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", help="input file (AllSets.json)", required=True, metavar="file"
    )
    parser.add_argument(
        "-o", help="output file (*.sqlite)", required=True, metavar="file"
    )
    args = parser.parse_args()

    # Define our I/O paths
    input_file = pathlib.Path(args.i).expanduser()
    output_file = pathlib.Path(args.o).expanduser()

    if not validate_io_streams(input_file, output_file):
        exit(1)

    # Build the SQLite database
    sql_connection = sqlite3.connect(output_file)
    sql_connection.execute("pragma journal_mode=wal;")

    build_sql_schema(sql_connection)
    parse_and_import_cards(input_file, sql_connection)


def validate_io_streams(input_file: pathlib.Path, output_file: pathlib.Path) -> bool:
    """
    Ensure I/O paths are valid and clean for program
    :param input_file: Input file (JSON)
    :param output_file: Output file (SQLite)
    :return: Good to continue status
    """
    if not input_file.is_file():
        LOGGER.fatal("Input file {} does not exist.".format(input_file))
        return False

    output_file.parent.mkdir(exist_ok=True)
    if output_file.is_file():
        LOGGER.warning("Output file {} exists already, moving it.".format(output_file))
        output_file.replace(output_file.parent.joinpath(output_file.name + ".old"))

    return True


def build_sql_schema(sql_connection: sqlite3.Connection) -> None:
    """
    Create the SQLite DB schema
    :param sql_connection: Connection to the database
    """
    LOGGER.info("Building SQLite Schema")
    cursor = sql_connection.cursor()

    # Build Set table
    cursor.execute(
        "CREATE TABLE `sets` ("
        "baseSetSize INTEGER,"
        "block TEXT,"
        "boosterV3 TEXT,"
        "code TEXT,"
        "codeV3 TEXT,"
        "isFoilOnly INTEGER NOT NULL DEFAULT 0,"
        "isOnlineOnly INTEGER NOT NULL DEFAULT 0,"
        "keyruneCode TEXT,"
        "mcmName TEXT,"
        "mcmId INTEGER,"
        "meta TEXT,"
        "mtgoCode TEXT,"
        "name TEXT,"
        "parentCode TEXT,"
        "releaseDate TEXT,"
        "tcgplayerGroupId INTEGER,"
        "totalSetSize INTEGER,"
        "type TEXT"
        ")"
    )

    # Translations for set names
    cursor.execute(
        "CREATE TABLE `set_translations` ("
        "language TEXT,"
        "translation TEXT,"
        "setCode TEXT"
        ")"
    )

    # Build foreignData table
    cursor.execute(
        "CREATE TABLE `foreignData` ("
        "uuid TEXT,"
        "flavorText TEXT,"
        "language TEXT,"
        "multiverseId INTEGER,"
        "name TEXT,"
        "text TEXT,"
        "type TEXT"
        ")"
    )

    # Build legalities table
    cursor.execute(
        "CREATE TABLE `legalities` (" "uuid TEXT," "format TEXT," "status TEXT" ")"
    )

    # Build ruling table
    cursor.execute("CREATE TABLE `rulings` (" "uuid TEXT," "date TEXT," "text TEXT" ")")

    # Build cards table
    cursor.execute(
        "CREATE TABLE `cards` ("
        "artist TEXT,"
        "borderColor TEXT,"
        "colorIdentity TEXT,"
        "colorIndicator TEXT,"
        "colors TEXT,"
        "convertedManaCost FLOAT,"
        "duelDeck TEXT,"
        "faceConvertedManaCost FLOAT,"
        "flavorText TEXT,"
        "frameEffect TEXT,"
        "frameVersion TEXT,"
        "hand TEXT,"
        "hasFoil INTEGER NOT NULL DEFAULT 0,"
        "hasNonFoil INTEGER NOT NULL DEFAULT 0,"
        "isAlternative INTEGER NOT NULL DEFAULT 0,"
        "isOnlineOnly INTEGER NOT NULL DEFAULT 0,"
        "isOversized INTEGER NOT NULL DEFAULT 0,"
        "isReserved INTEGER NOT NULL DEFAULT 0,"
        "isStarter INTEGER NOT NULL DEFAULT 0,"
        "isTimeshifted INTEGER NOT NULL DEFAULT 0,"
        "layout TEXT,"
        "life TEXT,"
        "loyalty TEXT,"
        "manaCost TEXT,"
        "mcmName TEXT DEFAULT NULL,"
        "mcmId INTEGER DEFAULT 0,"
        "mcmMetaId INTEGER DEFAULT 0,"
        "mtgstocksId INTEGER DEFAULT 0,"
        "multiverseId INTEGER,"
        "name TEXT,"
        "names TEXT,"
        "number TEXT,"
        "originalText TEXT,"
        "originalType TEXT,"
        "printings TEXT,"
        "prices TEXT,"
        "power TEXT,"
        "purchaseUrls TEXT,"
        "rarity TEXT,"
        "scryfallId TEXT,"
        "scryfallOracleId TEXT,"
        "scryfallIllustrationId TEXT,"
        "setCode TEXT,"
        "side TEXT,"
        "subtypes TEXT,"
        "supertypes TEXT,"
        "tcgplayerProductId INTEGER,"
        "tcgplayerPurchaseUrl TEXT,"
        "text TEXT,"
        "toughness TEXT,"
        "type TEXT,"
        "types TEXT,"
        "uuid TEXT(36) PRIMARY KEY,"
        "uuidV421 TEXT,"
        "variations TEXT,"
        "watermark TEXT"
        ")"
    )

    # Build tokens table
    cursor.execute(
        "CREATE TABLE `tokens` ("
        "artist TEXT,"
        "borderColor TEXT,"
        "colorIdentity TEXT,"
        "colorIndicator TEXT,"
        "colors TEXT,"
        "duelDeck TEXT,"
        "isOnlineOnly INTEGER NOT NULL DEFAULT 0,"
        "layout TEXT,"
        "loyalty TEXT,"
        "name TEXT,"
        "names TEXT,"
        "number TEXT,"
        "power TEXT,"
        "reverseRelated TEXT,"
        "scryfallId TEXT,"
        "scryfallOracleId TEXT,"
        "scryfallIllustrationId TEXT,"
        "setCode TEXT,"
        "side TEXT,"
        "text TEXT,"
        "toughness TEXT,"
        "type TEXT,"
        "uuid TEXT,"
        "uuidV421 TEXT,"
        "watermark TEXT"
        ")"
    )

    # Execute the commands
    sql_connection.commit()


def parse_and_import_cards(
    input_file: pathlib.Path, sql_connection: sqlite3.Connection
) -> None:
    """
    Parse the JSON cards and input them into the database
    :param input_file: AllSets.json file
    :param sql_connection: Database connection
    """
    LOGGER.info("Loading JSON into memory")
    json_data = json.load(input_file.open("r", encoding="utf8"))

    LOGGER.info("Building sets")
    for set_code, set_data in json_data.items():
        # Handle set insertion
        LOGGER.info("Inserting set row for {}".format(set_code))
        set_insert_values = handle_set_row_insertion(set_data)
        sql_dict_insert(set_insert_values, "sets", sql_connection)

        for card in set_data.get("cards"):
            LOGGER.debug("Inserting card row for {}".format(card.get("name")))
            card_attr: Dict[str, Any] = handle_card_row_insertion(card, set_code)
            sql_insert_all_card_fields(card_attr, sql_connection)

        for token in set_data.get("tokens"):
            LOGGER.debug("Inserting token row for {}".format(token.get("name")))
            token_attr = handle_token_row_insertion(token, set_code)
            sql_dict_insert(token_attr, "tokens", sql_connection)

        for language, translation in set_data["translations"].items():
            LOGGER.debug("Inserting set_translation row for {}".format(language))
            set_translation_attr = handle_set_translation_row_insertion(language, translation, set_code)
            sql_dict_insert(set_translation_attr, "set_translations", sql_connection)

    sql_connection.commit()


def sql_insert_all_card_fields(
    card_attributes: Dict[str, Any], sql_connection: sqlite3.Connection
) -> None:
    """
    Given all of the card's data, insert the data into the
    appropriate SQLite tables.
    :param card_attributes: Tuple of data
    :param sql_connection: DB Connection
    """
    sql_dict_insert(card_attributes["cards"], "cards", sql_connection)

    for foreign_val in card_attributes["foreignData"]:
        sql_dict_insert(foreign_val, "foreignData", sql_connection)

    for legal_val in card_attributes["legalities"]:
        sql_dict_insert(legal_val, "legalities", sql_connection)

    for rule_val in card_attributes["rulings"]:
        sql_dict_insert(rule_val, "rulings", sql_connection)


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
                "multiverseId": entry.get("multiverseId", 0),
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


def handle_set_translation_row_insertion(
    language: str,
    translation: str,
    set_name: str
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
        "setCode": set_name
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
    card_skip_keys = ["foreignData", "legalities", "rulings"]

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

    return {
        "cards": card_insert_values,
        "foreignData": foreign_insert_values,
        "legalities": legal_insert_values,
        "rulings": ruling_insert_values,
    }


def modify_for_sql_insert(data: Any) -> Union[str, int, float]:
    """
    Arrays and booleans can't be inserted, so we need to stringify
    :param data: Data to modify
    :return: string value
    """
    if isinstance(data, (str, int, float)):
        return data

    if isinstance(data, list) and data and isinstance(data[0], str):
        return ", ".join(data)

    if isinstance(data, bool):
        return int(data == "True")

    if isinstance(data, dict):
        return str(data)

    return ""


def sql_dict_insert(
    data: Dict[str, Any], table: str, sql_connection: sqlite3.Connection
) -> None:
    """
    Insert a dictionary into a sqlite table
    :param data: Dict to insert
    :param table: Table to insert to
    :param sql_connection: SQL connection
    """
    cursor = sql_connection.cursor()
    columns = ", ".join(data.keys())
    placeholders = ":" + ", :".join(data.keys())
    query = "INSERT INTO " + table + " (%s) VALUES (%s)" % (columns, placeholders)
    cursor.execute(query, data)
