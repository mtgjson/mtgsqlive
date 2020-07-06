"""
Convert MTGJSON v4 to SQLite
"""

import json
import logging
import pathlib
import sqlite3
import time
from typing import Any, Dict, List, Union

LOGGER = logging.getLogger(__name__)
JsonDict = Dict[str, any]


def execute(json_input, output_file, check_extras=False) -> None:
    """Main function to handle the logic

    :param json_input: Input file (JSON)
    :param output_file: Output dir
    :param extras: additional json files to process
    """
    if not valid_input_output(json_input, output_file):
        exit(1)
    check_extra_inputs(json_input, output_file)

    LOGGER.info("Loading json file into memory")
    with json_input.open("r", encoding="utf8") as json_file:
        json_data = json.load(json_file)
    build_sql_database(output_file, json_data)
    build_sql_schema(json_data, output_file)
    parse_and_import_cards(json_data, json_input, output_file)
    parse_and_import_extras(json_input, output_file)
    commit_changes_and_close_db(output_file)


def valid_input_output(input_file: pathlib.Path,
                       output_dir: Dict) -> bool:
    """
    Ensure I/O paths are valid and clean for program
    """
    if not input_file.is_file():
        LOGGER.fatal(f"Invalid input file/directory {input_file}")
        return False

    # Create the output directory if it doesn't exist
    output_dir["path"].parent.mkdir(exist_ok=True)
    if output_dir["path"].is_file():
        LOGGER.warning(f"Output path {output_dir['path']} already exists, "
                       "moving it")
        # Backup the existing file to another file with .old extension
        output_dir["path"].replace(
            output_dir["path"].parent.joinpath(
                output_dir["path"].name + ".old"
            )
        )
    return True


def check_extra_inputs(input_file: pathlib.Path,
                       output_dir: Dict, check_extras=False) -> None:
    """
    Check if there are more json files to convert to sql
    """
    extras = ["AllPrices.json", "AllDeckFiles",
              "Keywords.json", "CardTypes.json"]

    for extra in extras:
        output_dir[extra] = False

    if not check_extras:
        return 
    for extra in extras:
        if input_file.parent.joinpath(extra).is_file() or input_file.parent.joinpath(extra).is_dir():
            LOGGER.info("Building with " + extra + " supplement")
            output_dir[extra] = True


def build_sql_database(output_file: str, json_data: JsonDict) -> None:
    if output_file["path"].suffix == ".sql":
        version = get_version(json_data)
        output_file["handle"] = open(output_file["path"], "w", encoding="utf8")
        # Create a file header and ensure utf8 encoding
        output_file["handle"].write(
            "\n".join(
                (
                    "-- MTGSQLive Output File",
                    "-- ({})".format(str(time.strftime("%Y-%m-%d %H:%M:%S"))),
                    "-- MTGJSON Version: {}".format(version),
                    "",
                    "START TRANSACTION;",
                    "SET names 'utf8mb4';",
                    "",
                    "",
                )
            )
        )
    else:
        output_file["handle"] = sqlite3.connect(str(output_file["path"]))
        output_file["handle"].execute("pragma journal_mode=wal;")


def get_version(json_data: Dict) -> str:
    if "meta" in json_data:
        if "version" in json_data["meta"]:
            return json_data["meta"]["version"]
    else:
        for set_code, set_data in json_data.items():
            if "meta" in set_data:
                if "version" in set_data["meta"]:
                    return set_data["meta"]["version"]
    return "Unknown"


def build_sql_schema(json_data: Dict, output_file: Dict) -> None:
    """
    Create the SQLite DB schema
    """
    LOGGER.info("Building SQLite schema")
    if output_file["path"].suffix == ".sql":
        schema = generate_sql_schema(json_data, output_file, "mysql")
        output_file["handle"].write(schema)
        output_file["handle"].write("COMMIT;\n\n")
    else:
        schema = generate_sql_schema(json_data, output_file, "sqlite")
        cursor = output_file["handle"].cursor()
        cursor.executescript(schema)
        output_file["handle"].commit()


def generate_sql_schema(json_data: Dict,
                        output_file: Dict, engine: str) -> str:
    """
    Generate the SQL database schema from the JSON input

    The function loops through the json data and builds an object
    specifying what columns and data types should be in the SQL tables,
    then uses that object to output an actual SQL query string to make
    the table.
    :param json_data: JSON dictionary
    :param engine: target SQL engine
    """
    
    version = get_version(json_data)
    schema = {
        "sets": {},
        "cards": {},
        "tokens": {},
        "rulings": {
            "text": {"type": "TEXT"},
            "date": {"type": "DATE"},
        },
        "legalities": {
            "format": {"type": "TEXT" if engine == "sqlite" else "ENUM"},
            "status": {"type": "TEXT" if engine == "sqlite" else "ENUM"},
        },
        "foreign_data": {
            "flavorText": {"type": "TEXT"},
            "language": {"type": "TEXT" if engine == "sqlite" else "ENUM"},
            "multiverseid": {"type": "INTEGER"},
            "name": {"type": "TEXT"},
            "text": {"type": "TEXT"},
            "type": {"type": "TEXT"},
        },
        "set_translations": {
            "language": {"type": "TEXT" if engine == "sqlite" else "ENUM"},
            "translation": {"type": "TEXT"},
        },
    }
    indexes = {
        "cards": {"uuid": "(36) UNIQUE"},
        "tokens": {"uuid": "(36)"},
        "sets": {"code": "(8) UNIQUE"},
    }
    enums = {
        "sets": ["type"],
        "prices": ["type"],
        "foreign_data": ["language"],
        "set_translations": ["language"],
        "legalities": ["format", "status"],
        "cards": ["borderColor", "frameEffect", "frameVersion", "layout",
                  "rarity"],
        "tokens": ["borderColor", "layout"],
    }
    if "data" in json_data:
        json_data = json_data["data"]
    # To understand the following code you may need to open
    # https://www.mtgjson.com/files/AllPrintings.json
    # to see the json structure
    for setCode, setData in json_data.items():
        # loop through the set properties, you can view the properties in:
        for setKey, setValue in setData.items():
            if setKey == "translations":
                setKey = "set_translations"
            # determine if the set property should be its own table
            if setKey in schema:
                if setKey == "cards" or setKey == "tokens":
                    # loop through each card/token property
                    for card in setValue:
                        for cardKey, cardValue in card.items():
                            if cardKey == "foreignData":
                                cardKey = "foreign_data"
                            # handle identifiers property
                            if cardKey == "identifiers":
                                for idKey, idValue in cardValue.items():
                                    if not idKey in schema[setKey]:
                                        schema[setKey][idKey] = {
                                            "type": get_sql_type(idValue, engine)
                                        }
                                continue
                            # determine if the card/token property is a table
                            if cardKey in schema:
                                # handle enum options
                                if cardKey in enums:
                                    if cardKey == "foreign_data":
                                        if schema[cardKey]["language"]["type"] == "ENUM":
                                            for foreign in cardValue:
                                                if "options" in schema[cardKey]["language"]:
                                                    if foreign["language"] not in schema[cardKey]["language"]["options"]:
                                                        schema[cardKey]["language"]["options"].append(foreign["language"])
                                                else:
                                                    schema[cardKey]["language"]["options"] = [foreign["language"]]
                                    elif cardKey == "legalities":
                                        if schema[cardKey]["format"]["type"] == "ENUM":
                                            for format in cardValue.keys():
                                                if "options" in schema[cardKey]["format"]:
                                                    if format not in schema[cardKey]["format"]["options"]:
                                                        schema[cardKey]["format"]["options"].append(format)
                                                else:
                                                    schema[cardKey]["format"]["options"] = [format]
                                        if schema[cardKey]["status"]["type"] == "ENUM":
                                            for status in cardValue.values():
                                                if "options" in schema[cardKey]["status"]:
                                                    if status not in schema[cardKey]["status"]["options"]:
                                                        schema[cardKey]["status"]["options"].append(status)
                                                else:
                                                    schema[cardKey]["status"]["options"] = [status]
                                    elif cardKey == "prices":
                                        if schema[cardKey]["type"]["type"] == "ENUM":
                                            for type in cardValue.keys():
                                                if "options" in schema[cardKey]["type"]:
                                                    if type not in schema[cardKey]["type"]["options"]:
                                                        schema[cardKey]["type"]["options"].append(type)
                                                else:
                                                    schema[cardKey]["type"]["options"] = [type]
                                # create the 'uuid' foreign key for each reference table
                                if "uuid" not in schema[cardKey]:
                                    if engine == "sqlite":
                                        schema[cardKey]["uuid"] = {
                                            "type": "TEXT(36) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE"
                                        }
                                    else:
                                        schema[cardKey]["uuid"] = {
                                            "type": "CHAR(36) NOT NULL,\n    INDEX(uuid),\n    FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE"
                                        }
                            else:  # 'cards' table properties
                                # determine if the card property is already in the list
                                if cardKey in schema[setKey].keys():
                                    if cardKey in enums[setKey] and not engine == "sqlite":
                                        if cardValue not in schema[setKey][cardKey]["options"]:
                                            schema[setKey][cardKey]["options"].append(cardValue)
                                else:
                                    if cardKey in enums[setKey] and not engine == "sqlite":
                                        schema[setKey][cardKey] = {"type": "ENUM", "options": [cardValue]}
                                    else:
                                        # determine type of the property
                                        schema[setKey][cardKey] = {
                                            "type": get_sql_type(cardValue, engine)
                                        }
                                    # determine if the card property is an index
                                    if cardKey in indexes[setKey]:
                                        if engine == "sqlite":
                                            schema[setKey][cardKey]["type"] += (
                                                indexes[setKey][cardKey] + " NOT NULL"
                                            )
                                        else:
                                            schema[setKey][cardKey]["type"] = (
                                                "CHAR"
                                                + indexes[setKey][cardKey]
                                                + " NOT NULL"
                                            )
                if setKey == "set_translations":
                    if schema[setKey]["language"]["type"] == "ENUM":
                        if setValue:
                            for language in setValue.keys():
                                if "options" not in schema[setKey]["language"]:
                                    schema[setKey]["language"]["options"] = [language]
                                else:
                                    if language not in schema[setKey]["language"]["options"]:
                                        schema[setKey]["language"]["options"].append(language)
                # add 'setCode' to each table that references 'sets'
                if "setCode" not in schema[setKey]:
                    if engine == "sqlite":
                        schema[setKey]["setCode"] = {
                            "type": "TEXT(8) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE"
                        }
                    else:
                        schema[setKey]["setCode"] = {
                            "type": "VARCHAR(8) NOT NULL,\n    INDEX(setCode),\n    FOREIGN KEY (setCode) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE"
                        }
            else:  # 'sets' table properties
                if setKey in schema["sets"].keys():
                    # if the property type is enum add the value to list if necessary
                    if setKey in enums["sets"] and not engine == "sqlite":
                        if setValue not in schema["sets"][setKey]["options"]:
                            schema["sets"][setKey]["options"].append(setValue)
                else:
                    # determine type of the set property
                    if setKey in enums["sets"] and not engine == "sqlite":
                        schema["sets"][setKey] = {"type": "ENUM", "options": [setValue]}
                    elif setKey == "releaseDate":
                        schema["sets"][setKey] = {"type": "DATE"}
                    else:
                        schema["sets"][setKey] = {
                            "type": get_sql_type(setValue, engine)
                        }
                    if setKey in indexes["sets"]:
                        if engine == "sqlite":
                            schema["sets"][setKey]["type"] += (
                                indexes["sets"][setKey] + " NOT NULL"
                            )
                        else:
                            schema["sets"][setKey]["type"] = (
                                "VARCHAR" + indexes["sets"][setKey] + " NOT NULL"
                            )

    # add extra tables manually if necessary
    if output_file["AllPrices.json"] or version.startswith("4"):
        schema["prices"] = {
            "uuid": { "type": "TEXT(36) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE" if engine == "sqlite" else "CHAR(36) NOT NULL,\n    INDEX(uuid),\n    FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE" },
            "price": {"type": "FLOAT" if engine == "sqlite" else "DECIMAL(8,2)"},
            "type": {"type": "TEXT" if engine == "sqlite" else "ENUM"},
            "date": {"type": "DATE"},
        }
    
    if output_file["AllDeckFiles"]:
        schema["decks"] = {
            "fileName": {"type": "TEXT"},
            "name": {"type": "TEXT"},
            "mainboard": {"type": "TEXT NOT NULL"},
            "sideboard": {"type": "TEXT"},
            "type": {"type": "TEXT"},
            "releaseDate": {"type": "TEXT"},
            "code": {
                "type": "TEXT(8) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE"
                if engine == "sqlite"
                else "VARCHAR(8) NOT NULL,\n    INDEX(code),\n    FOREIGN KEY (code) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE"
            },
        }
    if output_file["Keywords.json"]:
        schema["keywords"] = {
            "word": {"type": "TEXT UNIQUE NOT NULL"},
            "type": {"type": "TEXT NOT NULL"},
        }
    if output_file["CardTypes.json"]:
        schema["types"] = {
            "type": {"type": "TEXT UNIQUE NOT NULL"},
            "subTypes": {"type": "TEXT"},
            "supertypes": {"type": "TEXT"},
        }
    return get_query_from_dict(schema, engine)


def get_sql_type(mixed, engine: str) -> str:
    """
    Return a string with the type of the parameter mixed

    The type depends on the SQL engine in some cases
    """
    if isinstance(mixed, str) or isinstance(mixed, list) or isinstance(mixed, dict):
        return "TEXT"
    elif isinstance(mixed, bool):
        if engine == "sqlite":
            return "INTEGER NOT NULL DEFAULT 0"
        else:
            return "TINYINT(1) NOT NULL DEFAULT 0"
    elif isinstance(mixed, float):
        return "FLOAT"
    elif isinstance(mixed, int):
        return "INTEGER"
    return "TEXT"


def get_query_from_dict(schema, engine):
    q = ""
    for table_name, table_data in schema.items():
        q += f"CREATE TABLE `{table_name}` (\n"
        if engine == "sqlite":
            q += "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
        else:
            q += "    id INTEGER PRIMARY KEY AUTO_INCREMENT,\n"
        for attribute in sorted(table_data.keys()):
            if table_data[attribute]["type"] == "ENUM" and "options" not in table_data[attribute]:
                table_data[attribute]["type"] = "TEXT"
            q += f"    {attribute} {table_data[attribute]['type']}"
            if table_data[attribute]["type"] == "ENUM":
                q += "('" + "', '".join(table_data[attribute]["options"]) + "')"
            q += ",\n"
        if engine == "sqlite":
            q = q[:-2] + "\n);\n\n"
        else:
            q = q[:-2] + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n"

    return q


def parse_and_import_cards(
    json_data: Dict, input_file: pathlib.Path, output_file: Dict
) -> None:
    """
    Parse the JSON cards and input them into the database

    :param input_file: AllPrintings.json file
    :param output_file: Output info dictionary
    """
    LOGGER.info("Building sets")
    if "data" in json_data:
        json_data = json_data["data"]
    for set_code, set_data in json_data.items():
        LOGGER.info(f"Inserting set row for {set_code}")
        set_insert_values = handle_set_row_insertion(set_data)
        sql_dict_insert(set_insert_values, "sets", output_file)

        for card in set_data.get("cards"):
            LOGGER.debug(f"Inserting card row for {card.get('name')}")
            card_attr: JsonDict = handle_card_row_insertion(card, set_code)
            sql_insert_all_card_fields(card_attr, output_file)

        for token in set_data.get("tokens"):
            LOGGER.debug(f"Inserting token row for {token.get('name')}")
            token_attr = handle_token_row_insertion(token, set_code)
            sql_dict_insert(token_attr, "tokens", output_file)

        for language, translation in set_data.get("translations", {}).items():
            LOGGER.debug(f"Inserting set_translation row for {language}")
            set_translation_attr = handle_set_translation_row_insertion(
                language, translation, set_code
            )
            sql_dict_insert(set_translation_attr, "set_translations", output_file)


def handle_set_row_insertion(set_data: JsonDict) -> JsonDict:
    """
    This method will take the set data and convert it,
    preparing for SQLite insertion

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


def handle_card_row_insertion(card_data: JsonDict, set_name: str) -> JsonDict:
    """
    This method will take the card data and convert it,
    preparing for SQLite insertion

    :param card_data: Data to process
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    # ORDERING MATTERS HERE
    card_skip_keys = ["foreignData", "legalities", "rulings", "prices"]

    card_insert_values: JsonDict = {"setCode": set_name}
    for key, value in card_data.items():
        if key in card_skip_keys:
            continue
        if key == "identifiers":
            for idKey, idValue in value.items():
                card_insert_values[idKey] = modify_for_sql_insert(idValue)
        else:
            card_insert_values[key] = modify_for_sql_insert(value)

    foreign_insert_values: List[JsonDict] = []
    if card_skip_keys[0] in card_data.keys():
        foreign_insert_values = handle_foreign_rows(card_data, card_data["uuid"])

    legal_insert_values: List[JsonDict] = []
    if card_skip_keys[1] in card_data.keys():
        legal_insert_values = handle_legal_rows(card_data, card_data["uuid"])

    ruling_insert_values: List[JsonDict] = []
    if card_skip_keys[2] in card_data.keys():
        ruling_insert_values = handle_ruling_rows(card_data, card_data["uuid"])

    price_insert_values: List[JsonDict] = []
    if card_skip_keys[3] in card_data.keys():
        price_insert_values = handle_price_rows(card_data, card_data["uuid"])

    return {
        "cards": card_insert_values,
        "foreign_data": foreign_insert_values,
        "legalities": legal_insert_values,
        "rulings": ruling_insert_values,
        "prices": price_insert_values,
    }


def sql_insert_all_card_fields(
    card_attributes: JsonDict, output_file: Dict
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

    if not output_file["AllPrices.json"]:
        for price_val in card_attributes["prices"]:
            sql_dict_insert(price_val, "prices", output_file)


def handle_token_row_insertion(token_data: JsonDict, set_name: str) -> JsonDict:
    """
    This method will take the token data and convert it,
    preparing for SQLite insertion

    :param token_data: Data to process
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    token_insert_values: JsonDict = {"setCode": set_name}
    for key, value in token_data.items():
        if key == "identifiers":
            for idKey, idValue in value.items():
                token_insert_values[idKey] = modify_for_sql_insert(idValue)
        else:
            token_insert_values[key] = modify_for_sql_insert(value)

    return token_insert_values


def handle_set_translation_row_insertion(
    language: str, translation: str, set_name: str
) -> JsonDict:
    """
    This method will take the set translation data and convert it,
    preparing for SQLite insertion

    :param language: The language of the set translation
    :param translation: The set name translated in to the given language
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    set_translation_insert_values: JsonDict = {
        "language": language,
        "translation": translation,
        "setCode": set_name,
    }

    return set_translation_insert_values


def parse_and_import_extras(input_file: pathlib.Path, output_file: Dict) -> None:
    """
    Parse the extra data files and input them into the database

    :param input_file: AllPrintings.json file
    :param output_file: Output info dictionary
    """
    if output_file["AllPrices.json"]:
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

    if output_file["AllDeckFiles"]:
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
            if "fileName" not in deck_data:
                deck_data["fileName"] = deck_file.stem
            sql_dict_insert(deck_data, "decks", output_file)

    if output_file["Keywords.json"]:
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

    if output_file["CardTypes.json"]:
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


def handle_foreign_rows(
    card_data: JsonDict, card_uuid: str
) -> List[JsonDict]:
    """
    This method will take the card data and convert it,
    preparing for SQLite insertion

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
    card_data: JsonDict, card_uuid: str
) -> List[JsonDict]:
    """
    This method will take the card data and convert it,
    preparing for SQLite insertion

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
    card_data: JsonDict, card_uuid: str
) -> List[JsonDict]:
    """This method will take the card data and convert it,
    preparing for SQLite insertion

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
    card_data: JsonDict, card_uuid: str
) -> List[JsonDict]:
    """This method will take the card data and convert it,
    preparing for SQLite insertion

    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts, ready for insertion
    """
    prices = []
    for price_type in card_data["prices"]:
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


def modify_for_sql_file(data: JsonDict) -> JsonDict:
    for key in data.keys():
        if isinstance(data[key], str):
            data[key] = "'" + data[key].replace("'", "''") + "'"
        if str(data[key]) == "False":
            data[key] = 0
        if str(data[key]) == "True":
            data[key] = 1
        if data[key] is None:
            data[key] = "NULL"
    return data


def sql_dict_insert(data: JsonDict, table: str, output_file: Dict) -> None:
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
        LOGGER.warning(f"Failed to insert row in '{table}' with values: {datastr}")


def commit_changes_and_close_db(output_file: Dict) -> None:
    if output_file["path"].suffix == ".sql":
        output_file["handle"].write("COMMIT;")
    else:
        output_file["handle"].commit()
    output_file["handle"].close()
