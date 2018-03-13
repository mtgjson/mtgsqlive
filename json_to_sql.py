#!/usr/bin/env python3

import json
import sqlite3
import time
import os
import sys
import shutil

"""
Ensure the database is in the proper formatting
"""
def is_sqlite3_file(filename):
    if not os.path.isfile(filename):
        return False
    if os.path.getsize(filename) < 100:  # SQLite database file header is 100 bytes
        return False

    with open(filename, 'rb') as file_contents:
        header = file_contents.read(100)

    return header[0:16] == b'SQLite format 3\x00'

"""
Create DB Schema that cards will be loaded into.
Params: Active DB connection
"""
def create_db(database_connection):
    cursor = database_connection.cursor()
    try:
        cursor.execute("""create table cards (
                                    cId integer PRIMARY KEY,
                                    id text,
                                    layId integer,
                                    name text,
                                    names text,
                                    manaCost text,
                                    cmc numeric,
                                    type text,
                                    rareId integer,
                                    text text,
                                    flavor text,
                                    artist text,
                                    number text,
                                    power text,
                                    toughness text,
                                    loyalty text,
                                    multiverseid text,
                                    variations text,
                                    imageName text,
                                    watermark text,
                                    borderId integer,
                                    timeshifted boolean,
                                    hand text,
                                    life text,
                                    reserved boolean,
                                    releaseDate text,
                                    starter boolean,
                                    rulings text,
                                    foreignNames text,
                                    originalText text,
                                    originalType text,
                                    source text,
                                    setId integer,
                                    mciNumber text
                                );""")
    except sqlite3.OperationalError as e:
        print(e)
        print("ERROR: A database already exists at this location")
        database_connection.close()
        exit(3)

    # Later maybe add imports for now just re-add cards each time. cursor.execute('create table imports (datetime)')

    #Create lookup tables
    cursor.execute("create table luLayouts (layId integer PRIMARY KEY, layoutName text);")
    cursor.execute("create table luColors (colId integer PRIMARY KEY, colorName text);")
    cursor.execute("create table luColorIdentities (ciId integer PRIMARY KEY, colorIdentityName text);")
    cursor.execute("create table luSupertypes (suptId integer PRIMARY KEY, supertypeName text);")
    cursor.execute("create table luTypes (tId integer PRIMARY KEY, typeName text);")
    cursor.execute("create table luSubtypes (subtId integer PRIMARY KEY, subtypeName text);")
    cursor.execute("create table luRarities (rId integer PRIMARY KEY, rarityName text);")
    cursor.execute("create table luPrintings (pId integer PRIMARY KEY, printingName text);")
    cursor.execute("create table luBorders (bId integer PRIMARY KEY, borderName text);")
    cursor.execute("create table luLegalities (legId integer PRIMARY KEY, legalityName text);")
    cursor.execute("create table luFormats (fId integer PRIMARY KEY, formatName text);")
    cursor.execute("create table luSets (setId integer PRIMARY KEY, setName text, setCode text, setReleaseDate text);")

    #Create links tables
    cursor.execute("create table lnkCardColor (lccId integer PRIMARY KEY, cId integer, colId integer);")
    cursor.execute("create table lnkCardColorIdentity (lcciId integer PRIMARY KEY, cId integer, ciId integer);")
    cursor.execute("create table lnkCardSupertype (lcsuptId integer PRIMARY KEY, cId integer, suptId integer);")
    cursor.execute("create table lnkCardType (lctId integer PRIMARY KEY, cId integer, tId integer);")
    cursor.execute("create table lnkCardSubtype (lcsubtId integer PRIMARY KEY, cId integer, subtId integer);")
    cursor.execute("create table lnkCardPrinting (lcpId integer PRIMARY KEY, cId integer, pId integer);")
    cursor.execute("create table lnkLegalityFormat (llfId integer PRIMARY KEY, legId integer, fId integer);")
    cursor.execute("create table lnkCardLegFormat (lclfId integer PRIMARY KEY, cId integer, llfId integer);")

    #Create Views
    #vwFullCards big join to write later

    database_connection.commit()
    return

"""
Gets the value of the key from the card row. 
Params: Card row, key
"""
def get_value_from_key(row, key):
    return row.get(key)

"""
Gets the id of a value in a lookup table.
"""
def get_db_id(table_name, table_column_name, table_id_column, lookup_value, cursor):
    if lookup_value is None:
        return None

    lookup_query = ""
    legality_id = 0
    format_id = 0

    if table_name == "lnkLegalityFormat":
        legality_rule = json.loads(lookup_value)
        legality_id = get_db_id('luLegalities', 'legalityName', 'legId', legality_rule['legality'], cursor)
        format_id = get_db_id('luFormats', 'formatName', 'fID', legality_rule['format'], cursor)
        lookup_query = "SELECT llfId FROM lnkLegalityFormat WHERE legId = ? AND fId = ?;"
        cursor.execute(lookup_query, (legality_id, format_id))
    else:
        lookup_query = "SELECT {0} FROM {1} WHERE {2} = ?;".format(table_id_column, table_name, table_column_name)
        cursor.execute(lookup_query, (lookup_value,))
    
    returned_values = cursor.fetchall()

    if len(returned_values) == 1:
        thing = returned_values[0][0]
        return thing
    elif len(returned_values) == 0:
        if table_name == "lnkLegalityFormat":
            cursor.execute("INSERT INTO lnkLegalityFormat (legId, fId) VALUES (?, ?);", (legality_id, format_id))
        else:
            cursor.execute("INSERT INTO {0} ({1}) VALUES (?);".format(table_name, table_column_name), (lookup_value,))
        return cursor.lastrowid
    else:
        print("ERROR: Lookup table search returned an unexpected number of results terminating.")
        exit(3)

"""
Convert the JSON into a database format and insert the values
Params: Json data, database connection
"""
def load_cards(json_data, db_connection):
    cursor = db_connection.cursor()

    # Insert last updated time to database (so if you use the same people know when last updated)
    #cursor.execute('insert into imports values (?)', [str(time.strftime("%Y-%m-%d %H:%M:%S"))])

    # Get the set names from the AllSets file and put them into a dictionary for later use
    set_names = []
    for col_name in json_data.keys():
        set_names.append(col_name)

    # Iterate through each set, one at a time
    for card_set_code in set_names:
        set_data = json_data[card_set_code] # All of the data for the set (I.e. SOI-x.json)

        card_set_name = set_data["name"]
        set_release_date = set_data["releaseDate"]
        cursor.execute("INSERT INTO luSets (setName, setCode, setReleaseDate) VALUES (?, ?, ?);", (card_set_name, card_set_code, set_release_date))
        set_db_id = cursor.lastrowid
        cards_in_set = set_data["cards"] # Now iterate through the setCards for each card in the set

        for card_in_set in cards_in_set:
            card_id = get_value_from_key(card_in_set, "id")
            card_layout_id = get_db_id('luLayouts', 'layoutName','layId', get_value_from_key(card_in_set, "layout"),cursor)
            card_name = get_value_from_key(card_in_set, "name")
            card_additional_names = json.dumps(get_value_from_key(card_in_set, "names"))
            card_mana_cost = get_value_from_key(card_in_set, "manaCost")
            card_cmc = get_value_from_key(card_in_set, "cmc")
            card_type = get_value_from_key(card_in_set, "type")
            card_rarity_id = get_db_id('luRarities','rarityName','rId', get_value_from_key(card_in_set, "rarity"), cursor)
            card_text = get_value_from_key(card_in_set, "text")
            card_flavor_text = get_value_from_key(card_in_set, "flavor")
            card_artist = get_value_from_key(card_in_set, "artist")
            card_number = get_value_from_key(card_in_set, "number")
            card_power = get_value_from_key(card_in_set, "power")
            card_toughness = get_value_from_key(card_in_set, "toughness")
            card_loyalty = get_value_from_key(card_in_set, "loyalty")
            card_multiverse_id = get_value_from_key(card_in_set, "multiverseid")
            lan_card_variations = json.dumps(get_value_from_key(card_in_set, "variations"))
            card_image_name = get_value_from_key(card_in_set, "imageName")
            card_watermark = get_value_from_key(card_in_set, "watermark")
            card_border_id = get_db_id('luBorders', 'borderName', 'bId', get_value_from_key(card_in_set, "border"), cursor)
            card_time_shifted = bool(get_value_from_key(card_in_set, "timeshifted"))
            card_hand = get_value_from_key(card_in_set, "hand")
            card_life = get_value_from_key(card_in_set, "life")
            card_reserved = bool(get_value_from_key(card_in_set, "reserved"))
            card_release_date = get_value_from_key(card_in_set, "releaseDate")
            card_starter = bool(get_value_from_key(card_in_set, "starter"))
            card_rulings = json.dumps(get_value_from_key(card_in_set, "rulings"))
            card_foreign_names = json.dumps(get_value_from_key(card_in_set, "foreignNames"))
            card_original_text = get_value_from_key(card_in_set, "originalText")
            card_original_type = get_value_from_key(card_in_set, "originalType")
            card_source = get_value_from_key(card_in_set, "source")
            card_mci_id = get_value_from_key(card_in_set, "mciNumber")

            # Put all fields into an array
            card_data = [
                card_id, card_layout_id, card_name, card_additional_names, card_mana_cost,
                card_cmc, card_type, card_rarity_id, card_text, card_flavor_text,
                card_artist, card_number, card_power, card_toughness, card_loyalty,
                card_multiverse_id, lan_card_variations, card_image_name, card_watermark, card_border_id,
                card_time_shifted, card_hand, card_life, card_reserved, card_release_date,
                card_starter, card_rulings, card_foreign_names, card_original_text,
                card_original_type, card_source, set_db_id, card_mci_id
            ]

            # Insert thisCard into the database
            cursor.execute(
                """INSERT INTO cards 
                                    (id,
                                    layId,
                                    name,
                                    names,
                                    manaCost,
                                    cmc,
                                    type,
                                    rareId,
                                    text,
                                    flavor,
                                    artist,
                                    number,
                                    power,
                                    toughness,
                                    loyalty,
                                    multiverseid,
                                    variations,
                                    imageName,
                                    watermark,
                                    borderId,
                                    timeshifted,
                                    hand,
                                    life,
                                    reserved,
                                    releaseDate,
                                    starter,
                                    rulings,
                                    foreignNames,
                                    originalText,
                                    originalType,
                                    source,
                                    setId,
                                    mciNumber)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",
                card_data
            )

            card_db_id = cursor.lastrowid

            # Link legalities
            card_legalities = get_value_from_key(card_in_set, "legalities")
            if card_legalities:
                for legality in card_legalities:
                    cursor.execute("INSERT INTO lnkCardLegFormat (cId, llfId) VALUES (?, ?);", (card_db_id, get_db_id('lnkLegalityFormat', '', '', json.dumps(legality), cursor)))

            # Link printings
            card_printings = get_value_from_key(card_in_set, "printings")
            for printing in card_printings:
                cursor.execute("INSERT INTO lnkCardPrinting (cId, pId) VALUES (?, ?);", (card_db_id, get_db_id('luPrintings', 'printingName', 'pID', printing, cursor)))

            # Link colors
            card_colors = get_value_from_key(card_in_set, "colors")
            if card_colors:
                for color in card_colors:
                    cursor.execute("INSERT INTO lnkCardColor (cId, colId) VALUES (?, ?);", (card_db_id, get_db_id('luColors','colorName','colId', color, cursor)))

            # Link color identities
            card_color_identities = get_value_from_key(card_in_set, "colorIdentity")
            if card_color_identities:
                for colorIdentity in card_color_identities:
                    cursor.execute("INSERT INTO lnkCardColorIdentity (cId, ciId) VALUES (?, ?);", (card_db_id, get_db_id('luColorIdentities','colorIdentityName','ciId', colorIdentity, cursor)))

            # Link supertypes
            card_supertypes = get_value_from_key(card_in_set, "supertypes")
            if card_supertypes:
                for supertype in card_supertypes:
                    cursor.execute("INSERT INTO lnkCardSupertype (cId, suptId) VALUES (?, ?);", (card_db_id, get_db_id('luSupertypes','supertypeName','suptId', supertype, cursor)))

            # Link types
            card_types = get_value_from_key(card_in_set, "types")
            if card_types:
                for type in card_types:
                    cursor.execute("INSERT INTO lnkCardType (cId, tId) VALUES (?, ?);", (card_db_id, get_db_id('luTypes','typeName','tId', type, cursor)))

            # Link subtypes
            card_subtypes = get_value_from_key(card_in_set, "subtypes")
            if card_subtypes:
                for subtype in card_subtypes:
                    cursor.execute("INSERT INTO lnkCardSubtype (cId, subtId) VALUES (?, ?);", (card_db_id, get_db_id('luSubtypes', 'subtypeName', 'subtId', subtype, cursor)))

    # Push the change and close connection
    db_connection.commit()
    return

"""
Main method, starts the database change process
"""
def main():
    if len(sys.argv) < 3:
        print("Usage: %s <database path> <json path> [append current database]" % sys.argv[0])
        exit(1)

    db_path = os.path.expanduser(sys.argv[1])
    json_path = os.path.expanduser(sys.argv[2])

    try:
        init_db = bool(sys.argv[3])
    except IndexError:
        init_db = False

    # Creating a new database
    if init_db:
        # If the db already exists, stash it
        if os.path.exists(db_path):
            if is_sqlite3_file(db_path):
                old_db_path = db_path + ".old"
                if os.path.exists(old_db_path):
                    print("Removing old saved database...")
                    os.remove(old_db_path)
                print("Moving old database to %s..." % old_db_path)
                shutil.move(db_path, old_db_path)
            else:
                print("ERROR: File at %s is not a database file" % db_path)
                exit(2)

    # Load json data from the file into a variable
    json_data = json.load(open(json_path, 'r'))

    # Open connection we will use to create and load database.
    connection = sqlite3.connect(db_path)

    # Create the empty database schema we will soon load with yummy MTG card data!
    create_db(connection)

    # Use the json variable to load and link card tables.
    load_cards(json_data, connection)

    # Close our db connection and print success!
    connection.close()
    print("Database updated")

    return


if __name__ == '__main__':
    print("Starting JSON to SQLite Conversion...")
    main()
