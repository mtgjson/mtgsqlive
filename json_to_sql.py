#!/usr/bin/env python3
import json
import sqlite3
import time
import os
import sys
import shutil

"""
Create the database
Params: Active DB connection
"""


def create_db(ac_database_connection):
    lc_connection = ac_database_connection.cursor()
    try:
        lc_connection.execute('create table cards'
                              '('
                              'id,'
                              'layout,'
                              'name,'
                              'names,'
                              'manaCost,'
                              'cmc,'
                              'colors,'
                              'colorIdentity,'
                              'type,'
                              'supertypes,'
                              'types,'
                              'subtypes,'
                              'rarity,'
                              'text,'
                              'flavor,'
                              'artist,'
                              'number,'
                              'power,'
                              'toughness,'
                              'loyalty,'
                              'multiverseid,'
                              'variations,'
                              'imageName,'
                              'watermark,'
                              'border,'
                              'timeshifted,'
                              'hand,'
                              'life,'
                              'reserved,'
                              'releaseDate,'
                              'starter,'
                              'rulings,'
                              'foreignNames,'
                              'printings,'
                              'originalText,'
                              'originalType,'
                              'legalities,'
                              'source,'
                              'setName,'
                              'setCode,'
                              'setReleaseDate,'
                              'mciNumber'
                              ')'
                              )
    except sqlite3.OperationalError:
        print("ERROR: A database already exists at this location")
        lc_connection.close()
        exit(3)

    lc_connection.execute('create table lastUpdated (datetime)')
    ac_database_connection.commit()
    lc_connection.close()
    return


"""
Gets the value of the key from the card row. 
Params: Card row, key
"""


def get_value_from_key(aac_row, as_key):
    ls_value = aac_row.get(as_key)
    if ls_value:
        return json.dumps(ls_value)


"""
Convert the JSON into a database format and insert the values
Params: Json data, database connection
"""


def json_to_db(aas_json_data, ac_db_connection):
    lc_connection = ac_db_connection.cursor()

    # Insert last updated time to database (so if you use the same people know when last updated)
    lc_connection.execute('insert into lastUpdated values (?)', [str(time.strftime("%Y-%m-%d %H:%M:%S"))])

    # Get the set names from the AllSets file and put them into a dictionary for later use
    las_set_names = []
    for ls_col_name in aas_json_data.keys():
        las_set_names.append(ls_col_name)

    # Iterate through each set, one at a time
    for ls_card_set_code in las_set_names:
        las_set_data = aas_json_data[ls_card_set_code]  # All of the data for the set (I.e. SOI-x.json)

        ls_card_set_name = las_set_data["name"]
        ls_set_release_date = las_set_data["releaseDate"]
        las_cards_in_set = las_set_data["cards"]  # Now iterate through the setCards for each card in the set

        for las_card_in_set in las_cards_in_set:
            ls_card_id = get_value_from_key(las_card_in_set, "id")
            ls_card_layout = get_value_from_key(las_card_in_set, "layout")
            ls_card_name = get_value_from_key(las_card_in_set, "name")
            las_card_additional_names = get_value_from_key(las_card_in_set, "names")
            ls_card_mana_cost = get_value_from_key(las_card_in_set, "manaCost")
            ln_card_cmc = get_value_from_key(las_card_in_set, "cmc")
            las_card_colors = get_value_from_key(las_card_in_set, "colors")
            las_card_color_identity = get_value_from_key(las_card_in_set, "colorIdentity")
            ls_card_type = get_value_from_key(las_card_in_set, "type")
            las_card_supertypes = get_value_from_key(las_card_in_set, "supertypes")
            las_card_types = get_value_from_key(las_card_in_set, "types")
            las_card_subtypes = get_value_from_key(las_card_in_set, "subtypes")
            ls_card_rarity = get_value_from_key(las_card_in_set, "rarity")
            ls_card_text = get_value_from_key(las_card_in_set, "text")
            ls_card_flavor_text = get_value_from_key(las_card_in_set, "flavor")
            ls_card_artist = get_value_from_key(las_card_in_set, "artist")
            ls_card_number = get_value_from_key(las_card_in_set, "number")
            ls_card_power = get_value_from_key(las_card_in_set, "power")
            ls_card_toughness = get_value_from_key(las_card_in_set, "toughness")
            ls_card_loyalty = get_value_from_key(las_card_in_set, "loyalty")
            ln_card_multiverse_id = get_value_from_key(las_card_in_set, "multiverseid")
            lan_card_variations = get_value_from_key(las_card_in_set, "variations")
            ls_card_image_name = get_value_from_key(las_card_in_set, "imageName")
            ls_card_watermark = get_value_from_key(las_card_in_set, "watermark")
            ls_card_border = get_value_from_key(las_card_in_set, "border")
            lb_card_time_shifted = get_value_from_key(las_card_in_set, "timeshifted")
            ln_card_hand = get_value_from_key(las_card_in_set, "hand")
            ln_card_life = get_value_from_key(las_card_in_set, "life")
            lb_card_reserved = get_value_from_key(las_card_in_set, "reserved")
            ls_card_release_date = get_value_from_key(las_card_in_set, "releaseDate")
            lb_card_starter = get_value_from_key(las_card_in_set, "starter")
            las_card_rulings = get_value_from_key(las_card_in_set, "rulings")
            las_card_foreign_names = get_value_from_key(las_card_in_set, "foreignNames")
            las_card_printings = get_value_from_key(las_card_in_set, "printings")
            ls_card_original_text = get_value_from_key(las_card_in_set, "originalText")
            ls_card_original_type = get_value_from_key(las_card_in_set, "originalType")
            las_card_legalities = get_value_from_key(las_card_in_set, "legalities")
            ls_card_source = get_value_from_key(las_card_in_set, "source")
            ln_card_mci_id = get_value_from_key(las_card_in_set, "mciNumber")

            # Put all fields into an array
            las_card_data = [
                ls_card_id, ls_card_layout, ls_card_name, las_card_additional_names, ls_card_mana_cost,
                ln_card_cmc, las_card_colors, las_card_color_identity, ls_card_type, las_card_supertypes,
                las_card_types, las_card_subtypes, ls_card_rarity, ls_card_text, ls_card_flavor_text,
                ls_card_artist, ls_card_number, ls_card_power, ls_card_toughness, ls_card_loyalty,
                ln_card_multiverse_id, lan_card_variations, ls_card_image_name, ls_card_watermark, ls_card_border,
                lb_card_time_shifted, ln_card_hand, ln_card_life, lb_card_reserved, ls_card_release_date,
                lb_card_starter, las_card_rulings, las_card_foreign_names, las_card_printings, ls_card_original_text,
                ls_card_original_type, las_card_legalities, ls_card_source, ls_card_set_name, ls_card_set_code,
                ls_set_release_date, ln_card_mci_id
            ]

            # Insert thisCard into the database
            lc_connection.execute(
                'insert into cards values'
                '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                las_card_data
            )

    # Push the change and close connection
    ac_db_connection.commit()
    lc_connection.close()
    print("Database updated")
    return


"""
Main method, starts the database change process
"""


def main():
    if len(sys.argv) < 3:
        print("Usage: %s <database path> <json path> [append current database]" % sys.argv[0])
        exit(1)

    as_db_path = os.path.expanduser(sys.argv[1])
    as_json_path = os.path.expanduser(sys.argv[2])

    try:
        ab_create_db = bool(sys.argv[3])
    except IndexError:
        ab_create_db = False

    # Creating a new database
    if ab_create_db:
        # If the db already exists, stash it
        if os.path.exists(as_db_path):
            if is_sqlite3_file(as_db_path):
                ls_old_db_path = as_db_path + ".old"
                if os.path.exists(ls_old_db_path):
                    print("Removing old saved database...")
                    os.remove(ls_old_db_path)
                print("Moving old database to %s..." % ls_old_db_path)
                shutil.move(as_db_path, ls_old_db_path)
            else:
                print("ERROR: File at %s is not a database file" % as_db_path)
                exit(2)

    lc_connection = sqlite3.connect(as_db_path)
    create_db(lc_connection)

    # Load json data from the file into a variable
    las_json_data = json.load(open(as_json_path, 'r'))

    # Convert the JSON variable into a full database
    json_to_db(las_json_data, lc_connection)

    return


"""
Ensure the database is in the proper formatting
"""


def is_sqlite3_file(as_filename):
    if not os.path.isfile(as_filename):
        return False
    if os.path.getsize(as_filename) < 100:  # SQLite database file header is 100 bytes
        return False

    with open(as_filename, 'rb') as ls_file_contents:
        header = ls_file_contents.read(100)

    return header[0:16] == b'SQLite format 3\x00'


if __name__ == '__main__':
    print("Starting JSON to SQLite Conversion...")
    main()
