#!/usr/bin/env python3
import json
import sqlite3
import os
import sys


def id(x):
    return x


def dict_from_row(row):
    return {k: ga_decoders.get(k, json.loads)(v) for k, v in zip(row.keys(), row) if v is not None}


def remove_set_info(dictionary):
    dictionary.pop("setName", None)
    dictionary.pop("setCode", None)
    dictionary.pop("setReleaseDate", None)
    return dictionary


def remove_unneeded(dictionary):
    dictionary.pop("artist", None)
    dictionary.pop("border", None)
    dictionary.pop("flavor", None)
    dictionary.pop("foreignNames", None)
    dictionary.pop("id", None)
    dictionary.pop("multiverseid", None)
    dictionary.pop("number", None)
    dictionary.pop("originalText", None)
    dictionary.pop("originalType", None)
    dictionary.pop("rarity", None)
    dictionary.pop("releaseDate", None)
    dictionary.pop("reserved", None)
    dictionary.pop("timeshifted", None)
    dictionary.pop("variations", None)
    dictionary.pop("watermark", None)
    dictionary.pop("imageName", None)
    dictionary.pop("mciNumber", None)
    dictionary = remove_set_info(dictionary)
    return dictionary


def set_dictionary(row):
    return dict(zip(row.keys(), row))


def db_to_json_all_sets(database_connection):
    database_connection.row_factory = sqlite3.Row  # Enable keys for the rows
    cursor = database_connection.cursor()
    cursor.execute("SELECT DISTINCT setCode from cards")

    la_main_dict = {}
    las_rows = cursor.fetchall()
    for ls_set_code in las_rows:
        la_return_data = []
        ls_set_code = set_dictionary(ls_set_code)
        cursor.execute("SELECT * FROM cards WHERE setCode = ?", [ls_set_code["setCode"]])
        card_rows = cursor.fetchall()

        ls_set_name = None
        ls_set_release_date = None
        for las_row in card_rows:
            las_row = dict_from_row(las_row)  # Turn SQL.Row -> Dictionary

            # Set temporary variables used for JSON Sorting data for AllSets
            if not ls_set_name or not ls_set_release_date:
                ls_set_name = las_row["setName"]
                ls_set_release_date = las_row["setReleaseDate"]

            # Remove temporary variables from the dictionary, as they're unneeded
            las_row = remove_set_info(las_row)
            la_return_data.append(las_row)

        # Inset into dictionary the JSON data
        la_main_dict[ls_set_code["setCode"]] = dict(
            zip(["cards", "name", "releaseDate"], [la_return_data, ls_set_name, ls_set_release_date]))

    database_connection.close()
    return la_main_dict


def db_to_json_all_cards(database_connection):
    database_connection.row_factory = sqlite3.Row  # Enable keys for the row
    cursor = database_connection.cursor()

    las_main_dict = {}
    cursor.execute("SELECT DISTINCT name from cards ORDER BY name, setReleaseDate ASC")
    rows = cursor.fetchall()

    # This loop will take a while (~5 minutes) to complete. Be patient
    for this_card in rows:
        cursor.execute("SELECT * FROM cards WHERE name = ?", [this_card["name"]])
        card_rows = cursor.fetchall()
        for row in card_rows:
            row = dict_from_row(row)
            row = remove_unneeded(row)
            las_main_dict[json.loads(this_card["name"])] = row
    return las_main_dict


def main():
    if len(sys.argv) != 4:
        print("USAGE: %s <database input path> <json output path> <'sets' or 'cards'>" % sys.argv[0])
        exit(1)

    ls_db_path = sqlite3.connect(os.path.expanduser(sys.argv[1]))  # File location for database
    ls_json_path = os.path.expanduser(sys.argv[2])  # File location for output
    lb_is_sets = sys.argv[3] == "sets"  # Are we doing sets or cards

    if lb_is_sets:
        dictionary = db_to_json_all_sets(ls_db_path)
    else:
        dictionary = db_to_json_all_cards(ls_db_path)

    with open(ls_json_path, 'w') as json_f:
        json.dump(dictionary, json_f, sort_keys=True, indent=4)


if __name__ == '__main__':
    ga_decoders = {'setName': id, 'setCode': id, 'setReleaseDate': id}
    main()
