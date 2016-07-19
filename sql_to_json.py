#!/usr/bin/env python3
import json
import sqlite3
import os
import fileinput
import sys

def id(x):
    return x

DECODERS = { 'setName': id, 'setCode': id, 'setReleaseDate': id }

def dict_from_row(row):
    return {k: DECODERS.get(k, json.loads)(v) for k, v in zip(row.keys(), row) if v is not None}    

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

def db_to_json_allsets(database_connection):
    database_connection.row_factory = sqlite3.Row # Enable keys for the rows
    cursor = database_connection.cursor()
    cursor.execute("SELECT DISTINCT setCode from cards")

    mainDict = {}
    returnData = []
    rows = cursor.fetchall()
    for setCode in rows:
        setCode = set_dictionary(setCode)
        cursor.execute("SELECT * FROM cards WHERE setCode = ?" % [setCode["setCode"]])
        card_rows = cursor.fetchall()

        setName = None
        setReleaseDate = None
        for row in card_rows:
            row = dict_from_row(row) # Turn SQL.Row -> Dictionary

            # Set temporary variables used for JSON Sorting data for AllSets
            if not setName or not setReleaseDate:
                setName = row["setName"]
                setReleaseDate = row["setReleaseDate"]

            # Remove temporary variables from the dictionary, as they're unneeded
            row = remove_set_info(row)
            returnData.append(row)

        # Inset into dictionary the JSON data
        mainDict[setCode["setCode"]] = dict(zip(["cards", "name", "releaseDate"], [returnData, setName, setReleaseDate]))

        # Reset variables
        setName = None
        setReleaseDate = None
        returnData = []

    database_connection.close()

    return mainDict

def db_to_json_allcards(database_connection):
    database_connection.row_factory = sqlite3.Row # Enable keys for the row
    cursor = database_connection.cursor()
    
    mainDict = {}
    cursor.execute("SELECT DISTINCT name from cards ORDER BY name, setReleaseDate ASC")
    rows = cursor.fetchall()

    # This loop will take a while (~5 minutes) to complete. Be patient
    for this_card in rows:
        cursor.execute("SELECT * FROM cards WHERE name = ?", [this_card["name"]])
        card_rows = cursor.fetchall()
        for row in card_rows:
            row = dict_from_row(row)
            row = remove_unneeded(row)
            mainDict[json.loads(this_card["name"])] = row
    return mainDict

def main():
    if len(sys.argv) != 4:
        print("Must provide 3 arguements: database_location, json_output_location, sets_or_cards")
        os._exit(1)

    db_path = sqlite3.connect(os.path.expanduser(sys.argv[1])) # File location for database
    file_path = os.path.expanduser(sys.argv[2]) # File location for output
    
    if (sys.argv[3] == "sets"):
        dictionary = db_to_json_allsets(db_path)
    elif (sys.argv[3] == "cards"):
        dictionary = db_to_json_allcards(db_path)
    else:
        print('Final arguement must be "sets" or "cards"')
        os._exit(1)

    with open(file_path, 'w') as json_f:
        json.dump(dictionary, json_f, sort_keys=True, indent=4)

if __name__ == '__main__':
    main()