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

def set_dictionary(row):
    return dict(zip(row.keys(), row))

def db_to_json(database_connection):
    database_connection.row_factory = sqlite3.Row # Enable keys for the rows
    cursor = database_connection.cursor()
    cursor.execute("SELECT DISTINCT setCode from cards")

    mainDict = {}
    returnData = []
    rows = cursor.fetchall()
    for setCode in rows:
        setCode = set_dictionary(setCode)
        cursor.execute("SELECT * FROM cards WHERE setCode = '%s'" % setCode["setCode"])
        card_rows = cursor.fetchall()

        setName = None
        setReleaseDate = None
        for row in card_rows:
            row = dict_from_row(row)

            if not setName or not setReleaseDate:
                setName = row["setName"]
                setReleaseDate = row["setReleaseDate"]

            row = remove_set_info(row)
            returnData.append(row)

        mainDict[setCode["setCode"]] = dict(zip(["cards", "name", "releaseDate"], [returnData, setName, setReleaseDate]))
        setName = None
        setReleaseDate = None
        returnData = []
    database_connection.close()

    return mainDict
    
def main():
    if len(sys.argv) != 3:
        print("Must provide 2 arguements: database_location, json_output_location")
        os._exit(1)

    db_path = sqlite3.connect(os.path.expanduser(sys.argv[1])) # File location for database
    file_path = os.path.expanduser(sys.argv[2]) # File location for output
    
    dictionary = db_to_json(db_path)

    with open(file_path, 'w') as json_f:
        json.dump(dictionary, json_f, sort_keys=True, indent=4)

if __name__ == '__main__':
    main()