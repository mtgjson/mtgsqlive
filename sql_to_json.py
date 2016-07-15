#!/usr/bin/env python3
import json
import sqlite3
import os

def dict_from_row(row):
    return dict(zip(row.keys(), row))

def remove_empty_keys(d):
    for k in list(d):
        if not d[k]:
            del d[k]
    return d

def db_to_json(database_connection):
    database_connection.row_factory = sqlite3.Row # Enable keys for the rows
    cursor = database_connection.cursor()

    cursor.execute("SELECT DISTINCT setCode from cards LIMIT 3,5")

    mainDict = []
    returnData = []
    rows = cursor.fetchall()
    for setCode in rows:
        setCode = remove_empty_keys(dict_from_row(setCode))
        cursor.execute("SELECT * FROM cards WHERE setCode = '%s' LIMIT 5" % setCode["setCode"])
        card_rows = cursor.fetchall()

        for row in card_rows:
            row = remove_empty_keys(dict_from_row(row))

            returnData.append(row)

        mainDict.append([setCode, returnData])
        returnData = []
    database_connection.close()
    return mainDict
    
def main():
    #d = os.path.join(os.path.expanduser(input("Location of database: ")), "Magic DB.db")
    d = os.path.join(os.path.expanduser("~/Desktop"), "Magic DB.db")
    d = sqlite3.connect(d)

    #xml = os.path.join(os.path.expanduser(input("Location of save file: ")), "Output.json")
    xml = os.path.join(os.path.expanduser("~/Desktop"), "Output.json")

    json_code = json.dumps(db_to_json(d), indent=4, sort_keys=True)
    
    writeFile = open(xml, 'w')
    writeFile.write(json_code)
    writeFile.close()

if __name__ == '__main__':
    main()