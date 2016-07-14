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

    cursor.execute(""" SELECT * from cards """)

    returnData = ""
    rows = cursor.fetchall()
    for row in rows:
        row = remove_empty_keys(dict_from_row(row))
        dump = json.dumps(row, sort_keys=True)
        returnData += dump
    
    database_connection.close()
    return returnData
    
def main():
    d = os.path.join(os.path.expanduser(input("Location of database: ")), "Magic DB.db")
    d = sqlite3.connect(d)
    
    xml = os.path.join(os.path.expanduser(input("Location of save file: ")), "Output.json")
        
    json_code = db_to_json(d)
    
    writeFile = open(xml, 'w')
    writeFile.write(json_code)
    writeFile.close()

if __name__ == '__main__':
    main()