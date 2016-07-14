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

def db_to_json(database, fileName="", writeToFile=True):
    if (writeToFile):
        printFile = open(fileName, 'w')
    
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row # Enable keys for the rows
    cursor = conn.cursor()
 
    cursor.execute(""" SELECT * from cards """)

    rows = cursor.fetchall()

    for row in rows:
        row = remove_empty_keys(dict_from_row(row))
        
        dump = json.dumps(row, sort_keys=True)
        if (writeToFile):
            printFile.write(dump)
        else:
            print(dump)
    
    conn.close()
    printFile.close()
    
def main():
    d = os.path.join(os.path.expanduser(input("Location of database: ")), "Magic DB.db")
    xml = os.path.join(os.path.expanduser(input("Location of save file: ")), "Output.json")
    db_to_json(d, xml)

if __name__ == '__main__':
    main()