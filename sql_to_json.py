#!/usr/bin/env python3
import json
import sqlite3
import os
import fileinput

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

    cursor.execute("SELECT DISTINCT setCode from cards LIMIT 10")

    mainDict = []
    returnData = []
    rows = cursor.fetchall()
    for setCode in rows:
        setCode = remove_empty_keys(dict_from_row(setCode))
        cursor.execute("SELECT * FROM cards WHERE setCode = '%s'" % setCode["setCode"])
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
    xml = os.path.join(os.path.expanduser("~/Desktop"), "Output.tmp.json")
    xml2 = os.path.join(os.path.expanduser("~/Desktop"), "Output.json")

    json_code = json.dumps(db_to_json(d), sort_keys=True, indent=2)
    
    writeFile = open(xml, 'w')
    writeFile.write(json_code)
    writeFile.close()
    
    # Additional hacks to now cleanup the file, needs to be redone / hopefully not needed
    with open(xml) as f:
        with open(xml2, 'w') as f2:
            for line in f.readlines():
                if replace_and_write_these_keys(f2, line, "colorIdentity"): continue
                if replace_and_write_these_keys(f2, line, "colors"): continue
                if replace_and_write_these_keys(f2, line, "printings"): continue
                if replace_and_write_these_keys(f2, line, "subtypes"): continue
                if replace_and_write_these_keys(f2, line, "legalities"): continue
                if replace_and_write_these_keys(f2, line, "types"): continue
                #if replace_and_write_these_keys(f2, line, "rulings"): continue
                #if replace_and_write_these_keys(f2, line, "foreignNames"): continue
                
                if '"' + "variations" + '":' in line:
                    f2.write(",")
                    
                f2.write(line)
        f2.close()
    os.remove(xml)                     
     
def replace_and_write_these_keys(file_opened, line, key_val):
    retVal = str_to_json(line, key_val)
    if retVal:
        file_opened.write(retVal)
        return True

def str_to_json(line, key_val):
    if '"' + key_val + '":' in line:
        line_index = line.index('"[')
        line = line[:line_index] + line[line_index:].replace('\\"', '"')[1:]

        while line.strip()[-1:] != "]":
            line = line[:-1]
            
        if key_val != "types":
            line += ","
        line += "\n"
        return line      

if __name__ == '__main__':
    main()