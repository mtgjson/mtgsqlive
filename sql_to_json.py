#!/usr/bin/env python3
import json
import sqlite3
import os
import fileinput
import sys

def dict_from_row(row):
    return dict(zip(row.keys(), row))

def remove_empty_keys(d):
    for k in list(d):
        if not d[k]:
            try:
                if d[k] == 0:
                    continue
                del d[k]
            except:
                print("BOOBOO")
    return d

def db_to_json(database_connection):
    database_connection.row_factory = sqlite3.Row # Enable keys for the rows
    cursor = database_connection.cursor()
    cursor.execute("SELECT DISTINCT setCode from cards")

    mainDict = {}
    returnData = []
    rows = cursor.fetchall()
    for setCode in rows:
        setCode = remove_empty_keys(dict_from_row(setCode))
        cursor.execute("SELECT * FROM cards WHERE setCode = '%s'" % setCode["setCode"])
        card_rows = cursor.fetchall()

        setName = None
        setReleaseDate = None
        for row in card_rows:
            row = remove_empty_keys(dict_from_row(row))
            returnData.append(row)
            if not setName and not setReleaseDate:
                setName = row["setName"]
                setReleaseDate = row["setReleaseDate"]

        mainDict[setCode["setCode"]] = dict(zip(["cards", "name", "releaseDate"], [returnData, setName, setReleaseDate]))
        setName = None
        setReleaseDate = None
        returnData = []
    database_connection.close()

    return mainDict
    
def main():
    d = os.path.expanduser(sys.argv[1]) # File location for database
    d = sqlite3.connect(d)

    xml = "/tmp/Output.tmp.json"
    xml2 = os.path.expanduser(sys.argv[2]) # File location for output
    
    json_code = json.dumps(db_to_json(d), sort_keys=True, indent=2)
    
    writeFile = open(xml, 'w')
    writeFile.write(json_code)
    writeFile.close()
    
    int_strings = ('cmc":', 'loyalty":', 'multiverseid":', 'hand":', 'life":')
    bool_strings = ('reserved":', 'starter":', 'timeshifted":')
    skip_strings = ('setCode":', 'setName":', 'setReleaseDate":')
    bonus_comma_strings = ('variations":', 'watermark":')
    fix_rarity = False            
    # Additional hacks to now cleanup the file, needs to be redone / hopefully not needed
    with open(xml) as f:
        with open(xml2, 'w') as f2:
            for line in f.readlines():
                if '"rulings":' in line: continue
                if '"foreignNames":' in line: continue
                if '"printings":' in line: continue
                if '"originalText":' in line: continue
                if '"originalType":' in line: continue
                if '"legalities":' in line: continue
                if '"source":' in line: continue

                if any(s in line for s in skip_strings): continue
                elif any(s in line for s in int_strings):
                    line = str_to_int(line)
                
                if replace_and_write_these_keys(f2, line, "colorIdentity"): continue
                if replace_and_write_these_keys(f2, line, "colors"): continue
                if replace_and_write_these_keys(f2, line, "printings"): continue
                if replace_and_write_these_keys(f2, line, "supertypes"): continue
                if replace_and_write_these_keys(f2, line, "subtypes"): continue
                if replace_and_write_these_keys(f2, line, "legalities"): continue
                if replace_and_write_these_keys(f2, line, "types"): continue
                if replace_and_write_these_keys(f2, line, "names"): continue
                
                #if replace_and_write_these_keys(f2, line, "rulings"): continue
                #if replace_and_write_these_keys(f2, line, "foreignNames"): continue
                
                if any(s in line for s in bool_strings):
                    line = str_to_bool(line)
                    
                
                
                if 'token card' in line:
                    fix_rarity = True
                elif fix_rarity and 'rarity":' in line:
                    while line.strip()[-1:] != '"':
                        line = line[:-1]
                    fix_rarity = False
                
                if any(s in line for s in bonus_comma_strings):
                    f2.write(",")
                    
                if replace_and_write_these_keys(f2, line, "variations"): continue

                f2.write(line)
        f2.close()
        cleanup_json(xml2)
    os.remove(xml)                     

def str_to_int(line):
    try:
        line_to_int = line.index('": "')
        line_after_int = line.index('",')
    except:
        return line

    try:
        line = line[:line_to_int] + '": ' + line[line_to_int + 4:line_after_int] + line[line_after_int + 1:]
    except:
        print(line_to_int, line_after_int, line)

    return line

def str_to_bool(line):
    try:
        line_to_int = line.index('": "')
        line_after_int = line.index('",')
    except:
        return line

    try:
        line = line[:line_to_int] + '": ' + line[line_to_int + 4:line_after_int].lower() + line[line_after_int + 1:]
    except:
        print(line_to_int, line_after_int, line[line_to_int + 4:line_after_int], line)

    return line

def cleanup_json(file_path):
    jsonFile = open(file_path, "r")
    data = json.load(jsonFile)
    jsonFile.close()

    jsonFile = open(file_path, "w+")
    jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
    jsonFile.close()

def replace_and_write_these_keys(file_opened, line, key_val):
    retVal = str_to_json(line, key_val)
    if retVal:
        if key_val != "variations":
            file_opened.write(retVal)
        else:
            while retVal.strip()[-1:] != "]":
                retVal = retVal[:-1]
            file_opened.write(retVal)
        return True

# Yes this is a mess, but it works for now
def str_to_json(line, key_val):
    if '"' + key_val + '":' in line:
        line_index = line.index('"[')

        line = line[:line_index] + line[line_index:].replace('\\"', '"').replace('”', '\\"').replace('“', '\\"').replace('’', "\\'")[1:]

        while line.strip()[-1:] != "]":
            line = line[:-1]
         
        try: line = line[:line_index] + json.dumps(json.loads(line[line_index:]), indent=2)
        except: line = line
            
        if key_val != "types":
            line += ","
        line += "\n"
        return line      

if __name__ == '__main__':
    main()