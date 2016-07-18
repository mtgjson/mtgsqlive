#!/usr/bin/env python3
import json
import sqlite3
import time
import os
import sys

# Create the database
def create_db(database_connection):
    c = database_connection.cursor()
    c.execute('create table cards (id, layout, name, names, manaCost, cmc, colors, colorIdentity, type, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, loyalty, multiverseid, variations, imageName, watermark, border, timeshifted, hand, life, reserved, releaseDate, starter, rulings, foreignNames, printings, originalText, originalType, legalities, source, setName, setCode, setReleaseDate, mciNumber)')
    c.execute('create table lastUpdated (datetime)')
    database_connection.commit()
    c.close()
    
def getVal(data, field):
    value = data.get(field)
    if value:
        return json.dumps(value)
    
def json_to_db(json_file_opened, database_connection):    
    c = database_connection.cursor()
    
    # Insert last updated time to database (so if you use the same people know when last updated)
    c.execute('insert into lastUpdated values (?)', [str(time.strftime("%Y-%m-%d %H:%M:%S"))])

    # Get the setnames in the AllSets file and put them into a dictionary for later use
    setNames = []
    for i in json_file_opened.keys():
        setNames.append(i)

    # Iterate through each set, one at a time
    for thisSet in setNames:
        data = json_file_opened[thisSet] # All of the data for the set (I.e. SOI-x.json)

        setName = data["name"]
        setReleaseDate = data["releaseDate"]
        setCards = data["cards"] # Now iterate through the setCards for each card in the set
    
        for thisCard in setCards:
            thisCard_id = getVal(thisCard, "id")
            layout = getVal(thisCard, "layout")
            name = getVal(thisCard, "name")
            names = getVal(thisCard, "names")
            manaCost = getVal(thisCard, "manaCost")
            cmc = getVal(thisCard, "cmc")
            colors = getVal(thisCard, "colors")
            colorIdentity = getVal(thisCard, "colorIdentity")
            thisCard_type = getVal(thisCard, "type")
            supertypes =  getVal(thisCard, "supertypes")
            types =  getVal(thisCard, "types")
            subtypes =  getVal(thisCard, "subtypes")
            rarity = getVal(thisCard, "rarity")
            text = getVal(thisCard, "text")
            flavor = getVal(thisCard, "flavor")
            artist = getVal(thisCard, "artist")
            number = getVal(thisCard, "number")
            power = getVal(thisCard, "power")
            toughness = getVal(thisCard, "toughness")
            loyalty = getVal(thisCard, "loyalty")
            multiverseid = getVal(thisCard, "multiverseid")
            variations = getVal(thisCard, "variations")
            imageName = getVal(thisCard, "imageName")
            watermark = getVal(thisCard, "watermark")
            border = getVal(thisCard, "border")
            timeshifted = getVal(thisCard, "timeshifted")
            hand = getVal(thisCard, "hand")
            life = getVal(thisCard, "life")
            reserved = getVal(thisCard, "reserved")
            releaseDate = getVal(thisCard, "releaseDate")
            starter = getVal(thisCard, "starter")
            rulings = getVal(thisCard, "rulings")
            foreignNames = getVal(thisCard, "foreignNames")
            printings =  getVal(thisCard, "printings")
            originalText = getVal(thisCard, "originalText")
            originalType = getVal(thisCard, "originalType")
            legalities =  getVal(thisCard, "legalities")
            source = getVal(thisCard, "source")
            mciNumber = getVal(thisCard, "mciNumber")
        
            thisCard_data = [thisCard_id, layout, name, names, manaCost, cmc, colors, colorIdentity, thisCard_type, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, loyalty, multiverseid, variations, imageName, watermark, border, timeshifted, hand, life, reserved, releaseDate, starter, rulings, foreignNames, printings, originalText, originalType, legalities, source, setName, thisSet, setReleaseDate, mciNumber]

            # Insert thisCard into the database
            c.execute('insert into cards values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', thisCard_data)
    database_connection.commit()
    c.close()
    
def main():
    i = sys.argv[1] # Should create new DB 
    db_path = os.path.expanduser(sys.argv[2]) # File location for database
    
    if (i == '1'):
        if os.path.isfile(db_path):
            os.remove(db_path)
        db_path = sqlite3.connect(db_path)
        create_db(db_path)
    else:
        db_path = sqlite3.connect(db_path)
        
    json_path = os.path.expanduser(sys.argv[3]) # File location for input file
    json_path = json.load(open(json_path, 'r'))

    json_to_db(json_path, db_path)

if __name__ == '__main__':
    main()