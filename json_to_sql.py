#!/usr/bin/env python3
import json
import sqlite3
import time
import os
import sys
import re

def getVal(data, field):
    val = data.get(field)
    if val:
        return str(val).replace('”', '"').replace('“', '"').replace('’', "'").replace("\\", "")
    return val
    
def fixJson_foreign(data):
    if data:
        data = data.replace("'language'", '"language"')
        data = data.replace("'multiverseid'", '"multiverseid"')
        data = data.replace("'name'", '"name"')
        data = data.replace("', \"", '", "')
        data = data.replace("\": '", "\": \"")
        data = data.replace("'}", '"}')
    return data

def fixJson(data):
    if data:
        p = re.compile("[\w]'[\w]")
        for m in p.finditer(data):
            data = data.replace(m.group(), m.group().replace("'", "TMP_HOLD"))

        p = re.compile("[\w]\"[\w]")
        for m in p.finditer(data):
            data = data.replace(m.group(), m.group().replace("'", "DREAK_HOLD"))
        data = data.replace("'", '"').replace("TMP_HOLD", "'").replace("DREAK_HOLD", '"')
    return data

def create_db(database_connection):
    c = database_connection.cursor()
    c.execute('create table cards (id, layout, name, names, manaCost, cmc, colors, colorIdentity, type, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, loyalty, multiverseid, variations, imageName, watermark, border, timeshifted, hand, life, reserved, releaseDate, starter, rulings, foreignNames, printings, originalText, originalType, legalities, source, setName, setCode, setReleaseDate, mciNumber)')
    c.execute('create table lastUpdated (datetime)')
    database_connection.commit()
    c.close()

def json_to_db(json_file_opened, database_connection):    
    c = database_connection.cursor()
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
            names = fixJson( getVal(thisCard, "names") )
            manaCost = getVal(thisCard, "manaCost")
            cmc = getVal(thisCard, "cmc")
            colors = fixJson( getVal(thisCard, "colors") )
            colorIdentity = fixJson( getVal(thisCard, "colorIdentity") )
            thisCard_type = getVal(thisCard, "type")
            supertypes = fixJson( getVal(thisCard, "supertypes") )
            types = fixJson( getVal(thisCard, "types") )
            subtypes = fixJson( getVal(thisCard, "subtypes") )
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
            rulings = ( getVal(thisCard, "rulings") )
            foreignNames = fixJson_foreign( getVal(thisCard, "foreignNames") )
            printings = fixJson( getVal(thisCard, "printings") )
            originalText = getVal(thisCard, "originalText")
            originalType = getVal(thisCard, "originalType")
            legalities = fixJson( getVal(thisCard, "legalities") )
            source = getVal(thisCard, "source")
            mciNumber = getVal(thisCard, "mciNumber")
        
            thisCard_data = [thisCard_id, layout, name, names, manaCost, cmc, colors, colorIdentity, thisCard_type, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, loyalty, multiverseid, variations, imageName, watermark, border, timeshifted, hand, life, reserved, releaseDate, starter, rulings, foreignNames, printings, originalText, originalType, legalities, source, setName, thisSet, setReleaseDate, mciNumber]

            c.execute('insert into cards values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', thisCard_data)
    database_connection.commit()
    c.close()
    
def main():
    i = sys.argv[1] # Should create new DB 
    d = os.path.expanduser(sys.argv[2]) # File location for database
    
    if (i == '1'):
        if os.path.isfile(d):
            os.remove(d)
        d = sqlite3.connect(d)
        create_db(d)
    else:
        d = sqlite3.connect(d)
        
    xml = os.path.expanduser(sys.argv[3]) # File location for input file
    xml = json.load(open(xml, 'r'))

    json_to_db(xml, d)

if __name__ == '__main__':
    main()