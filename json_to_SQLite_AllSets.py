#!/usr/bin/env python3
import json
import sqlite3
import datetime
import os

JSON_FILE = input("Where is the \"AllSets-x.json\" file you'd like to import?\n> ") #Input filename via command line
DB_FILE = "Magic Database Complete " + str(datetime.datetime.now()) + ".db"

# Get the value of the card, return None if NULL
def getVal(data, field):
    try: retVal = str(data[field])
    except KeyError: retVal = None
    return retVal

traffic = json.load(open(JSON_FILE))
conn = sqlite3.connect(DB_FILE)

c = conn.cursor()
c.execute('create table table_name (name, names, setName, thisSet, setReleaseDate, thisCard_id, manaCost, cmc, colors, colorIdentity, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, loyalty, multiverseid, variations, reserved, rulings, printings, legalities)')


# Get the setnames in the AllSets file and put them into a dictionary for later use
setNames = []
for i in traffic.keys():
    setNames.append(i)

# Iterate through each set, one at a time
for thisSet in setNames:
    data = traffic[thisSet] # All of the data for the set (I.e. SOI-x.json)

    setName = data["name"]
    setReleaseDate = data["releaseDate"]
    
    setCards = data["cards"] # Now iterate through the setCards for each card in the set
    
    for thisCard in setCards:
        thisCard_id = getVal(thisCard, "id")
        name = getVal(thisCard, "name")
        names = getVal(thisCard, "names")
        manaCost = getVal(thisCard, "manaCost")
        cmc = getVal(thisCard, "cmc")
        colors = getVal(thisCard, "colors")
        colorIdentity = getVal(thisCard, "colorIdentity")
        supertypes = getVal(thisCard, "supertypes")
        types = getVal(thisCard, "types")
        subtypes = getVal(thisCard, "subtypes")
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
        reserved = getVal(thisCard, "reserved")
        rulings = getVal(thisCard, "rulings")
        printings = getVal(thisCard, "printings")
        legalities = getVal(thisCard, "legalities")
        
        thisCard_data = [name, names, setName, thisSet, setReleaseDate, thisCard_id, manaCost, cmc, colors, colorIdentity, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, loyalty, multiverseid, variations, reserved, rulings, printings, legalities]

        c.execute('insert into table_name values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', thisCard_data)

print("Database file can be found at " + os.path.abspath(DB_FILE))
conn.commit()
c.close()