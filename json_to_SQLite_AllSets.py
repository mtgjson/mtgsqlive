#!/usr/bin/env python3
import json
import sqlite3
import time
import os

JSON_FILE = os.path.expanduser(input("Where is the \"AllSets-x.json\" file you'd like to import?\n> "))
DB_FILE = os.path.join(os.path.dirname(JSON_FILE), "Magic Database Complete.db")

# Get the value of the card, return None if NULL
def getVal(data, field):
    val = data.get(field)
    if val:
        return str(val)
    return val

traffic = json.load(open(JSON_FILE))
conn = sqlite3.connect(DB_FILE)

c = conn.cursor()

c.execute('create table cards (id, layout, name, names, manaCost, cmc, colors, colorIdentity, type, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, loyalty, multiverseid, variations, imageName, watermark, border, timeshifted, hand, life, reserved, releaseDate, starter, rulings, foreignNames, printings, originalText, originalType, legalities, source, setName, setCode, setReleaseDate)')

c.execute('create table lastUpdated (datetime)')
c.execute('insert into lastUpdated values (?)', [str(time.strftime("%Y-%m-%d %H:%M:%S"))])

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
        layout = getVal(thisCard, "layout")
        name = getVal(thisCard, "name")
        names = getVal(thisCard, "names")
        manaCost = getVal(thisCard, "manaCost")
        cmc = getVal(thisCard, "cmc")
        colors = getVal(thisCard, "colors")
        colorIdentity = getVal(thisCard, "colorIdentity")
        thisCard_type = getVal(thisCard, "type")
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
        printings = getVal(thisCard, "printings")
        originalText = getVal(thisCard, "originalText")
        originalType = getVal(thisCard, "originalType")
        legalities = getVal(thisCard, "legalities")
        source = getVal(thisCard, "source")
        
        thisCard_data = [thisCard_id, layout, name, names, manaCost, cmc, colors, colorIdentity, thisCard_type, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, loyalty, multiverseid, variations, imageName, watermark, border, timeshifted, hand, life, reserved, releaseDate, starter, rulings, foreignNames, printings, originalText, originalType, legalities, source, setName, thisSet, setReleaseDate]

        c.execute('insert into cards values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', thisCard_data)

print("Database file can be found at " + os.path.abspath(DB_FILE))
conn.commit()
c.close()