#!/usr/bin/env python3
import json
import sqlite3
import datetime
import os

JSON_FILE = input("Where is the single set JSON file you'd like to import?\n> ") #Input filename via command line
DB_FILE = "Magic Database Single Set" + str(datetime.datetime.now()) + ".db"

traffic = json.load(open(JSON_FILE))
conn = sqlite3.connect(DB_FILE)

c = conn.cursor()
c.execute('create table table_name (name, manaCost, cmc, colors, type_, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, layout, multiverseid, imageName, id_,   names, colorIdentity, loyalty, variations, watermark, border, timeshifted, hand, life, reserved, releaseDate, starter, rulings, foreignNames, printings, originalType, legalities, source)')

def getVal(data, field):
    try: retVal = str(data[field])
    except KeyError: retVal = None
    return retVal

cards = traffic["cards"]
for i in range(0, len(cards)):    
    name = getVal(cards[i], "name")
    manaCost = getVal(cards[i], "manaCost")
    cmc = getVal(cards[i], "cmc")
    colors = getVal(cards[i], "colors")
    type_ = getVal(cards[i], "type")
    supertypes = getVal(cards[i], "supertypes")
    types = getVal(cards[i], "types")
    subtypes = getVal(cards[i], "subtypes")
    rarity = getVal(cards[i], "rarity")
    text = getVal(cards[i], "text")
    flavor = getVal(cards[i], "flavor")
    artist = getVal(cards[i], "artist")
    number = getVal(cards[i], "number")
    power = getVal(cards[i], "power")
    toughness = getVal(cards[i], "toughness")
    layout = getVal(cards[i], "layout")
    multiverseid = getVal(cards[i], "multiverseid")
    imageName = getVal(cards[i], "imageName")
    id_ = getVal(cards[i], "id")
    names = getVal(cards[i], "names")
    colorIdentity = getVal(cards[i], "colorIdentity")
    loyalty = getVal(cards[i], "loyalty")
    variations = getVal(cards[i], "variations")
    watermark = getVal(cards[i], "watermark")
    border = getVal(cards[i], "border")
    timeshifted = getVal(cards[i], "timeshifted")
    hand = getVal(cards[i], "hand")
    life = getVal(cards[i], "life")
    reserved = getVal(cards[i], "reserved")
    releaseDate = getVal(cards[i], "releaseDate")
    starter = getVal(cards[i], "starter")
    rulings = getVal(cards[i], "rulings")
    foreignNames = getVal(cards[i], "foreignNames")
    printings = getVal(cards[i], "printings")
    originalType = getVal(cards[i], "originalType")
    legalities = getVal(cards[i], "legalities")
    source = getVal(cards[i], "source")

    data = [name, manaCost, cmc, colors, type_, supertypes, types, subtypes, rarity, text, flavor, artist, number, power, toughness, layout, multiverseid, imageName, id_, names, colorIdentity, loyalty, variations, watermark, border, timeshifted, hand, life, reserved, releaseDate, starter, rulings, foreignNames, printings, originalType, legalities, source]    

    c.execute('insert into table_name values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', data)

print("Database file can be found at " + os.path.abspath(DB_FILE))
conn.commit()
c.close()