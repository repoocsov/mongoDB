# "How was working with MongoDB different from working with PostgreSQL? What was easier, and what was harder?"
"""
MongoDB was similar in terms of difficulty. Both followed the same method of opening a connection with credentials (a pipeline).
Subtle differences in the syntax of how to do it. Personally I find MongoDB easier as the data has less limitations on it.
Data types don't have to be specified and there is more flexibility. This has it's downsides though. Less limitations means more
chance for human error.
"""

import pymongo
import sqlite3
import json
import os
from dotenv import load_dotenv, find_dotenv
import psycopg2
from psycopg2.extras import execute_values

"""--------------------------
        LOAD RPG DATA
--------------------------"""
# Connect to DB
DB_FILEPATH = os.path.join(os.path.dirname(__file__), 'rpg_db.sqlite3')
conn = sqlite3.connect(DB_FILEPATH)
curs = conn.cursor()

# Function to transform tables into lists of dictionaries
def get_list_of_dict(keys, list_of_tuples):
     """
     This function will accept keys and list_of_tuples as args and return list of dicts
     """
     list_of_dict = [dict(zip(keys, values)) for values in list_of_tuples]
     return list_of_dict


""" Transforming each sqlite3 table """
# armory_item
sql = """
SELECT *
FROM armory_item
"""
keys = ('item_id', 'name', 'value', 'weight')
armory_items = curs.execute(sql).fetchall()
armory_items = get_list_of_dict(keys, armory_items)

# armory_weapon
sql = """
SELECT *
FROM armory_weapon
"""
keys = ('item_ptr_id', 'power')
armory_weapons = curs.execute(sql).fetchall()
armory_weapons = get_list_of_dict(keys, armory_weapons)

# charactercreator_character
sql = """
SELECT *
FROM charactercreator_character
"""
keys = ('character_id', 'name', 'level', 'exp', 'hp', 'strength', 'intelligence', 'dexterity', 'wisdom')
charactercreator_characters = curs.execute(sql).fetchall()
charactercreator_characters = get_list_of_dict(keys, charactercreator_characters)

# charactercreator_character_inventory
sql = """
SELECT *
FROM charactercreator_character_inventory
"""
keys = ('id', 'character_id', 'item_id')
charactercreator_character_inventorys = curs.execute(sql).fetchall()
charactercreator_character_inventorys = get_list_of_dict(keys, charactercreator_character_inventorys)

# charactercreator_cleric
sql = """
SELECT *
FROM charactercreator_cleric
"""
keys = ('character_ptr_id', 'using_shield', 'mana')
charactercreator_clerics = curs.execute(sql).fetchall()
charactercreator_clerics = get_list_of_dict(keys, charactercreator_clerics)

# charactercreator_figher
sql = """
SELECT *
FROM charactercreator_fighter
"""
keys = ('character_ptr_id', 'using_shield', 'rage')
charactercreator_fighters = curs.execute(sql).fetchall()
charactercreator_fighters = get_list_of_dict(keys, charactercreator_fighters)

# charactercreator_mage
sql = """
SELECT *
FROM charactercreator_mage
"""
keys = ('character_ptr_id', 'has_pet', 'mana')
charactercreator_mages = curs.execute(sql).fetchall()
charactercreator_mages = get_list_of_dict(keys, charactercreator_mages)

# charactercreator_necromancer
sql = """
SELECT *
FROM charactercreator_necromancer
"""
keys = ('mage_ptr_id', 'talisman_charged')
charactercreator_necromancers = curs.execute(sql).fetchall()
charactercreator_necromancers = get_list_of_dict(keys, charactercreator_necromancers)

# charactercreator_thief
sql = """
SELECT *
FROM charactercreator_thief
"""
keys = ('character_ptr_id', 'is_sneaking', 'energy')
charactercreator_thiefs = curs.execute(sql).fetchall()
charactercreator_thiefs = get_list_of_dict(keys, charactercreator_thiefs)


"""----------------------------------
       STORE DATA IN MONGODB
----------------------------------"""
# Check if data is already stored

# Establish a connection
load_dotenv()
MONGO_USER = os.getenv("MONGO_USER", default="OOPS")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER", default="OOPS")
connection_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(connection_uri)

# Create/connect to a cluster (DB) to hold our collections (tables)
db = client.rpg_collection 

if db.collection.countDocuments == 0:
     # Add data to each individual collection
     collection = db.armory_item 
     collection.insert_many(armory_items)

     collection = db.armory_weapon 
     collection.insert_many(armory_weapons)

     collection = db.charactercreator_character 
     collection.insert_many(charactercreator_characters)

     collection = db.charactercreator_character_inventory
     collection.insert_many(charactercreator_character_inventorys)

     collection = db.charactercreator_cleric
     collection.insert_many(charactercreator_clerics)

     collection = db.charactercreator_fighter
     collection.insert_many(charactercreator_fighters)

     collection = db.charactercreator_mage
     collection.insert_many(charactercreator_mages)

     collection = db.charactercreator_necromancer
     collection.insert_many(charactercreator_necromancers)

     collection = db.charactercreator_thief
     collection.insert_many(charactercreator_thiefs)

"""----------------------------------
      ANSWER MONDAY'S QUESTIONS 
----------------------------------"""
# Filter warnings...
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 

# How many total Characters are there?
response = db.charactercreator_character.count()
print("Total Characters:", response)

# How many of each specific subclass?
clerics = db.charactercreator_cleric.count()
fighters = db.charactercreator_fighter.count()
mages = db.charactercreator_mage.count()
necromancers = db.charactercreator_necromancer.count()
thiefs = db.charactercreator_thief.count()
print("Clerics:", clerics, "\nFighters:", fighters, "\nMages:", mages, "\nNecromancers:", necromancers, "\nThieves:", thiefs)

# How many total Items?
response = db.armory_item.count()
print("Total Items:", response)

# How many of the Items are weapons? How many are not?
weapons = db.armory_weapon.count()
nonweapons = (db.armory_item.count() - db.armory_weapon.count())
print("Total Weapons:", weapons, "\nNon-Weapons:", nonweapons)


"""
Aaron - Also, just a general reminder - practice your SQL!
There's a lot this week, but those reps are what will help with the SC
(i.e. it's OK to get a flavor of MongoDB but not answer every question with it).
"""
# How many Items does each character have? (Return first 20 rows)
# response = db.charactercreator_character_inventory.aggregate([
#   {
#     $group: {
#       _id: "$character_id"
#     }
#   }
# ])
# print("Items per character (first 20):", response[0])

# How many Weapons does each character have? (Return first 20 rows)


# On average, how many Items does each Character have?


# On average, how many Weapons does each character have?


