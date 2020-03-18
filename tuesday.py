import sqlite3
import os
import psycopg2
from dotenv import load_dotenv
import json
from psycopg2.extras import execute_values

"""
  Using sqlite to retrieve rpg DB info that will be inserted into the postgres DB
"""
# DB_FILEPATH = 'rpg_db.sqlite3'
DB_FILEPATH = os.path.join(os.path.dirname(__file__), 'rpg_db.sqlite3')

# Connect to DB
conn = sqlite3.connect(DB_FILEPATH)
curs = conn.cursor()
tables = []

sql = """
SELECT *
FROM armory_item
"""
armory_items = curs.execute(sql).fetchall()
tables.append(armory_items)

sql = """
SELECT *
FROM armory_weapon
"""
armory_weapons = curs.execute(sql).fetchall()
tables.append(armory_weapons)

sql = """
SELECT *
FROM charactercreator_character
"""
charactercreator_characters = curs.execute(sql).fetchall()
tables.append(charactercreator_characters)

sql = """
SELECT *
FROM charactercreator_character_inventory
"""
charactercreator_character_inventorys = curs.execute(sql).fetchall()
tables.append(charactercreator_character_inventorys)

sql = """
SELECT *
FROM charactercreator_cleric
"""
charactercreator_clerics = curs.execute(sql).fetchall()
tables.append(charactercreator_clerics)

sql = """
SELECT *
FROM charactercreator_fighter
"""
charactercreator_fighters = curs.execute(sql).fetchall()
tables.append(charactercreator_fighters)

sql = """
SELECT *
FROM charactercreator_mage
"""
charactercreator_mages = curs.execute(sql).fetchall()
tables.append(charactercreator_mages)

sql = """
SELECT *
FROM charactercreator_necromancer
"""
charactercreator_necromancers = curs.execute(sql).fetchall()
tables.append(charactercreator_necromancers)

sql = """
SELECT *
FROM charactercreator_thief
"""
charactercreator_thiefs = curs.execute(sql).fetchall()
tables.append(charactercreator_thiefs)


"""
  Using postgreSGL to insert the retrieved data
"""
# Loading environment variables
load_dotenv() 

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
cursor = connection.cursor()

# Create Tables
query = """
CREATE TABLE IF NOT EXISTS armory_item (
  item_id SERIAL PRIMARY KEY,
  name varchar(40) NOT NULL,
  value Integer NOT NULL,
  weight Integer NOT NULL
);
CREATE TABLE IF NOT EXISTS armory_weapon (
  item_ptr_id SERIAL PRIMARY KEY,
  power Integer NOT NULL
);
CREATE TABLE IF NOT EXISTS charactercreator_character (
  character_id SERIAL PRIMARY KEY,
  name varchar(40) NOT NULL,
  level Integer NOT NULL,
  exp Integer NOT NULL,
  hp Integer NOT NULL,
  strength Integer NOT NULL,
  intelligence Integer NOT NULL,
  dexterity Integer NOT NULL,
  wisdom Integer NOT NULL
);
CREATE TABLE IF NOT EXISTS charactercreator_character_inventory (
  id SERIAL PRIMARY KEY,
  character_id Integer,
  item_id Integer
);
CREATE TABLE IF NOT EXISTS charactercreator_cleric (
  character_ptr_id SERIAL PRIMARY KEY,
  using_shield Integer NOT NULL,
  mana Integer NOT NULL
);
CREATE TABLE IF NOT EXISTS charactercreator_fighter (
  character_ptr_id SERIAL PRIMARY KEY,
  using_shield Integer NOT NULL,
  rage Integer NOT NULL
);
CREATE TABLE IF NOT EXISTS charactercreator_mage (
  character_ptr_id SERIAL PRIMARY KEY,
  has_pet Integer NOT NULL,
  mana Integer NOT NULL
);
CREATE TABLE IF NOT EXISTS charactercreator_necromancer (
  mage_ptr_id SERIAL PRIMARY KEY,
  talisman_charged Integer NOT NULL
);
CREATE TABLE IF NOT EXISTS charactercreator_thief (
  character_ptr_id SERIAL PRIMARY KEY,
  is_sneaking Integer NOT NULL,
  energy Integer NOT NULL
);
"""
cursor.execute(query)



"""This can only be run once"""
insertion_query = "INSERT INTO armory_item (item_id, name, value, weight) VALUES %s"
execute_values(cursor, insertion_query, tables[0])
cursor.execute("SELECT * from armory_item;")
result = cursor.fetchall()
connection.commit()

insertion_query = "INSERT INTO armory_weapon (item_ptr_id, power) VALUES %s"
execute_values(cursor, insertion_query, tables[1])
cursor.execute("SELECT * from armory_weapon;")
result = cursor.fetchall()
connection.commit()

insertion_query = "INSERT INTO charactercreator_character (character_id, name, level, exp, hp, strength, intelligence, dexterity, wisdom) VALUES %s"
execute_values(cursor, insertion_query, tables[2])
cursor.execute("SELECT * from charactercreator_character;")
result = cursor.fetchall()
connection.commit()

insertion_query = "INSERT INTO charactercreator_character_inventory (id, character_id, item_id) VALUES %s"
execute_values(cursor, insertion_query, tables[3])
cursor.execute("SELECT * from charactercreator_character_inventory;")
result = cursor.fetchall()
connection.commit()

insertion_query = "INSERT INTO charactercreator_cleric (character_ptr_id, using_shield, mana) VALUES %s"
execute_values(cursor, insertion_query, tables[4])
cursor.execute("SELECT * from charactercreator_cleric;")
result = cursor.fetchall()
connection.commit()

insertion_query = "INSERT INTO charactercreator_fighter (character_ptr_id, using_shield, rage) VALUES %s"
execute_values(cursor, insertion_query, tables[5])
cursor.execute("SELECT * from charactercreator_fighter;")
result = cursor.fetchall()
connection.commit()

insertion_query = "INSERT INTO charactercreator_mage (character_ptr_id, has_pet, mana) VALUES %s"
execute_values(cursor, insertion_query, tables[6])
cursor.execute("SELECT * from charactercreator_mage;")
result = cursor.fetchall()
connection.commit()

insertion_query = "INSERT INTO charactercreator_necromancer (mage_ptr_id, talisman_charged) VALUES %s"
execute_values(cursor, insertion_query, tables[7])
cursor.execute("SELECT * from charactercreator_necromancer;")
result = cursor.fetchall()
connection.commit()

insertion_query = "INSERT INTO charactercreator_thief (character_ptr_id, is_sneaking, energy) VALUES %s"
execute_values(cursor, insertion_query, tables[8])
cursor.execute("SELECT * from charactercreator_thief;")
result = cursor.fetchall()
connection.commit()

connection.close()