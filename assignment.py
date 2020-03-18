# monday/assignment.py
import sqlite3
import os

"""N0TE: FOR WHATEVER REASON THE 2ND STATEMENT CAUSES THIS TO RUN SLOWLY. WAIT JUST A FEW MINUTES."""

# DB_FILEPATH = 'rpg_db.sqlite3'
DB_FILEPATH = os.path.join(os.path.dirname(__file__), 'rpg_db.sqlite3')

# Connect to DB
conn = sqlite3.connect(DB_FILEPATH)
curs = conn.cursor()


# How many total Characters are there?
 


# REVISIT THIS. THESE ARE NOT IN ORDER AND DON'T INCLUDE THE NAMES OF THE CLASSES.
# How many of each specific subclass?
sql = """
SELECT
	COUNT(DISTINCT charactercreator_cleric.character_ptr_id) AS Clerics,
	COUNT(DISTINCT charactercreator_fighter.character_ptr_id) AS Fighters,
	COUNT(DISTINCT charactercreator_mage.character_ptr_id) AS Mages,
	COUNT(DISTINCT charactercreator_necromancer.mage_ptr_id) AS Necromancers,
	COUNT(DISTINCT charactercreator_thief.character_ptr_id) AS Thiefs
FROM charactercreator_cleric, charactercreator_fighter, charactercreator_mage, charactercreator_necromancer, charactercreator_thief
"""
result = curs.execute(sql).fetchall()
print(result)


# How many total Items?
sql = """
SELECT
    COUNT(DISTINCT item_id) AS UniqueItems
FROM armory_item
"""
result = curs.execute(sql).fetchall()
print(result)


# How many of the Items are weapons? How many are not?
sql = """
SELECT
    COUNT(DISTINCT item_ptr_id) AS Weapons,
    (COUNT(DISTINCT item_id) - COUNT(DISTINCT item_ptr_id)) AS NonWeapons
FROM armory_item
LEFT JOIN armory_weapon ON armory_item.item_id = armory_weapon.item_ptr_id
"""
result = curs.execute(sql).fetchall()
print(result)


# How many Items does each character have? (Return first 20 rows)
sql = """
SELECT
	character_id,
   COUNT(*) AS NumberOfItems
FROM charactercreator_character_inventory
GROUP BY character_id
ORDER BY character_id
LIMIT 20
"""
result = curs.execute(sql).fetchall()
print(result)


# How many Weapons does each character have? (Return first 20 rows)
sql = """
SELECT
	character_id,
	COUNT(item_ptr_id) AS NumberOfWeapons
FROM charactercreator_character_inventory
LEFT JOIN armory_weapon ON charactercreator_character_inventory.item_id = armory_weapon.item_ptr_id
GROUP BY character_id
ORDER BY item_ptr_id
LIMIT 20
"""
result = curs.execute(sql).fetchall()
print(result)


# On average, how many Items does each Character have?
sql = """
SELECT AVG(NumberOfItems) FROM
	(SELECT
		COUNT() AS NumberOfItems
		FROM charactercreator_character_inventory
		GROUP BY character_id
		ORDER BY character_id)
"""
result = curs.execute(sql).fetchall()
print(result)


# On average, how many Weapons does each character have?
sql = """
SELECT AVG(NumberOfWeapons) FROM
	(SELECT
		COUNT(item_ptr_id) AS NumberOfWeapons
		FROM charactercreator_character_inventory
		LEFT JOIN armory_weapon ON charactercreator_character_inventory.item_id = armory_weapon.item_ptr_id
		GROUP BY character_id
		ORDER BY item_ptr_id)
"""
result = curs.execute(sql).fetchall()
print(result)
