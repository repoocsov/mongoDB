# "How was working with MongoDB different from working with PostgreSQL? What was easier, and what was harder?"
"""

"""

import pymongo
import sqlite3
import json
import os
from dotenv import load_dotenv, find_dotenv
import psycopg2
from psycopg2.extras import execute_values

"""LOAD RPG DATA"""
DB_FILEPATH = os.path.join(os.path.dirname(__file__), 'rpg_db.sqlite3')

# Connect to DB
conn = sqlite3.connect(DB_FILEPATH)
curs = conn.cursor()

sql = """
SELECT *
FROM armory_item
LIMIT 20;
"""
armory_items = curs.execute(sql).fetchall()

# sql = """
# Pragma table_info(armory_item)
# """
# keys = curs.execute(sql).fetchall()
keys = ('item_id', 'name', 'value', 'weight')
print(keys)

def get_list_of_dict(keys, list_of_tuples):
     """
     This function will accept keys and list_of_tuples as args and return list of dicts
     """
     list_of_dict = [dict(zip(keys, values)) for values in list_of_tuples]
     return list_of_dict

list_of_dicts = get_list_of_dict(keys, armory_items)
print(list_of_dicts)



"""STORE DATA IN MONGODB"""
load_dotenv()

MONGO_USER = os.getenv("MONGO_USER", default="OOPS")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER", default="OOPS")
#connection_uri = f"mongodb+srv://dbUser:dbUserPassword@cluster0-lsebk.mongodb.net/test?retryWrites=true&w=majority"
connection_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"

client = pymongo.MongoClient(connection_uri)
db = client.test_database_2 
collection = db.rpg_collection 

collection.insert_many(list_of_dicts)
print("DOCS:", collection.count_documents({}))
print("ANY NAMES:", collection.count_documents({"name": {"$gt": 0}}))