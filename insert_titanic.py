import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

"""
  set up a new table for the Titanic data (titanic.csv)
"""

# Importing the csv file
df = pd.read_csv('https://raw.githubusercontent.com/LambdaSchool/DS-Unit-3-Sprint-2-SQL-and-Databases/master/module2-sql-for-analysis/titanic.csv')
df['Survived'] = df['Survived'].replace({0: False, 1: True})
titanic_data = df.values.tolist()


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

# Create armory_item table
query = """
CREATE TYPE sexes AS ENUM ('male', 'female');

CREATE TABLE IF NOT EXISTS titanic (
  UniqueID SERIAL PRIMARY KEY,
  Survived Boolean NOT NULL,
  Pclass Integer NOT NULL,
  Name varchar(100) NOT NULL,
  Sex sexes NOT NULL,
  Age Integer,
  Siblings_Spouses_Aboard Integer,
  Parents_Children_Aboard Integer,
  Fare NUMERIC
);
"""
cursor.execute(query)

"""This can only be run once"""
# Insert data
insertion_query = "INSERT INTO titanic (Survived, Pclass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Aboard, Fare) VALUES %s"
execute_values(cursor, insertion_query, titanic_data)
cursor.execute("SELECT * from titanic;")
result = cursor.fetchall()
connection.commit()

connection.close()