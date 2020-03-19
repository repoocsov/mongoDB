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


"""---------------------------------------------
  Using postgreSGL to insert the retrieved data
---------------------------------------------"""
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

# Insert data
insertion_query = "INSERT INTO titanic (Survived, Pclass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Aboard, Fare) VALUES %s"
execute_values(cursor, insertion_query, titanic_data)
cursor.execute("SELECT * from titanic;")
result = cursor.fetchall()
connection.commit()


"""---------------------------------------------
             BUSINESS QUESTIONS
---------------------------------------------"""
# How many passengers survived, and how many died?
query = """
SELECT
	survived,
	COUNT('survived') AS "Count"
FROM titanic
GROUP BY "survived";
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nSurival:", result)

# How many passengers were in each class?
query = """
SELECT
	pclass,
	COUNT('pclass') AS "Count"
FROM titanic
GROUP BY "pclass";
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nPassengers per class:", result)

# How many passengers survived/died within each class?
query = """
SELECT
	pclass,
	COUNT(CASE WHEN survived=True THEN 1 END) AS Survived,
	COUNT(CASE WHEN survived=False THEN 1 END) AS Deceased
FROM titanic
GROUP BY "pclass";
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nPassengers survival per class:", result)

# What was the average age of survivors vs nonsurvivors?
query = """
SELECT
	survived,
	AVG(age)
FROM titanic
GROUP BY survived
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nAverage age of survived/deceased:", result)

# What was the average age of each passenger class?
query = """
SELECT
	pclass,
	AVG(age)
FROM titanic
GROUP BY "pclass";
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nAverage age per class:", result)

# What was the average fare by passenger class? By survival?
query = """
SELECT
	pclass,
	survived,
	avg(fare)
FROM titanic
GROUP BY pclass, survived
ORDER BY pclass,  survived
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nAverage fare per class and survival:", result)

# How many siblings/spouses aboard on average, by passenger class? By survival?
query = """
SELECT
	pclass,
	survived,
	avg(siblings_spouses_aboard)
FROM titanic
GROUP BY pclass, survived
ORDER BY pclass,  survived
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nAverage siblings/spouses by class and survival:", result)

# How many parents/children aboard on average, by passenger class? By survival?
query = """
SELECT
	pclass,
	survived,
	avg(parents_children_aboard)
FROM titanic
GROUP BY pclass, survived
ORDER BY pclass,  survived
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nAverage parents/children by class and survival:", result)

# Do any passengers have the same name?
query = """
SELECT
	COUNT(DISTINCT name) - COUNT(*) AS "How many distinct"
FROM titanic
"""
cursor.execute(query)
result = cursor.fetchall()
print("\nMatching names:", result)

# Closing the connection
connection.close()

"""
(Bonus! Hard, may require pulling and processing with Python) How many married couples were aboard the Titanic? 
Assume that two people (one Mr. and one Mrs.) with the same last name and with at least 1 sibling/spouse aboard are a married couple.
"""