import pandas as pd
import sqlite3

# Read in the CSV
df = pd.read_csv('https://archive.ics.uci.edu/ml/machine-learning-databases/00476/buddymove_holidayiq.csv')

# Check the Shape and Nulls
shape = df.shape
print(shape)
nulls = df.isnull().sum().sum()
print(nulls)

# Open a connection to a new (blank) database file buddymove_holidayiq.sqlite3
conn = sqlite3.connect('buddymove_holidayiq.sqlite3')
curs = conn.cursor()

# Use df.to_sql (documentation) to insert the data into a new table review in the SQLite3 database
df.to_sql('review', conn)

# Count how many rows you have - it should be 249!
sql = """
SELECT
	COUNT(*)
FROM review
"""
result = curs.execute(sql).fetchall()
print(result)

# How many users who reviewed at least 100 Nature in the category also reviewed at least 100 in the Shopping category?
sql = """
SELECT
	COUNT(*) AS UsersWith100NatureAnd100Shopping
FROM review
WHERE Nature >= 100 AND Shopping >= 100
"""
result = curs.execute(sql).fetchall()
print(result)
