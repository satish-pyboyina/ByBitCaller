import sqlite3

# Connect to the database
conn = sqlite3.connect('ohlc.db')

# Create a cursor
cursor = conn.cursor()

# Specify the table name for which you want to retrieve column names
table_name = 'ETHUSD'

# Execute the query to fetch the table's schema
cursor.execute(f"PRAGMA table_info({table_name})")

# Fetch all rows from the result set
columns_info = cursor.fetchall()

# Extract column names from the result set
column_names = [column_info[1] for column_info in columns_info]
print(column_names)

# # Execute a SELECT query
# cursor.execute(f"SELECT * FROM {table_name} limit 10")

# # Fetch one row of data
# row = cursor.fetchone()
# print(row)

# fetch all rows of data
# all_rows = cursor.fetchall()
# print(cursor.fetchall())

# # Execute a SELECT query
cursor.execute(f"SELECT min(startTime), max(startTime), count(*) FROM {table_name} ")
print(cursor.fetchall())

# Close the cursor and connection
cursor.close()
conn.close()