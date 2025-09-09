import sqlite3

conn = sqlite3.connect("call_data.db")
cursor = conn.cursor()

# List all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

# Read from a table (replace 'calls' with your table name)
cursor.execute("SELECT * FROM calls LIMIT 10;")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
