import sqlite3

con = sqlite3.connect('database.db')
cur = con.cursor()
cur.execute("SELECT * FROM sqlite_master")
print(cur.fetchall())
con.close()
