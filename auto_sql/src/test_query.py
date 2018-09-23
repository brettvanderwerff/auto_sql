import sqlite3
con = sqlite3.connect('database.db')
cur = con.cursor()
cur.execute("SELECT * FROM sqlite_master")
print(cur.fetchall())
#cur.execute("SELECT COUNT(record_id) FROM surveys") #4329312
#print(cur.fetchall())
con.close()