import sqlite3

con = sqlite3.connect("database/trading.db")
cur = con.cursor()

columns = ["mobile","pan","gstin","address","city","state"]

for col in columns:
    try:
        cur.execute(f"ALTER TABLE acc_mast ADD COLUMN {col} TEXT")
        print(f"Added column: {col}")
    except:
        print(f"Column already exists: {col}")

con.commit()
con.close()
print("Update complete")
