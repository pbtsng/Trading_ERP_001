import sqlite3

con = sqlite3.connect("database/trading.db")
cur = con.cursor()

cols = ["item_name","section","part"]

for c in cols:
    try:
        cur.execute(f"ALTER TABLE loading_advice_body ADD COLUMN {c} TEXT")
        print(f"Added {c}")
    except:
        print(f"{c} already exists")

con.commit()
con.close()
print("Loading Advice Body updated")
