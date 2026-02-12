import sqlite3

con = sqlite3.connect("database/trading.db")
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS sale_invoice_head(
    inv_id INTEGER PRIMARY KEY AUTOINCREMENT,
    la_id INTEGER,
    customer TEXT,
    inv_date TEXT,
    total REAL
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS sale_invoice_body(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inv_id INTEGER,
    item_name TEXT,
    section TEXT,
    qty REAL,
    rate REAL,
    amount REAL
)
""")

con.commit()
con.close()

print("Sale Invoice tables created successfully")
