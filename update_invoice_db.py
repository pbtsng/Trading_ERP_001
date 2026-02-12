import sqlite3

con = sqlite3.connect("database/trading.db")
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS sale_invoice(
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
 base_rate REAL,
 section_rate REAL,
 gst REAL,
 final_rate REAL,
 amount REAL
)
""")

con.commit()
con.close()
print("Invoice tables ready")
