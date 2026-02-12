import sqlite3

con = sqlite3.connect("database/trading.db")
cur = con.cursor()

# USERS
cur.execute("""
CREATE TABLE IF NOT EXISTS users_mast(
 user_code TEXT PRIMARY KEY,
 username TEXT,
 password_hash TEXT,
 role TEXT
)
""")

# ACCOUNT MASTER
cur.execute("""
CREATE TABLE IF NOT EXISTS acc_mast(
 acc_id INTEGER PRIMARY KEY AUTOINCREMENT,
 acc_name TEXT,
 acc_type TEXT
)
""")

# ITEM MASTER
cur.execute("""
CREATE TABLE IF NOT EXISTS item_mast(
 item_id INTEGER PRIMARY KEY AUTOINCREMENT,
 item_name TEXT
)
""")

# SALE ORDER
cur.execute("""
CREATE TABLE IF NOT EXISTS sale_orders(
 so_id INTEGER PRIMARY KEY AUTOINCREMENT,
 so_date TEXT,
 acc_id INTEGER,
 item_id INTEGER,
 child_item TEXT,
 qty REAL,
 rate REAL,
 supplied_qty REAL DEFAULT 0,
 amount REAL,
 user_code TEXT
)
""")

# PURCHASE ORDER
cur.execute("""
CREATE TABLE IF NOT EXISTS purchase_orders(
 po_id INTEGER PRIMARY KEY AUTOINCREMENT,
 po_date TEXT,
 acc_id INTEGER,
 item_id INTEGER,
 child_item TEXT,
 qty REAL,
 rate REAL,
 supplied_qty REAL DEFAULT 0,
 amount REAL,
 user_code TEXT
)
""")

# LOADING ADVICE HEAD
cur.execute("""
CREATE TABLE IF NOT EXISTS loading_advice_head(
 la_id INTEGER PRIMARY KEY AUTOINCREMENT,
 la_date TEXT,
 vehicle_no TEXT,
 supplier_id INTEGER,
 customer_id INTEGER,
 user_code TEXT
)
""")

# LOADING ADVICE BODY
cur.execute("""
CREATE TABLE IF NOT EXISTS loading_advice_body(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 la_id INTEGER,
 so_id INTEGER,
 po_id INTEGER,
 item_id INTEGER,
 qty REAL,
 purchase_rate REAL,
 sale_rate REAL,
 rate_diff REAL,
 amount REAL
)
""")

con.commit()
con.close()
print("Database Ready")
