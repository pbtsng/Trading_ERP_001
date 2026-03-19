import sqlite3

con = sqlite3.connect("database/trading.db")
cur = con.cursor()

# Ensure acc_mast columns exist
columns = ["mobile","pan","gstin","address","city","state"]

for col in columns:
    try:
        cur.execute(f"ALTER TABLE acc_mast ADD COLUMN {col} TEXT")
        print(f"Added column: {col}")
    except:
        print(f"Column already exists: {col}")

for stmt in [
    "ALTER TABLE users_mast ADD COLUMN mfa_required INTEGER DEFAULT 0",
    "ALTER TABLE users_mast ADD COLUMN default_theme TEXT",
    "ALTER TABLE users_mast ADD COLUMN default_density TEXT",
]:
    try:
        cur.execute(stmt)
        print("Applied:", stmt)
    except:
        print("Skipped:", stmt)

cur.execute("""
CREATE TABLE IF NOT EXISTS acc_groups(
  group_id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_name TEXT UNIQUE,
  acc_type TEXT
)
""")
print("Ensured acc_groups table exists")

cur.execute("""
INSERT OR IGNORE INTO acc_groups(group_name,acc_type) VALUES 
 ('Current Assets','ASSET'), 
 ('Fixed Assets','ASSET'), 
 ('Current Liabilities','LIABILITY'), 
 ('Direct Income','INCOME'), 
 ('Indirect Income','INCOME'), 
 ('Direct Expense','EXPENSE'), 
 ('Indirect Expense','EXPENSE'), 
 ('Capital','EQUITY')
""")
print("Seeded default acc_groups (INSERT OR IGNORE)")

# Accounting vouchers tables
cur.execute("""
CREATE TABLE IF NOT EXISTS vouchers_head(
  v_id INTEGER PRIMARY KEY AUTOINCREMENT,
  v_date TEXT,
  v_type TEXT,
  narration TEXT,
  user_code TEXT
)
""")
print("Ensured vouchers_head exists")

cur.execute("""
CREATE TABLE IF NOT EXISTS vouchers_lines(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  v_id INTEGER,
  ledger_id INTEGER,
  acc_id INTEGER,
  dr REAL DEFAULT 0,
  cr REAL DEFAULT 0
)
""")
print("Ensured vouchers_lines exists")

cur.execute("""
CREATE TABLE IF NOT EXISTS credit_notes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  so_id INTEGER,
  cn_date TEXT,
  customer TEXT,
  amount REAL,
  reason TEXT
)
""")
print("Ensured credit_notes exists")

cur.execute("""
CREATE TABLE IF NOT EXISTS debit_notes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_id INTEGER,
  dn_date TEXT,
  supplier TEXT,
  amount REAL,
  reason TEXT
)
""")
print("Ensured debit_notes exists")

cur.execute("""
CREATE TABLE IF NOT EXISTS item_ledger(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  la_id INTEGER,
  la_date TEXT,
  item_id INTEGER,
  so_id INTEGER,
  po_id INTEGER,
  customer_id INTEGER,
  supplier_id INTEGER,
  qty REAL
)
""")
print("Ensured item_ledger exists")

cur.execute("""
CREATE TABLE IF NOT EXISTS audit_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  log_time TEXT,
  user_code TEXT,
  table_name TEXT,
  action TEXT,
  pk TEXT,
  details TEXT
)
""")
print("Ensured audit_log exists")

# WhatsApp / notification tracking
cur.execute("""
CREATE TABLE IF NOT EXISTS notifications(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT,
  channel TEXT,
  direction TEXT,
  kind TEXT,
  ref_id INTEGER,
  party_name TEXT,
  mobile TEXT,
  message TEXT,
  status TEXT,
  reply_text TEXT,
  reply_time TEXT,
  user_code TEXT
)
""")
print("Ensured notifications table exists")

# WhatsApp sent flags and approval fields
for stmt in [
    "ALTER TABLE sale_orders ADD COLUMN whatsapp_sent INTEGER DEFAULT 0",
    "ALTER TABLE purchase_orders ADD COLUMN whatsapp_sent INTEGER DEFAULT 0",
    "ALTER TABLE loading_advice_head ADD COLUMN whatsapp_sent INTEGER DEFAULT 0",
    "ALTER TABLE loading_advice_head ADD COLUMN approval_status TEXT",
    "ALTER TABLE loading_advice_head ADD COLUMN approval_note TEXT",
]:
    try:
        cur.execute(stmt)
        print("Applied:", stmt)
    except:
        print("Skipped:", stmt)

cur.execute("""
CREATE TABLE IF NOT EXISTS sale_schemes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  start_date TEXT,
  end_date TEXT,
  min_qty REAL,
  reward_text TEXT
)
""")
print("Ensured sale_schemes exists")

cur.execute("""
CREATE TABLE IF NOT EXISTS daily_rates(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rate_date TEXT,
  item_id INTEGER,
  sale_rate REAL,
  purchase_rate REAL
)
""")
print("Ensured daily_rates exists")
cur.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_rates_date_item
ON daily_rates(rate_date,item_id)
""")
print("Ensured idx_daily_rates_date_item exists")

cur.execute("""
CREATE TABLE IF NOT EXISTS grn_head(
  grn_id INTEGER PRIMARY KEY AUTOINCREMENT,
  la_id INTEGER,
  grn_date TEXT,
  supplier TEXT,
  total REAL
)
""")
print("Ensured grn_head exists")

cur.execute("""
CREATE TABLE IF NOT EXISTS grn_body(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  grn_id INTEGER,
  item_name TEXT,
  section TEXT,
  qty REAL,
  rate REAL,
  amount REAL
)
""")
print("Ensured grn_body exists")

# Create ledgers table if not exists
cur.execute("""
CREATE TABLE IF NOT EXISTS ledgers(
  ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ledger_name TEXT UNIQUE,
  group_id INTEGER
)
""")
print("Ensured ledgers table exists")

# Seed default ledgers
cur.execute("""
INSERT OR IGNORE INTO ledgers(ledger_name,group_id) VALUES 
 ('Cash',1), 
 ('Bank',1), 
 ('Sundry Debtors',1), 
 ('Sundry Creditors',3), 
 ('Sales',4), 
 ('Purchase',6), 
 ('Output CGST',3), 
 ('Output SGST',3), 
 ('Output IGST',3), 
 ('Input CGST',1), 
 ('Input SGST',1), 
 ('Input IGST',1), 
 ('Capital',8)
""")
print("Seeded default ledgers (INSERT OR IGNORE)")

con.commit()
con.close()
print("Update complete")
