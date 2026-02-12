import sqlite3
from datetime import date

DB = "database/trading.db"

def ensure_columns(cur):
    try:
        cur.execute("ALTER TABLE sale_orders ADD COLUMN status TEXT DEFAULT 'OPEN'")
    except:
        pass
    try:
        cur.execute("ALTER TABLE purchase_orders ADD COLUMN status TEXT DEFAULT 'OPEN'")
    except:
        pass

def upsert_accounts(cur):
    customers = [f"Customer {c}" for c in ["A","B","C","D","E"]]
    suppliers = [f"Supplier {s}" for s in ["A","B","C","D","E"]]

    for name in customers:
        cur.execute("SELECT acc_id FROM acc_mast WHERE acc_name=?", (name,))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO acc_mast(acc_name,acc_type,mobile,city,state)
                VALUES (?,?,?,?,?)
            """, (name, "Customer", "9999999999", "CityX", "StateY"))

    for name in suppliers:
        cur.execute("SELECT acc_id FROM acc_mast WHERE acc_name=?", (name,))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO acc_mast(acc_name,acc_type,mobile,city,state)
                VALUES (?,?,?,?,?)
            """, (name, "Supplier", "8888888888", "CityZ", "StateW"))

def upsert_items(cur):
    items = [f"Item {i}" for i in ["A","B","C","D","E"]]
    for name in items:
        cur.execute("SELECT item_id FROM item_mast WHERE item_name=?", (name,))
        if not cur.fetchone():
            cur.execute("INSERT INTO item_mast(item_name) VALUES (?)", (name,))

def insert_orders(cur):
    today = date.today().isoformat()
    # Get 5 customers, suppliers, items
    custs = cur.execute("SELECT acc_id FROM acc_mast WHERE acc_type='Customer' ORDER BY acc_id LIMIT 5").fetchall()
    sups  = cur.execute("SELECT acc_id FROM acc_mast WHERE acc_type='Supplier' ORDER BY acc_id LIMIT 5").fetchall()
    items = cur.execute("SELECT item_id FROM item_mast ORDER BY item_id LIMIT 5").fetchall()

    # SALE ORDERS
    for i in range(5):
        acc_id = custs[i][0]
        item_id = items[i][0]
        qty = 10 + i
        rate = 100 + i * 5
        amount = qty * rate
        cur.execute("""
            INSERT INTO sale_orders(so_date,acc_id,item_id,qty,rate,amount,user_code,status)
            VALUES (?,?,?,?,?,?,?,?)
        """, (today, acc_id, item_id, qty, rate, amount, "U001", "OPEN"))

    # PURCHASE ORDERS
    for i in range(5):
        acc_id = sups[i][0]
        item_id = items[i][0]
        qty = 12 + i
        rate = 95 + i * 4
        amount = qty * rate
        cur.execute("""
            INSERT INTO purchase_orders(po_date,acc_id,item_id,qty,rate,amount,user_code,status)
            VALUES (?,?,?,?,?,?,?,?)
        """, (today, acc_id, item_id, qty, rate, amount, "U001", "OPEN"))

def insert_loading_and_invoice(cur):
    today = date.today().isoformat()
    # Create Loading Advice head
    cur.execute("""
        INSERT INTO loading_advice_head(la_date,vehicle_no,user_code)
        VALUES (?,?,?)
    """, (today, "TEST-001", "U001"))
    la_id = cur.lastrowid

    # Get 5 latest sale/purchase orders
    sales = cur.execute("SELECT so_id,item_id,rate,qty,supplied_qty FROM sale_orders ORDER BY so_id DESC LIMIT 5").fetchall()
    purchases = cur.execute("SELECT po_id,item_id,rate,qty,supplied_qty FROM purchase_orders ORDER BY po_id DESC LIMIT 5").fetchall()

    for i in range(5):
        so_id, s_item_id, s_rate, s_qty, s_sup = sales[i]
        po_id, p_item_id, p_rate, p_qty, p_sup = purchases[i]
        qty = 2 + i
        # body insert
        cur.execute("""
            INSERT INTO loading_advice_body(la_id,so_id,po_id,section,part,qty)
            VALUES (?,?,?,?,?,?)
        """, (la_id, so_id, po_id, f"S{i+1}", f"BATCH{i+1}", qty))
        # update balances
        cur.execute("UPDATE sale_orders SET supplied_qty = COALESCE(supplied_qty,0) + ? WHERE so_id=?", (qty, so_id))
        cur.execute("UPDATE purchase_orders SET supplied_qty = COALESCE(supplied_qty,0) + ? WHERE po_id=?", (qty, po_id))
        # lock orders
        try:
            cur.execute("UPDATE sale_orders SET status='LOCKED' WHERE so_id=?", (so_id,))
            cur.execute("UPDATE purchase_orders SET status='LOCKED' WHERE po_id=?", (po_id,))
        except:
            pass

    # Build invoice
    # Determine customer name
    cust = cur.execute("""
        SELECT a.acc_name
        FROM loading_advice_body b
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE b.la_id=?
        LIMIT 1
    """, (la_id,)).fetchone()[0]

    # Create invoice head
    # Compute total as sum of qty*s_rate
    rows = cur.execute("""
        SELECT i.item_name, b.section, b.qty, s.rate
        FROM loading_advice_body b
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE b.la_id=?
    """, (la_id,)).fetchall()

    total = sum([r[2]*r[3] for r in rows])

    cur.execute("""
        INSERT INTO sale_invoice_head(la_id,inv_date,customer,total)
        VALUES (?,?,?,?)
    """, (la_id, today, cust, total))
    inv_id = cur.lastrowid

    for item_name, section, qty, rate in rows:
        amount = qty * rate
        cur.execute("""
            INSERT INTO sale_invoice_body(inv_id,item_name,section,qty,rate,amount)
            VALUES (?,?,?,?,?,?)
        """, (inv_id, item_name, section, qty, rate, amount))

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    ensure_columns(cur)
    upsert_accounts(cur)
    upsert_items(cur)
    insert_orders(cur)
    insert_loading_and_invoice(cur)
    con.commit()
    con.close()
    print("Seeded 5 entries across accounts, items, orders, loading advice, and invoice.")

if __name__ == "__main__":
    main()
