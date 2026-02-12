from flask import Flask, render_template, request, redirect, session, jsonify, send_file
import sqlite3
from werkzeug.security import check_password_hash
import pandas as pd
from datetime import date

app = Flask(__name__)
app.secret_key = "secret123"
DB = "database/trading.db"

# ---------------- DATE HELPER ----------------
def today():
    return date.today().isoformat()

# ---------------- DB ----------------
def db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

# ---------------- LOGIN ----------------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        con = db()
        u = con.execute(
            "SELECT * FROM users_mast WHERE username=?",
            (request.form["username"],)
        ).fetchone()

        if u and check_password_hash(u["password_hash"], request.form["password"]):
            session["user"] = u["user_code"]
            session["role"] = u["role"]
            return redirect("/dashboard")

    return render_template("login.html")

@app.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect("/")
    return render_template("dashboard.html")

# ---------------- SALE ORDER ----------------
@app.route("/sale", methods=["GET", "POST"])
def sale():
    if "user" not in session:
        return redirect("/")

    con = db()

    if request.method == "POST":
        qty = float(request.form["qty"])
        rate = float(request.form["rate"])
        amt = qty * rate

        con.execute("""
            INSERT INTO sale_orders
            (so_date, acc_id, item_id, qty, rate, amount, user_code)
            VALUES (?,?,?,?,?,?,?)
        """, (
            request.form["date"],
            request.form["acc_id"],
            request.form["item_id"],
            qty, rate, amt, session["user"]
        ))
        con.commit()

    customers = con.execute("""
        SELECT acc_id, acc_name, mobile, city, state
        FROM acc_mast
        WHERE acc_type='Customer'
        ORDER BY acc_name
    """).fetchall()

    items = con.execute("SELECT * FROM item_mast ORDER BY item_name").fetchall()

    rows = con.execute("""
        SELECT so_id, so_date, acc_name, item_name, qty, rate, amount
        FROM sale_orders
        JOIN acc_mast ON sale_orders.acc_id = acc_mast.acc_id
        JOIN item_mast ON sale_orders.item_id = item_mast.item_id
        ORDER BY so_id DESC
    """).fetchall()

    return render_template(
        "sale_order.html",
        customers=customers,
        items=items,
        rows=rows,
        today=today()
    )

# ---------------- PURCHASE ORDER ----------------
@app.route("/purchase", methods=["GET", "POST"])
def purchase():
    if "user" not in session:
        return redirect("/")

    con = db()

    if request.method == "POST":
        qty = float(request.form["qty"])
        rate = float(request.form["rate"])
        amt = qty * rate

        con.execute("""
            INSERT INTO purchase_orders
            (po_date, acc_id, item_id, qty, rate, amount, user_code)
            VALUES (?,?,?,?,?,?,?)
        """, (
            request.form["date"],
            request.form["acc_id"],
            request.form["item_id"],
            qty, rate, amt, session["user"]
        ))
        con.commit()

    suppliers = con.execute("""
        SELECT acc_id, acc_name, mobile, city, state
        FROM acc_mast
        WHERE acc_type='Supplier'
        ORDER BY acc_name
    """).fetchall()

    items = con.execute("SELECT * FROM item_mast ORDER BY item_name").fetchall()

    rows = con.execute("""
        SELECT po_id, po_date, acc_name, item_name, qty, rate, amount
        FROM purchase_orders
        JOIN acc_mast ON purchase_orders.acc_id = acc_mast.acc_id
        JOIN item_mast ON purchase_orders.item_id = item_mast.item_id
        ORDER BY po_id DESC
    """).fetchall()

    return render_template(
        "purchase_order.html",
        suppliers=suppliers,
        items=items,
        rows=rows,
        today=today()
    )

# ---------------- ADD ACCOUNT ----------------
@app.route("/add_account", methods=["POST"])
def add_account():
    if "user" not in session:
        return redirect("/")

    con = db()

    exists = con.execute(
        "SELECT acc_id FROM acc_mast WHERE acc_name=?",
        (request.form["name"],)
    ).fetchone()

    if not exists:
        con.execute("""
            INSERT INTO acc_mast
            (acc_name, acc_type, mobile, pan, gstin, address, city, state)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            request.form["name"],
            request.form["type"],
            request.form.get("mobile"),
            request.form.get("pan"),
            request.form.get("gstin"),
            request.form.get("address"),
            request.form.get("city"),
            request.form.get("state")
        ))
        con.commit()

    return redirect(request.referrer)

# ---------------- ADD ITEM ----------------
@app.route("/add_item", methods=["POST"])
def add_item():
    if "user" not in session:
        return redirect("/")

    con = db()

    exists = con.execute(
        "SELECT item_id FROM item_mast WHERE item_name=?",
        (request.form["name"],)
    ).fetchone()

    if not exists:
        con.execute(
            "INSERT INTO item_mast(item_name) VALUES (?)",
            (request.form["name"],)
        )
        con.commit()

    return redirect(request.referrer)

# ---------------- LOADING ADVICE ----------------
@app.route("/loading_advice", methods=["GET", "POST"])
def loading_advice():
    if "user" not in session:
        return redirect("/")
    con = db()
    if request.method == "POST":
        cur = con.execute("""
            INSERT INTO loading_advice_head
            (la_date, vehicle_no, user_code)
            VALUES (?,?,?)
        """, (
            request.form["date"],
            request.form["vehicle"],
            session["user"]
        ))
        la_id = cur.lastrowid
        so_ids = request.form.getlist("so_id")
        po_ids = request.form.getlist("po_id")
        sections = request.form.getlist("section")
        batchs = request.form.getlist("batch")
        qtys = request.form.getlist("qty")
        for i in range(len(qtys)):
            if not qtys[i]:
                continue
            qty = float(qtys[i])
            con.execute("""
                INSERT INTO loading_advice_body
                (la_id, so_id, po_id, section, part, qty)
                VALUES (?,?,?,?,?,?)
            """, (
                la_id, so_ids[i], po_ids[i],
                sections[i], batchs[i], qty
            ))
            con.execute(
                "UPDATE sale_orders SET supplied_qty = supplied_qty + ? WHERE so_id=?",
                (qty, so_ids[i])
            )
            con.execute(
                "UPDATE purchase_orders SET supplied_qty = supplied_qty + ? WHERE po_id=?",
                (qty, po_ids[i])
            )
        con.commit()

    sales = con.execute("""
        SELECT s.so_id, s.so_date, a.acc_name, i.item_name,
               (s.qty - s.supplied_qty) AS balance, s.rate
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id = a.acc_id
        JOIN item_mast i ON s.item_id = i.item_id
        WHERE (s.qty - s.supplied_qty) > 0
        ORDER BY s.so_date
    """).fetchall()
    purchases = con.execute("""
        SELECT p.po_id, p.po_date, a.acc_name, i.item_name,
               (p.qty - p.supplied_qty) AS balance, p.rate
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id = a.acc_id
        JOIN item_mast i ON p.item_id = i.item_id
        WHERE (p.qty - p.supplied_qty) > 0
        ORDER BY p.po_date
    """).fetchall()
    las = con.execute("""
        SELECT h.la_id, h.la_date, h.vehicle_no
        FROM loading_advice_head h
        LEFT JOIN sale_invoice_head si ON h.la_id = si.la_id
        WHERE si.inv_id IS NULL
        ORDER BY h.la_id DESC
    """).fetchall()
    la_rows = {}
    for la in las:
        la_rows[la["la_id"]] = con.execute("""
            SELECT section, qty, part
            FROM loading_advice_body
            WHERE la_id=?
        """, (la["la_id"],)).fetchall()
    return render_template(
        "loading_advice.html",
        sales=sales,
        purchases=purchases,
        las=las,
        la_rows=la_rows,
        today=today()
    )
# ---------------- SALE INVOICE ----------------
@app.route("/sale_invoice/<int:la_id>", methods=["GET","POST"])
def sale_invoice(la_id):

    if "user" not in session:
        return redirect("/")

    con = db()

    # Get customer name from sale order
    customer = con.execute("""
        SELECT a.acc_name
        FROM loading_advice_body b
        JOIN sale_orders s ON b.so_id = s.so_id
        JOIN acc_mast a ON s.acc_id = a.acc_id
        WHERE b.la_id = ?
        LIMIT 1
    """,(la_id,)).fetchone()

    rows = con.execute("""
        SELECT 
            i.item_name,
            b.section,
            b.qty
        FROM loading_advice_body b
        JOIN sale_orders s ON b.so_id = s.so_id
        JOIN item_mast i ON s.item_id = i.item_id
        WHERE b.la_id = ?
    """,(la_id,)).fetchall()

    if request.method=="POST":

        total = float(request.form["grand_total"])

        cur = con.execute("""
            INSERT INTO sale_invoice_head
            (la_id, inv_date, customer, total)
            VALUES (?,?,?,?)
        """,(la_id,
             request.form["date"],
             request.form["customer"],
             total))

        inv_id = cur.lastrowid

        items = request.form.getlist("item")
        sections = request.form.getlist("section")
        qtys = request.form.getlist("qty")
        rates = request.form.getlist("rate")
        amts = request.form.getlist("amount")

        for i in range(len(items)):
            con.execute("""
                INSERT INTO sale_invoice_body
                (inv_id,item_name,section,qty,rate,amount)
                VALUES (?,?,?,?,?,?)
            """,(inv_id,
                 items[i],
                 sections[i],
                 qtys[i],
                 rates[i],
                 amts[i]))

        con.commit()
        return redirect(f"/print_invoice/{inv_id}")

    return render_template(
        "sale_invoice.html",
        la_id=la_id,
        rows=rows,
        customer=customer["acc_name"],
        today=today()
    )
# ---------------- PRINT INVOICE ----------------
@app.route("/print_invoice/<int:inv_id>")
def print_invoice(inv_id):
    con = db()

    head = con.execute(
        "SELECT * FROM sale_invoice_head WHERE inv_id=?",
        (inv_id,)
    ).fetchone()

    body = con.execute(
        "SELECT item_name, section, qty, rate, amount FROM sale_invoice_body WHERE inv_id=?",
        (inv_id,)
    ).fetchall()

    return render_template("print_invoice.html", head=head, body=body)
# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)