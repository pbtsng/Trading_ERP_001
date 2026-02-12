from ai_engine import question_to_sql, run_sql, explain_result
from flask import Flask, render_template, request, redirect, session, jsonify, send_file, g, abort
import sqlite3
from werkzeug.security import check_password_hash
import pandas as pd
from datetime import date
import hmac, hashlib

app = Flask(__name__)
app.secret_key = "secret123"
DB = "database/trading.db"

# ---------------- DATE ----------------
def today():
    return date.today().isoformat()

# ---------------- DB ----------------
def db():
    con = getattr(g, "_db", None)
    if con is None:
        con = sqlite3.connect(DB)
        con.row_factory = sqlite3.Row
        g._db = con
    return con

@app.teardown_appcontext
def close_db(exception):
    con = getattr(g, "_db", None)
    if con is not None:
        con.close()
# ---------------- STATUS HELPERS ----------------
def lock_sale_order(so_id):
    db().execute("UPDATE sale_orders SET status='LOCKED' WHERE so_id=?", (so_id,))
    db().commit()

def lock_purchase_order(po_id):
    db().execute("UPDATE purchase_orders SET status='LOCKED' WHERE po_id=?", (po_id,))
    db().commit()

def lock_loading_advice(la_id):
    db().execute("UPDATE loading_advice_head SET status='LOCKED' WHERE la_id=?", (la_id,))
    db().commit()

# ---------------- ROLE CHECK ----------------
def can_edit():
    return session.get("role") in ["admin","manager"]

def so_token(so_id:int)->str:
    key = app.secret_key.encode()
    msg = f"so:{so_id}".encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()

app.jinja_env.globals.update(so_token=so_token)

# ---------------- LOGIN ----------------
@app.route("/", methods=["GET","POST"])
def login():
    if request.method=="POST":
        con=db()
        u=con.execute(
            "SELECT * FROM users_mast WHERE username=?",
            (request.form["username"],)
        ).fetchone()

        if u and check_password_hash(u["password_hash"], request.form["password"]):
            session["user"]=u["user_code"]
            session["role"]=u["role"]
            return redirect("/dashboard")

    return render_template("login.html")

# ---------------- DASHBOARD ----------------
@app.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect("/")
    con = db()
    so_total = con.execute("SELECT COALESCE(SUM(amount),0) FROM sale_orders").fetchone()[0]
    po_total = con.execute("SELECT COALESCE(SUM(amount),0) FROM purchase_orders").fetchone()[0]
    invoice_total = con.execute("SELECT COALESCE(SUM(total),0) FROM sale_invoice_head").fetchone()[0]
    pipeline = con.execute("""
        SELECT COALESCE(SUM((qty-COALESCE(supplied_qty,0))*rate),0)
        FROM sale_orders
        WHERE (qty-COALESCE(supplied_qty,0))>0
    """).fetchone()[0]
    brokerage = invoice_total * 0.01
    kpi = {
        "total_business": invoice_total,
        "total_profit": max(invoice_total - po_total, 0),
        "in_pipeline": pipeline,
        "brokerage": brokerage
    }
    sale_months = con.execute("""
        SELECT substr(so_date,1,7) ym, COALESCE(SUM(amount),0) total
        FROM sale_orders GROUP BY ym ORDER BY ym
    """).fetchall()
    purchase_months = con.execute("""
        SELECT substr(po_date,1,7) ym, COALESCE(SUM(amount),0) total
        FROM purchase_orders GROUP BY ym ORDER BY ym
    """).fetchall()
    loading_months = con.execute("""
        SELECT substr(h.la_date,1,7) ym, COALESCE(SUM(b.qty),0) total
        FROM loading_advice_body b
        JOIN loading_advice_head h ON b.la_id=h.la_id
        GROUP BY ym ORDER BY ym
    """).fetchall()
    invoice_months = con.execute("""
        SELECT substr(inv_date,1,7) ym, COALESCE(SUM(total),0) total
        FROM sale_invoice_head GROUP BY ym ORDER BY ym
    """).fetchall()
    sale_days = con.execute("""
        SELECT so_date d, COALESCE(SUM(amount),0) total
        FROM sale_orders GROUP BY d ORDER BY d
    """).fetchall()
    sale_years = con.execute("""
        SELECT substr(so_date,1,4) y, COALESCE(SUM(amount),0) total
        FROM sale_orders GROUP BY y ORDER BY y
    """).fetchall()
    purchase_days = con.execute("""
        SELECT po_date d, COALESCE(SUM(amount),0) total
        FROM purchase_orders GROUP BY d ORDER BY d
    """).fetchall()
    purchase_years = con.execute("""
        SELECT substr(po_date,1,4) y, COALESCE(SUM(amount),0) total
        FROM purchase_orders GROUP BY y ORDER BY y
    """).fetchall()
    loading_days = con.execute("""
        SELECT h.la_date d, COALESCE(SUM(b.qty),0) total
        FROM loading_advice_body b
        JOIN loading_advice_head h ON b.la_id=h.la_id
        GROUP BY d ORDER BY d
    """).fetchall()
    loading_years = con.execute("""
        SELECT substr(h.la_date,1,4) y, COALESCE(SUM(b.qty),0) total
        FROM loading_advice_body b
        JOIN loading_advice_head h ON b.la_id=h.la_id
        GROUP BY y ORDER BY y
    """).fetchall()
    invoice_days = con.execute("""
        SELECT inv_date d, COALESCE(SUM(total),0) total
        FROM sale_invoice_head GROUP BY d ORDER BY d
    """).fetchall()
    invoice_years = con.execute("""
        SELECT substr(inv_date,1,4) y, COALESCE(SUM(total),0) total
        FROM sale_invoice_head GROUP BY y ORDER BY y
    """).fetchall()
    pending_pos = con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,i.item_name,p.qty,COALESCE(p.supplied_qty,0) supplied,
               p.rate,(p.qty-COALESCE(p.supplied_qty,0)) pending
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE (p.qty-COALESCE(p.supplied_qty,0))>0
        ORDER BY p.po_date DESC
    """).fetchall()
    suppliers = [r[0] for r in con.execute("""
        SELECT DISTINCT a.acc_name
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE (p.qty-COALESCE(p.supplied_qty,0))>0
        ORDER BY a.acc_name
    """).fetchall()]
    return render_template("dashboard.html",
        kpi=kpi,
        sale_months=sale_months,
        purchase_months=purchase_months,
        loading_months=loading_months,
        invoice_months=invoice_months,
        sale_days=sale_days,
        sale_years=sale_years,
        purchase_days=purchase_days,
        purchase_years=purchase_years,
        loading_days=loading_days,
        loading_years=loading_years,
        invoice_days=invoice_days,
        invoice_years=invoice_years,
        pending_pos=pending_pos,
        suppliers=suppliers
    )

# ---------------- SALE ORDER ----------------
@app.route("/sale",methods=["GET","POST"])
def sale():

    if "user" not in session:
        return redirect("/")

    con=db()

    if request.method=="POST":
        qty=float(request.form["qty"])
        rate=float(request.form["rate"])
        amt=qty*rate

        con.execute("""
        INSERT INTO sale_orders
        (so_date,acc_id,item_id,qty,rate,amount,user_code)
        VALUES (?,?,?,?,?,?,?)
        """,(request.form["date"],
             request.form["acc_id"],
             request.form["item_id"],
             qty,rate,amt,session["user"]))
        con.commit()

    customers=con.execute("""
        SELECT acc_id,acc_name,mobile,city,state
        FROM acc_mast
        WHERE acc_type='Customer'
        ORDER BY acc_name
    """).fetchall()

    items=con.execute("SELECT * FROM item_mast ORDER BY item_name").fetchall()

    rows=con.execute("""
    SELECT so_id,so_date,acc_name,item_name,qty,rate,amount,status,acc_mast.mobile
    FROM sale_orders
    JOIN acc_mast ON sale_orders.acc_id=acc_mast.acc_id
    JOIN item_mast ON sale_orders.item_id=item_mast.item_id
    ORDER BY so_id DESC
""").fetchall()


    return render_template("sale_order.html",
        customers=customers,
        items=items,
        rows=rows,
        today=today()
    )

@app.route("/sale_invoice")
def sale_invoice_page():
    if "user" not in session:
        return redirect("/")
    con = db()
    invoices = con.execute("""
        SELECT inv_id,inv_date,customer,total
        FROM sale_invoice_head
        ORDER BY inv_id DESC
    """).fetchall()
    return render_template("sale_invoice.html", today=today(), customer="", rows=[], invoices=invoices)

@app.route("/so_preview/<int:so_id>", methods=["GET"])
def so_preview(so_id:int):
    token = request.args.get("token","")
    if token != so_token(so_id):
        abort(403)
    con = db()
    row = con.execute("""
        SELECT s.so_id,s.so_date,a.acc_name,a.mobile,i.item_name,s.qty,s.rate,s.amount,s.status
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE s.so_id=?
    """,(so_id,)).fetchone()
    if not row:
        abort(404)
    return render_template("so_preview.html", r=row)

@app.route("/so_preview/<int:so_id>/accept", methods=["POST"])
def so_accept(so_id:int):
    token = request.args.get("token","")
    if token != so_token(so_id):
        abort(403)
    con = db()
    con.execute("UPDATE sale_orders SET status='ACCEPTED' WHERE so_id=?", (so_id,))
    con.commit()
    return render_template("so_preview.html", r=con.execute("""
        SELECT s.so_id,s.so_date,a.acc_name,a.mobile,i.item_name,s.qty,s.rate,s.amount,s.status
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE s.so_id=?
    """,(so_id,)).fetchone())

@app.route("/so_preview/<int:so_id>/decline", methods=["POST"])
def so_decline(so_id:int):
    token = request.args.get("token","")
    if token != so_token(so_id):
        abort(403)
    con = db()
    con.execute("UPDATE sale_orders SET status='DECLINED' WHERE so_id=?", (so_id,))
    con.commit()
    return render_template("so_preview.html", r=con.execute("""
        SELECT s.so_id,s.so_date,a.acc_name,a.mobile,i.item_name,s.qty,s.rate,s.amount,s.status
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE s.so_id=?
    """,(so_id,)).fetchone())
# ---------------- PURCHASE ORDER ----------------
@app.route("/purchase",methods=["GET","POST"])
def purchase():

    if "user" not in session:
        return redirect("/")

    con=db()

    if request.method=="POST":
        qty=float(request.form["qty"])
        rate=float(request.form["rate"])
        amt=qty*rate

        con.execute("""
        INSERT INTO purchase_orders
        (po_date,acc_id,item_id,qty,rate,amount,user_code)
        VALUES (?,?,?,?,?,?,?)
        """,(request.form["date"],
             request.form["acc_id"],
             request.form["item_id"],
             qty,rate,amt,session["user"]))
        con.commit()

    suppliers=con.execute("""
        SELECT acc_id,acc_name,mobile,city,state
        FROM acc_mast
        WHERE acc_type='Supplier'
        ORDER BY acc_name
    """).fetchall()

    items=con.execute("SELECT * FROM item_mast ORDER BY item_name").fetchall()

    rows=con.execute("""
        SELECT po_id,po_date,acc_name,item_name,qty,rate,amount
        FROM purchase_orders
        JOIN acc_mast ON purchase_orders.acc_id=acc_mast.acc_id
        JOIN item_mast ON purchase_orders.item_id=item_mast.item_id
        ORDER BY po_id DESC
    """).fetchall()

    return render_template("purchase_order.html",
        suppliers=suppliers,
        items=items,
        rows=rows,
        today=today()
    )

# ---------------- ADD ACCOUNT ----------------
@app.route("/add_account",methods=["POST"])
def add_account():

    if "user" not in session:
        return redirect("/")

    con=db()

    row=con.execute(
        "SELECT acc_id FROM acc_mast WHERE acc_name=?",
        (request.form["name"],)
    ).fetchone()

    if not row:
        con.execute("""
        INSERT INTO acc_mast
        (acc_name,acc_type,mobile,pan,gstin,address,city,state)
        VALUES (?,?,?,?,?,?,?,?)
        """,(request.form["name"],
             request.form["type"],
             request.form.get("mobile"),
             request.form.get("pan"),
             request.form.get("gstin"),
             request.form.get("address"),
             request.form.get("city"),
             request.form.get("state")))
        con.commit()

    return redirect(request.referrer)

# ---------------- ADD ITEM ----------------
@app.route("/add_item",methods=["POST"])
def add_item():

    if "user" not in session:
        return redirect("/")

    con=db()

    row=con.execute(
        "SELECT item_id FROM item_mast WHERE item_name=?",
        (request.form["name"],)
    ).fetchone()

    if not row:
        con.execute(
            "INSERT INTO item_mast(item_name) VALUES(?)",
            (request.form["name"],)
        )
        con.commit()

    return redirect(request.referrer)

# ---------------- LOADING ADVICE ----------------
@app.route("/loading_advice",methods=["GET","POST"])
def loading_advice():

    if "user" not in session:
        return redirect("/")

    con=db()

    if request.method=="POST":

        cur=con.execute("""
        INSERT INTO loading_advice_head
        (la_date,vehicle_no,user_code)
        VALUES (?,?,?)
        """,(request.form["date"],
             request.form["vehicle"],
             session["user"]))
        la_id=cur.lastrowid

        so_ids=request.form.getlist("so_id")
        po_ids=request.form.getlist("po_id")
        sections=request.form.getlist("section")
        batchs=request.form.getlist("batch")
        qtys=request.form.getlist("qty")

        for i in range(len(qtys)):
            if qtys[i]=="":
                continue

            so_id = so_ids[i] if i < len(so_ids) else ""
            po_id = po_ids[i] if i < len(po_ids) else ""
            section = sections[i] if i < len(sections) else ""
            batch = batchs[i] if i < len(batchs) else ""

            if not so_id or not po_id:
                continue

            try:
                qty = float(qtys[i])
            except:
                qty = 0.0

            con.execute("""
            INSERT INTO loading_advice_body
            (la_id,so_id,po_id,section,part,qty)
            VALUES (?,?,?,?,?,?)
            """,(la_id,so_id,po_id,section,batch,qty))

            con.execute("UPDATE sale_orders SET supplied_qty=supplied_qty+? WHERE so_id=?",
                        (qty,so_id))
            con.execute("UPDATE purchase_orders SET supplied_qty=supplied_qty+? WHERE po_id=?",
                        (qty,po_id))
            lock_sale_order(so_id)
            lock_purchase_order(po_id)
        con.commit()

    sales=con.execute("""
        SELECT s.so_id,s.so_date,a.acc_name,i.item_name,
               (s.qty-s.supplied_qty) AS bal,s.rate
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE (s.qty-s.supplied_qty)>0
        ORDER BY s.so_date
    """).fetchall()

    purchases=con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,i.item_name,
               (p.qty-p.supplied_qty) AS bal,p.rate
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE (p.qty-p.supplied_qty)>0
        ORDER BY p.po_date
    """).fetchall()

    las=con.execute("""
        SELECT la_id,la_date,vehicle_no
        FROM loading_advice_head
        ORDER BY la_id DESC
    """).fetchall()

    la_rows={}
    for la in las:
        la_rows[la["la_id"]] = con.execute("""
            SELECT section,qty,part
            FROM loading_advice_body
            WHERE la_id=?
        """,(la["la_id"],)).fetchall()

    return render_template("loading_advice.html",
        sales=sales,
        purchases=purchases,
        las=las,
        la_rows=la_rows,
        today=today()
    )

@app.route("/loading_advice_print")
def loading_advice_print():
    if "user" not in session:
        return redirect("/")
    con = db()
    las = con.execute("""
        SELECT la_id,la_date,vehicle_no
        FROM loading_advice_head
        ORDER BY la_id DESC
    """).fetchall()
    la_rows = {}
    for la in las:
        la_rows[la["la_id"]] = con.execute("""
            SELECT section,qty,part
            FROM loading_advice_body
            WHERE la_id=?
        """, (la["la_id"],)).fetchall()
    return render_template("loading_advice_print.html", las=las, la_rows=la_rows, today=today())
# ---------------- API HELPERS ----------------
@app.route("/get_so/<int:so_id>")
def get_so(so_id):
    con = db()
    r = con.execute("""
        SELECT a.acc_name,s.qty,s.supplied_qty,s.rate
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE so_id=?
    """, (so_id,)).fetchone()
    return jsonify({"customer": r[0], "balance": r[1] - r[2], "rate": r[3]})

@app.route("/get_po/<int:po_id>")
def get_po(po_id):
    con = db()
    r = con.execute("""
        SELECT a.acc_name,p.qty,p.supplied_qty,p.rate,i.item_name
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE po_id=?
    """, (po_id,)).fetchone()
    return jsonify({"supplier": r[0], "balance": r[1] - r[2], "rate": r[3], "item": r[4]})

# ---------------- ACCOUNT MASTER ----------------
@app.route("/accounts", methods=["GET","POST"])
def accounts():
    if "user" not in session:
        return redirect("/")
    con = db()
    if request.method == "POST":
        name = request.form["name"]
        acc_type = request.form["type"]
        row = con.execute("SELECT acc_id FROM acc_mast WHERE acc_name=?", (name,)).fetchone()
        if not row:
            con.execute("""
                INSERT INTO acc_mast(acc_name,acc_type)
                VALUES (?,?)
            """, (name, acc_type))
            con.commit()
    rows = con.execute("SELECT * FROM acc_mast").fetchall()
    return render_template("accounts.html", rows=rows)

# ---------------- ITEM MASTER ----------------
@app.route("/items", methods=["GET","POST"])
def items():
    if "user" not in session:
        return redirect("/")
    con = db()
    if request.method == "POST":
        name = request.form["name"]
        row = con.execute("SELECT item_id FROM item_mast WHERE item_name=?", (name,)).fetchone()
        if not row:
            con.execute("INSERT INTO item_mast(item_name) VALUES (?)", (name,))
            con.commit()
    rows = con.execute("SELECT * FROM item_mast").fetchall()
    return render_template("items.html", rows=rows)

# ---------------- CONTROL REPORT ----------------
@app.route("/control_report")
def control_report():
    if "user" not in session:
        return redirect("/")
    con = db()
    so_total = con.execute("SELECT COALESCE(SUM(amount),0) FROM sale_orders").fetchone()[0]
    po_total = con.execute("SELECT COALESCE(SUM(amount),0) FROM purchase_orders").fetchone()[0]
    loaded_qty = con.execute("SELECT COALESCE(SUM(qty),0) FROM loading_advice_body").fetchone()[0]
    invoice_total = con.execute("SELECT COALESCE(SUM(total),0) FROM sale_invoice_head").fetchone()[0]
    summary = {
        "so_total": so_total,
        "po_total": po_total,
        "loaded_qty": loaded_qty,
        "invoice_total": invoice_total
    }
    items = []
    for i in con.execute("SELECT item_id,item_name FROM item_mast").fetchall():
        item_id, item_name = i[0], i[1]
        so_qty = con.execute("SELECT COALESCE(SUM(qty),0) FROM sale_orders WHERE item_id=?", (item_id,)).fetchone()[0]
        po_qty = con.execute("SELECT COALESCE(SUM(qty),0) FROM purchase_orders WHERE item_id=?", (item_id,)).fetchone()[0]
        loaded = con.execute("""
            SELECT COALESCE(SUM(b.qty),0)
            FROM loading_advice_body b
            JOIN sale_orders s ON b.so_id=s.so_id
            WHERE s.item_id=?
        """, (item_id,)).fetchone()[0]
        pending_so = con.execute("""
            SELECT COALESCE(SUM(qty - supplied_qty),0)
            FROM sale_orders WHERE item_id=?
        """, (item_id,)).fetchone()[0]
        pending_po = con.execute("""
            SELECT COALESCE(SUM(qty - supplied_qty),0)
            FROM purchase_orders WHERE item_id=?
        """, (item_id,)).fetchone()[0]
        items.append((item_name, so_qty, loaded, pending_so, po_qty, pending_po))
    return render_template("control_report.html", summary=summary, items=items)

# ---------------- ITEM DRILL ----------------
@app.route("/item_drill/<item>")
def item_drill(item):
    if "user" not in session:
        return redirect("/")
    con = db()
    sales = con.execute("""
        SELECT s.so_id,a.acc_name,s.qty,s.supplied_qty
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE i.item_name=?
        ORDER BY s.so_id DESC
    """, (item,)).fetchall()
    purchases = con.execute("""
        SELECT p.po_id,a.acc_name,p.qty,p.supplied_qty
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE i.item_name=?
        ORDER BY p.po_id DESC
    """, (item,)).fetchall()
    return render_template("item_drill.html", item=item, sales=sales, purchases=purchases)

# ---------------- SALE INVOICE ----------------
@app.route("/sale_invoice/<int:la_id>",methods=["GET","POST"])
def sale_invoice(la_id):

    if "user" not in session:
        return redirect("/")

    con=db()

    customer=con.execute("""
        SELECT a.acc_name
        FROM loading_advice_body b
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE b.la_id=?
        LIMIT 1
    """,(la_id,)).fetchone()

    rows=con.execute("""
        SELECT i.item_name,b.section,b.qty
        FROM loading_advice_body b
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE b.la_id=?
    """,(la_id,)).fetchall()

    if request.method=="POST":

        total=float(request.form.get("grand_total",0))

        cur=con.execute("""
        INSERT INTO sale_invoice_head
        (la_id,inv_date,customer,total)
        VALUES (?,?,?,?)
        """,(la_id,
             request.form["date"],
             request.form["customer"],
             total))

        inv_id=cur.lastrowid

        items=request.form.getlist("item")
        sections=request.form.getlist("section")
        qtys=request.form.getlist("qty")
        rates=request.form.getlist("rate")
        amts=request.form.getlist("amount")

        for i in range(len(items)):

            rate = float(rates[i]) if rates[i] else 0
            amt  = float(amts[i]) if amts[i] else 0

            con.execute("""
            INSERT INTO sale_invoice_body
            (inv_id,item_name,section,qty,rate,amount)
            VALUES (?,?,?,?,?,?)
            """,(inv_id,
                 items[i],
                 sections[i],
                 qtys[i],
                 rate,
                 amt))
        lock_loading_advice(la_id)
        con.commit()
        return redirect(f"/print_invoice/{inv_id}")

    return render_template(
        "sale_invoice.html",
        la_id=la_id,
        rows=rows,
        customer=customer["acc_name"],
        today=today(),
        invoices=con.execute("""
            SELECT inv_id,inv_date,customer,total
            FROM sale_invoice_head
            ORDER BY inv_id DESC
        """).fetchall()
    )

#-----------------AI----------------------------
@app.route("/ai")
def ai_page():
    return render_template("ai_chat.html")


@app.route("/ask_ai", methods=["POST"])
def ask_ai():
    try:
        question = request.form["question"]
        sql = question_to_sql(question)
        df = run_sql(sql)
        answer = explain_result(df)
        return jsonify({"sql": sql, "answer": answer})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ---------------- PRINT INVOICE ----------------
@app.route("/print_invoice/<int:inv_id>")
def print_invoice(inv_id):

    con=db()

    head=con.execute(
        "SELECT * FROM sale_invoice_head WHERE inv_id=?",
        (inv_id,)
    ).fetchone()

    body=con.execute("""
        SELECT item_name,section,qty,rate,amount
        FROM sale_invoice_body
        WHERE inv_id=?
    """,(inv_id,)).fetchall()

    return render_template("print_invoice.html",
        head=head,
        body=body
    )

# ---------------- EXPORT LOADING EXCEL ----------------
@app.route("/export_loading_excel")
def export_loading_excel():

    con=db()

    rows=con.execute("""
        SELECT h.la_id,h.la_date,h.vehicle_no,
               b.so_id,b.po_id,b.section,b.part,b.qty
        FROM loading_advice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        ORDER BY h.la_id DESC
    """).fetchall()

    df=pd.DataFrame(rows)
    file="loading_advice.xlsx"
    df.to_excel(file,index=False)

    return send_file(file,as_attachment=True)

# ---------------- Sale Delete ----------------
@app.route("/sale_delete/<int:id>")
def sale_delete(id):
    if not can_edit():
        abort(403)
    con=db()
    st=con.execute("SELECT status FROM sale_orders WHERE so_id=?", (id,)).fetchone()
    if st and st["status"]=="LOCKED":
        return "❌ Sale Order Locked"
    con.execute("DELETE FROM sale_orders WHERE so_id=?", (id,))
    con.commit()
    return redirect("/sale")
# ---------------- Purchase Delete ----------------
@app.route("/purchase_delete/<int:id>") 
def purchase_delete(id):
    if not can_edit():
        abort(403)
    con=db()
    st=con.execute("SELECT status FROM purchase_orders WHERE po_id=?", (id,)).fetchone()
    if st and st["status"]=="LOCKED":
        return "❌ Purchase Order Locked"
    con.execute("DELETE FROM purchase_orders WHERE po_id=?", (id,))
    con.commit()
    return redirect("/purchase")

# ---------------- SALE EDIT ----------------
@app.route("/sale_edit/<int:id>", methods=["GET","POST"])
def sale_edit(id):
    if "user" not in session:
        return redirect("/")
    if not can_edit():
        abort(403)
    con = db()
    row = con.execute("""
        SELECT so_id,so_date,acc_id,item_id,qty,rate,supplied_qty,status
        FROM sale_orders WHERE so_id=?
    """, (id,)).fetchone()
    if not row:
        return "Sale Order not found"
    if request.method=="POST":
        st = row["status"] if "status" in row.keys() else None
        if st == "LOCKED":
            return "❌ Sale Order Locked"
        qty = float(request.form["qty"])
        rate = float(request.form["rate"])
        if qty < float(row["supplied_qty"] or 0):
            return "❌ Qty cannot be less than supplied"
        amt = qty * rate
        con.execute("""
            UPDATE sale_orders SET qty=?, rate=?, amount=? WHERE so_id=?
        """, (qty, rate, amt, id))
        con.commit()
        return redirect("/sale")
    return render_template("sale_edit.html", r=row)

# ---------------- PURCHASE EDIT ----------------
@app.route("/purchase_edit/<int:id>", methods=["GET","POST"])
def purchase_edit(id):
    if "user" not in session:
        return redirect("/")
    if not can_edit():
        abort(403)
    con = db()
    row = con.execute("""
        SELECT po_id,po_date,acc_id,item_id,qty,rate,supplied_qty,status
        FROM purchase_orders WHERE po_id=?
    """, (id,)).fetchone()
    if not row:
        return "Purchase Order not found"
    if request.method=="POST":
        st = row["status"] if "status" in row.keys() else None
        if st == "LOCKED":
            return "❌ Purchase Order Locked"
        qty = float(request.form["qty"])
        rate = float(request.form["rate"])
        if qty < float(row["supplied_qty"] or 0):
            return "❌ Qty cannot be less than supplied"
        amt = qty * rate
        con.execute("""
            UPDATE purchase_orders SET qty=?, rate=?, amount=? WHERE po_id=?
        """, (qty, rate, amt, id))
        con.commit()
        return redirect("/purchase")
    return render_template("purchase_edit.html", r=row)

# ---------------- LOADING ADVICE EDIT ----------------
@app.route("/loading_advice_edit/<int:la_id>", methods=["GET","POST"])
def loading_advice_edit(la_id):
    if "user" not in session:
        return redirect("/")
    if not can_edit():
        abort(403)
    con = db()
    head = con.execute("SELECT * FROM loading_advice_head WHERE la_id=?", (la_id,)).fetchone()
    if not head:
        return "Loading Advice not found"
    st = head["status"] if "status" in head.keys() else None
    if request.method=="POST":
        if st == "LOCKED":
            return "❌ Loading Advice Locked"
        date_ = request.form["la_date"]
        vehicle = request.form["vehicle_no"]
        con.execute("UPDATE loading_advice_head SET la_date=?, vehicle_no=? WHERE la_id=?", (date_, vehicle, la_id))
        ids = request.form.getlist("id")
        qtys = request.form.getlist("qty")
        sections = request.form.getlist("section")
        parts = request.form.getlist("part")
        for i in range(len(ids)):
            rid = int(ids[i])
            new_qty = float(qtys[i] or 0)
            sec = sections[i]
            part = parts[i]
            old = con.execute("SELECT qty, so_id, po_id FROM loading_advice_body WHERE id=?", (rid,)).fetchone()
            if not old:
                continue
            delta = new_qty - float(old["qty"] or 0)
            con.execute("UPDATE loading_advice_body SET qty=?, section=?, part=? WHERE id=?", (new_qty, sec, part, rid))
            if delta != 0:
                con.execute("UPDATE sale_orders SET supplied_qty=COALESCE(supplied_qty,0)+? WHERE so_id=?", (delta, old["so_id"]))
                con.execute("UPDATE purchase_orders SET supplied_qty=COALESCE(supplied_qty,0)+? WHERE po_id=?", (delta, old["po_id"]))
        con.commit()
        return redirect("/loading_advice")
    rows = con.execute("SELECT id, section, qty, part FROM loading_advice_body WHERE la_id=?", (la_id,)).fetchall()
    return render_template("loading_advice_edit.html", head=head, rows=rows)

# ---------------- LOADING ADVICE DELETE ----------------
@app.route("/loading_advice_delete/<int:la_id>")
def loading_advice_delete(la_id):
    if "user" not in session:
        return redirect("/")
    if not can_edit():
        abort(403)
    con = db()
    head = con.execute("SELECT * FROM loading_advice_head WHERE la_id=?", (la_id,)).fetchone()
    if not head:
        return redirect("/loading_advice")
    st = head["status"] if "status" in head.keys() else None
    if st == "LOCKED":
        return "❌ Loading Advice Locked"
    rows = con.execute("SELECT id, qty, so_id, po_id FROM loading_advice_body WHERE la_id=?", (la_id,)).fetchall()
    for r in rows:
        qty = float(r["qty"] or 0)
        con.execute("UPDATE sale_orders SET supplied_qty=COALESCE(supplied_qty,0)-? WHERE so_id=?", (qty, r["so_id"]))
        con.execute("UPDATE purchase_orders SET supplied_qty=COALESCE(supplied_qty,0)-? WHERE po_id=?", (qty, r["po_id"]))
    con.execute("DELETE FROM loading_advice_body WHERE la_id=?", (la_id,))
    con.execute("DELETE FROM loading_advice_head WHERE la_id=?", (la_id,))
    con.commit()
    return redirect("/loading_advice")
# ---------------- RUN ----------------
if __name__=="__main__":
    app.run(host="0.0.0.0",port=5000,debug=True)
