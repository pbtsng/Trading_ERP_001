from flask import Flask,render_template,request,redirect,session,jsonify
import sqlite3
from werkzeug.security import check_password_hash
app = Flask(__name__)
app.secret_key="secret123"
DB="database/trading.db"
# ---------------- DB ----------------
def db():
    return sqlite3.connect(DB)
# ---------------- LOGIN ----------------
@app.route("/",methods=["GET","POST"])
def login():
    if request.method=="POST":
        con=db()
        u=con.execute("SELECT * FROM users_mast WHERE username=?",
                      (request.form["username"],)).fetchone()
        if u and check_password_hash(u[2],request.form["password"]):
            session["user"]=u[0]
            session["role"]=u[3]
            return redirect("/dashboard")
    return render_template("login.html")
@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")
# ---------------- SALE ORDER ----------------
@app.route("/sale",methods=["GET","POST"])
def sale():
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
    customers = con.execute("""
    SELECT acc_id, acc_name, mobile, city, state
    FROM acc_mast
    WHERE acc_type='Customer'
    ORDER BY acc_name
""").fetchall()

    items=con.execute("SELECT * FROM item_mast").fetchall()

    rows=con.execute("""
    SELECT so_id,so_date,acc_name,item_name,qty,rate,amount
    FROM sale_orders
    JOIN acc_mast ON sale_orders.acc_id=acc_mast.acc_id
    JOIN item_mast ON sale_orders.item_id=item_mast.item_id
    """).fetchall()
    return render_template("sale_order.html",
        customers=customers,items=items,rows=rows)
# ---------------- PURCHASE ORDER ----------------
@app.route("/purchase",methods=["GET","POST"])
def purchase():
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
    suppliers = con.execute("""
    SELECT acc_id, acc_name, mobile, city, state
    FROM acc_mast
    WHERE acc_type='Supplier'
    ORDER BY acc_name
""").fetchall()

    items=con.execute("SELECT * FROM item_mast").fetchall()

    rows=con.execute("""
    SELECT po_id,po_date,acc_name,item_name,qty,rate,amount
    FROM purchase_orders
    JOIN acc_mast ON purchase_orders.acc_id=acc_mast.acc_id
    JOIN item_mast ON purchase_orders.item_id=item_mast.item_id
    """).fetchall()

    return render_template("purchase_order.html",
        suppliers=suppliers,items=items,rows=rows)
# ---------------- ADD ACCOUNT (Customer / Supplier) ----------------
@app.route("/add_account", methods=["POST"])
def add_account():

    if "user" not in session:
        return redirect("/")

    name = request.form["name"]
    acc_type = request.form["type"]
    mobile = request.form.get("mobile")
    pan = request.form.get("pan")
    gstin = request.form.get("gstin")
    address = request.form.get("address")
    city = request.form.get("city")
    state = request.form.get("state")

    con = db()

    # Prevent duplicate name
    row = con.execute(
        "SELECT acc_id FROM acc_mast WHERE acc_name=?",
        (name,)
    ).fetchone()

    if not row:
        con.execute("""
        INSERT INTO acc_mast
        (acc_name,acc_type,mobile,pan,gstin,address,city,state)
        VALUES (?,?,?,?,?,?,?,?)
        """,(name,acc_type,mobile,pan,gstin,address,city,state))
        con.commit()

    return redirect(request.referrer)
# ---------------- ADD ITEM ----------------
@app.route("/add_item", methods=["POST"])
def add_item():

    if "user" not in session:
        return redirect("/")

    name = request.form["name"]

    con = db()

    # Prevent duplicate item
    row = con.execute(
        "SELECT item_id FROM item_mast WHERE item_name=?",
        (name,)
    ).fetchone()

    if not row:
        con.execute("""
        INSERT INTO item_mast(item_name)
        VALUES (?)
        """,(name,))
        con.commit()

    return redirect(request.referrer)

# ---------------- LOADING ADVICE ----------------
@app.route("/loading_advice",methods=["GET","POST"])
def loading_advice():

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
        items=request.form.getlist("item")
        sections=request.form.getlist("section")
        batchs=request.form.getlist("batch")
        qtys=request.form.getlist("qty")
        for i in range(len(qtys)):
            if qtys[i]=="":
                continue
            qty=float(qtys[i])
            srate=con.execute(
                "SELECT rate FROM sale_orders WHERE so_id=?",
                (so_ids[i],)).fetchone()[0]
            prate=con.execute(
                "SELECT rate FROM purchase_orders WHERE po_id=?",
                (po_ids[i],)).fetchone()[0]
            amt=qty*srate
            diff=srate-prate
            con.execute("""
            INSERT INTO loading_advice_body
            (la_id,so_id,po_id,item_name,section,part,
             qty,purchase_rate,sale_rate,rate_diff,amount)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,(la_id,so_ids[i],po_ids[i],
                 items[i],sections[i],batchs[i],
                 qty,prate,srate,diff,amt))
            con.execute("UPDATE sale_orders SET supplied_qty=supplied_qty+? WHERE so_id=?",(qty,so_ids[i]))
            con.execute("UPDATE purchase_orders SET supplied_qty=supplied_qty+? WHERE po_id=?",(qty,po_ids[i]))
        con.commit()
       # -------- DROPDOWNS ----------
    sales = con.execute("""
        SELECT s.so_id, a.acc_name,
               s.qty - s.supplied_qty AS bal,
               s.rate
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id = a.acc_id
        WHERE s.qty - s.supplied_qty > 0
    """).fetchall()
    purchases = con.execute("""
        SELECT p.po_id, a.acc_name,
               p.qty - p.supplied_qty AS bal,
               p.rate, i.item_name
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id = a.acc_id
        JOIN item_mast i ON p.item_id = i.item_id
        WHERE p.qty - p.supplied_qty > 0
    """).fetchall()
    las=con.execute("""
    SELECT h.la_id,h.la_date,h.vehicle_no,a.acc_name,SUM(b.qty)
    FROM loading_advice_head h
    JOIN loading_advice_body b ON h.la_id=b.la_id
    JOIN sale_orders s ON b.so_id=s.so_id
    JOIN acc_mast a ON s.acc_id=a.acc_id
    GROUP BY h.la_id
    ORDER BY h.la_id DESC
    """).fetchall()
    la_rows={}
    for la in las:
        la_rows[la[0]]=con.execute("""
        SELECT section,qty,part FROM loading_advice_body
        WHERE la_id=?
        """,(la[0],)).fetchall()
    return render_template("loading_advice.html",
        sales=sales,purchases=purchases,
        las=las,la_rows=la_rows)
# ---------------- SALE INVOICE ----------------
@app.route("/sale_invoice/<int:la_id>",methods=["GET","POST"])
def sale_invoice(la_id):
    con=db()
    rows=con.execute("""
    SELECT item_name,section,qty,sale_rate
    FROM loading_advice_body
    WHERE la_id=?
    """,(la_id,)).fetchall()
    if request.method=="POST":

        customer=request.form["customer"]
        date=request.form["date"]
        total=request.form["total"]

        cur=con.execute("""
        INSERT INTO sale_invoice_head
        (la_id,customer,inv_date,total)
        VALUES (?,?,?,?)
        """,(la_id,customer,date,total))

        inv_id=cur.lastrowid

        items=request.form.getlist("item")
        sections=request.form.getlist("section")
        qtys=request.form.getlist("qty")
        rates=request.form.getlist("rate")
        amts=request.form.getlist("amount")

        for i in range(len(items)):
            con.execute("""
            INSERT INTO sale_invoice_body
            (inv_id,item_name,section,qty,rate,amount)
            VALUES (?,?,?,?,?,?)
            """,(inv_id,items[i],sections[i],qtys[i],rates[i],amts[i]))

        con.commit()
        return redirect(f"/print_invoice/{inv_id}")

    return render_template("sale_invoice.html",la_id=la_id,rows=rows)

# ---------------- PRINT INVOICE ----------------
@app.route("/print_invoice/<int:inv_id>")
def print_invoice(inv_id):

    con=db()
    head=con.execute("SELECT * FROM sale_invoice_head WHERE inv_id=?",(inv_id,)).fetchone()
    body=con.execute("""
    SELECT item_name,section,qty,rate,amount
    FROM sale_invoice_body
    WHERE inv_id=?
    """,(inv_id,)).fetchall()

    return render_template("print_invoice.html",head=head,body=body)

# ---------------- API HELPERS ----------------
@app.route("/get_so/<int:so_id>")
def get_so(so_id):
    con=db()
    r=con.execute("""
    SELECT a.acc_name,s.qty,s.supplied_qty,s.rate
    FROM sale_orders s
    JOIN acc_mast a ON s.acc_id=a.acc_id
    WHERE so_id=?
    """,(so_id,)).fetchone()

    return jsonify({"customer":r[0],"balance":r[1]-r[2],"rate":r[3]})

@app.route("/get_po/<int:po_id>")
def get_po(po_id):
    con=db()
    r=con.execute("""
    SELECT a.acc_name,p.qty,p.supplied_qty,p.rate,i.item_name
    FROM purchase_orders p
    JOIN acc_mast a ON p.acc_id=a.acc_id
    JOIN item_mast i ON p.item_id=i.item_id
    WHERE po_id=?
    """,(po_id,)).fetchone()

    return jsonify({"supplier":r[0],"balance":r[1]-r[2],"rate":r[3],"item":r[4]})
import pandas as pd
from flask import send_file

@app.route("/export_loading_excel")
def export_loading_excel():

    con = db()

    rows = con.execute("""
        SELECT h.la_id,
               h.la_date,
               h.vehicle_no,
               a.acc_name AS customer,
               b.so_id,
               b.po_id,
               b.item_name,
               b.section,
               b.part,
               b.qty,
               b.purchase_rate,
               b.sale_rate,
               b.rate_diff,
               b.amount
        FROM loading_advice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN acc_mast a ON s.acc_id=a.acc_id
        ORDER BY h.la_id DESC
    """).fetchall()

    cols = [
        "LA No","LA Date","Vehicle","Customer",
        "SO No","PO No","Item","Section","Batch",
        "Qty","Purchase Rate","Sale Rate",
        "Rate Diff","Amount"
    ]

    df = pd.DataFrame(rows, columns=cols)

    file_path = "loading_advice_report.xlsx"
    df.to_excel(file_path, index=False)

    return send_file(file_path, as_attachment=True)

# ---------------- RUN ----------------
if __name__=="__main__":
    app.run(host="0.0.0.0",port=5000,debug=True)
# ---------------- ACCOUNT MASTER ----------------
@app.route("/accounts", methods=["GET","POST"])
def accounts():

    if "user" not in session:
        return redirect("/")

    con = db()

    if request.method == "POST":

        name = request.form["name"]
        acc_type = request.form["type"]

        # Prevent duplicate
        row = con.execute(
            "SELECT acc_id FROM acc_mast WHERE acc_name=?",
            (name,)
        ).fetchone()

        if not row:
            con.execute("""
            INSERT INTO acc_mast(acc_name,acc_type)
            VALUES (?,?)
            """,(name,acc_type))
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

        row = con.execute(
            "SELECT item_id FROM item_mast WHERE item_name=?",
            (name,)
        ).fetchone()
        if not row:
            con.execute("""
            INSERT INTO item_mast(item_name)
            VALUES (?)
            """,(name,))
            con.commit()
    rows = con.execute("SELECT * FROM item_mast").fetchall()
    return render_template("items.html", rows=rows)   