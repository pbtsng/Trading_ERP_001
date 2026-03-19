from ai_engine import question_to_sql, run_sql, explain_result
from flask import Flask, render_template, request, redirect, session, jsonify, send_file, g, abort, flash, url_for
import sqlite3
from werkzeug.security import check_password_hash
import pandas as pd
from datetime import date, datetime
import hmac, hashlib
from urllib.parse import quote_plus
import secrets, time

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

def po_token(po_id:int)->str:
    key = app.secret_key.encode()
    msg = f"po:{po_id}".encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()

app.jinja_env.globals.update(po_token=po_token)

def la_token(la_id:int)->str:
    key = app.secret_key.encode()
    msg = f"la:{la_id}".encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()

app.jinja_env.globals.update(la_token=la_token)

def field_attr(config, name, default_tabindex=None):
    cfg = config.get(name) if config else None
    attrs = []
    if cfg and cfg.get("required"):
        attrs.append("required")
        attrs.append('aria-required="true"')
    if cfg and cfg.get("tab_index") is not None:
        attrs.append(f'tabindex="{cfg["tab_index"]}"')
    elif default_tabindex is not None:
        attrs.append(f'tabindex="{default_tabindex}"')
    return " ".join(attrs)

app.jinja_env.globals.update(field_attr=field_attr)

def ensure_audit_table(con):
    con.execute("""
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
    for stmt in [
        "ALTER TABLE audit_log ADD COLUMN company_id INTEGER",
        "ALTER TABLE audit_log ADD COLUMN division_id INTEGER",
        "ALTER TABLE audit_log ADD COLUMN fy_id INTEGER"
    ]:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass

def run_self_audit(con):
    issues = []
    companies_missing = con.execute("""
        SELECT company_id,company_name
        FROM company_mast
        WHERE COALESCE(gstin,'')='' OR COALESCE(state,'')=''
    """).fetchall()
    if companies_missing:
        issues.append({
            "category": "Company Master",
            "severity": "WARNING",
            "code": "COMPANY_MISSING_GST_OR_STATE",
            "message": "Some companies are missing GSTIN or state.",
            "count": len(companies_missing),
            "examples": [f"{r['company_name']} (ID {r['company_id']})" for r in companies_missing[:5]],
        })
    fy_missing = con.execute("""
        SELECT c.company_id,c.company_name
        FROM company_mast c
        LEFT JOIN financial_years f ON c.company_id=f.company_id
        WHERE f.fy_id IS NULL
    """).fetchall()
    if fy_missing:
        issues.append({
            "category": "Financial Years",
            "severity": "WARNING",
            "code": "COMPANY_MISSING_FY",
            "message": "Some companies do not have any financial years defined.",
            "count": len(fy_missing),
            "examples": [f"{r['company_name']} (ID {r['company_id']})" for r in fy_missing[:5]],
        })
    cust_missing_state = con.execute("""
        SELECT DISTINCT a.acc_id,a.acc_name
        FROM acc_mast a
        JOIN sale_orders s ON a.acc_id=s.acc_id
        WHERE COALESCE(a.state,'')=''
    """).fetchall()
    if cust_missing_state:
        issues.append({
            "category": "Accounts",
            "severity": "WARNING",
            "code": "CUSTOMER_MISSING_STATE",
            "message": "Customers used in sale orders are missing state.",
            "count": len(cust_missing_state),
            "examples": [f"{r['acc_name']} (ID {r['acc_id']})" for r in cust_missing_state[:5]],
        })
    sup_missing_state = con.execute("""
        SELECT DISTINCT a.acc_id,a.acc_name
        FROM acc_mast a
        JOIN purchase_orders p ON a.acc_id=p.acc_id
        WHERE COALESCE(a.state,'')=''
    """).fetchall()
    if sup_missing_state:
        issues.append({
            "category": "Accounts",
            "severity": "WARNING",
            "code": "SUPPLIER_MISSING_STATE",
            "message": "Suppliers used in purchase orders are missing state.",
            "count": len(sup_missing_state),
            "examples": [f"{r['acc_name']} (ID {r['acc_id']})" for r in sup_missing_state[:5]],
        })
    vouchers_unbalanced = con.execute("""
        SELECT h.v_id,h.v_date,h.v_type,
               COALESCE(SUM(l.dr),0) dr,
               COALESCE(SUM(l.cr),0) cr
        FROM vouchers_head h
        LEFT JOIN vouchers_lines l ON h.v_id=l.v_id
        GROUP BY h.v_id,h.v_date,h.v_type
        HAVING ABS(COALESCE(SUM(l.dr),0) - COALESCE(SUM(l.cr),0)) > 0.01
    """).fetchall()
    if vouchers_unbalanced:
        issues.append({
            "category": "Vouchers",
            "severity": "ERROR",
            "code": "UNBALANCED_VOUCHERS",
            "message": "Some vouchers are not balanced (DR != CR).",
            "count": len(vouchers_unbalanced),
            "examples": [f"V#{r['v_id']} {r['v_type']} on {r['v_date']} (DR={r['dr']}, CR={r['cr']})" for r in vouchers_unbalanced[:5]],
        })
    vouchers_missing_ctx = con.execute("""
        SELECT v_id,v_date,v_type
        FROM vouchers_head
        WHERE company_id IS NULL OR fy_id IS NULL
    """).fetchall()
    if vouchers_missing_ctx:
        issues.append({
            "category": "Vouchers",
            "severity": "WARNING",
            "code": "VOUCHERS_MISSING_COMPANY_OR_FY",
            "message": "Some vouchers are missing company or financial year tagging.",
            "count": len(vouchers_missing_ctx),
            "examples": [f"V#{r['v_id']} {r['v_type']} on {r['v_date']}" for r in vouchers_missing_ctx[:5]],
        })
    invoices_missing_ctx = con.execute("""
        SELECT inv_id,inv_date,customer
        FROM sale_invoice_head
        WHERE company_id IS NULL OR fy_id IS NULL
    """).fetchall()
    if invoices_missing_ctx:
        issues.append({
            "category": "Sales Invoices",
            "severity": "WARNING",
            "code": "INVOICES_MISSING_COMPANY_OR_FY",
            "message": "Some sale invoices are missing company or financial year tagging.",
            "count": len(invoices_missing_ctx),
            "examples": [f"Invoice #{r['inv_id']} {r['customer']} on {r['inv_date']}" for r in invoices_missing_ctx[:5]],
        })
    grn_missing_ctx = con.execute("""
        SELECT grn_id,grn_date,supplier
        FROM grn_head
        WHERE company_id IS NULL OR fy_id IS NULL
    """).fetchall()
    if grn_missing_ctx:
        issues.append({
            "category": "GRN",
            "severity": "WARNING",
            "code": "GRN_MISSING_COMPANY_OR_FY",
            "message": "Some GRNs are missing company or financial year tagging.",
            "count": len(grn_missing_ctx),
            "examples": [f"GRN #{r['grn_id']} {r['supplier']} on {r['grn_date']}" for r in grn_missing_ctx[:5]],
        })
    return issues

def ensure_notifications_table(con):
    con.execute("""
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

def ensure_daily_rates_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_rates(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          rate_date TEXT,
          item_id INTEGER,
          sale_rate REAL,
          purchase_rate REAL
        )
    """)
    try:
        con.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_rates_date_item
            ON daily_rates(rate_date,item_id)
        """)
    except sqlite3.OperationalError:
        pass

def ensure_account_family_columns(con):
    for stmt in [
        "ALTER TABLE acc_mast ADD COLUMN print_name TEXT",
        "ALTER TABLE acc_mast ADD COLUMN parent_acc_id INTEGER",
        "ALTER TABLE acc_mast ADD COLUMN tds_section TEXT",
        "ALTER TABLE acc_mast ADD COLUMN tds_rate REAL"
    ]:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass

def ensure_company_core(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS company_mast(
          company_id INTEGER PRIMARY KEY AUTOINCREMENT,
          company_name TEXT,
          short_name TEXT,
          group_name TEXT,
          gstin TEXT,
          address TEXT,
          city TEXT,
          state TEXT,
          company_logo TEXT,
          app_logo TEXT
        )
    """)
    for stmt in [
        "ALTER TABLE company_mast ADD COLUMN group_name TEXT",
        "ALTER TABLE company_mast ADD COLUMN company_logo TEXT",
        "ALTER TABLE company_mast ADD COLUMN app_logo TEXT",
        "ALTER TABLE sale_orders ADD COLUMN company_id INTEGER",
        "ALTER TABLE sale_orders ADD COLUMN division_id INTEGER",
        "ALTER TABLE sale_orders ADD COLUMN fy_id INTEGER",
        "ALTER TABLE purchase_orders ADD COLUMN company_id INTEGER",
        "ALTER TABLE purchase_orders ADD COLUMN division_id INTEGER",
        "ALTER TABLE purchase_orders ADD COLUMN fy_id INTEGER",
        "ALTER TABLE loading_advice_head ADD COLUMN company_id INTEGER",
        "ALTER TABLE loading_advice_head ADD COLUMN division_id INTEGER",
        "ALTER TABLE loading_advice_head ADD COLUMN fy_id INTEGER",
        "ALTER TABLE sale_invoice_head ADD COLUMN company_id INTEGER",
        "ALTER TABLE sale_invoice_head ADD COLUMN division_id INTEGER",
        "ALTER TABLE sale_invoice_head ADD COLUMN fy_id INTEGER",
        "ALTER TABLE grn_head ADD COLUMN company_id INTEGER",
        "ALTER TABLE grn_head ADD COLUMN division_id INTEGER",
        "ALTER TABLE grn_head ADD COLUMN fy_id INTEGER",
        "ALTER TABLE vouchers_head ADD COLUMN company_id INTEGER",
        "ALTER TABLE vouchers_head ADD COLUMN division_id INTEGER",
        "ALTER TABLE vouchers_head ADD COLUMN fy_id INTEGER"
    ]:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass
    con.execute("""
        CREATE TABLE IF NOT EXISTS division_mast(
          division_id INTEGER PRIMARY KEY AUTOINCREMENT,
          company_id INTEGER,
          division_name TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS financial_years(
          fy_id INTEGER PRIMARY KEY AUTOINCREMENT,
          company_id INTEGER,
          fy_name TEXT,
          start_date TEXT,
          end_date TEXT
        )
    """)
    row = con.execute("SELECT company_id FROM company_mast ORDER BY company_id LIMIT 1").fetchone()
    if not row:
        con.execute(
            "INSERT INTO company_mast(company_name,short_name,city,state) VALUES (?,?,?,?)",
            ("Default Company","DEFAULT","",""),
        )
    company_id = con.execute("SELECT company_id FROM company_mast ORDER BY company_id LIMIT 1").fetchone()[0]
    today_str = today()
    y = int(today_str[:4])
    m = int(today_str[5:7])
    fy_year = y if m >= 4 else y - 1
    fy_start = f"{fy_year}-04-01"
    fy_end = f"{fy_year+1}-03-31"
    row_fy = con.execute(
        "SELECT fy_id FROM financial_years WHERE company_id=? AND start_date=? AND end_date=?",
        (company_id, fy_start, fy_end),
    ).fetchone()
    if not row_fy:
        con.execute(
            "INSERT INTO financial_years(company_id,fy_name,start_date,end_date) VALUES (?,?,?,?)",
            (company_id, f"{fy_year}-{str(fy_year+1)[-2:]}", fy_start, fy_end),
        )

def ensure_ai_settings_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS ai_settings(
          id INTEGER PRIMARY KEY CHECK (id=1),
          default_provider TEXT,
          gemini_key TEXT
        )
    """)
    try:
        con.execute("ALTER TABLE ai_settings ADD COLUMN gemini_key TEXT")
    except sqlite3.OperationalError:
        pass
    row = con.execute("SELECT id FROM ai_settings WHERE id=1").fetchone()
    if not row:
        con.execute("INSERT INTO ai_settings(id,default_provider) VALUES (1,?)", ("auto",))

def ensure_field_config_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS field_config(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          form_name TEXT,
          field_name TEXT,
          required INTEGER DEFAULT 0,
          tab_index INTEGER
        )
    """)
    try:
        con.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_field_config_form_field
            ON field_config(form_name,field_name)
        """)
    except sqlite3.OperationalError:
        pass

def load_field_config(con, form_name:str):
    ensure_field_config_table(con)
    rows = con.execute("""
        SELECT field_name,required,tab_index
        FROM field_config
        WHERE form_name=?
    """, (form_name,)).fetchall()
    cfg = {}
    for r in rows:
        cfg[r["field_name"]] = {
            "required": bool(r["required"]),
            "tab_index": r["tab_index"],
        }
    return cfg

def get_routes_catalog():
    return [
        {"title": "Dashboard", "path": "/dashboard", "section": "Dashboard"},
        {"title": "Sale Order", "path": "/sale", "section": "Sales"},
        {"title": "Sale Invoice", "path": "/sale_invoice", "section": "Sales"},
        {"title": "Purchase Order", "path": "/purchase", "section": "Purchase"},
        {"title": "Loading Advice", "path": "/loading_advice", "section": "Purchase"},
        {"title": "Item Ledger", "path": "/item_ledger", "section": "Inventory"},
        {"title": "Purchase Pipeline", "path": "/purchase_pipeline", "section": "Inventory"},
        {"title": "Chart of Accounts", "path": "/chart_of_accounts", "section": "Accounts"},
        {"title": "Ledger Voucher Entry", "path": "/ledger_vouchers", "section": "Accounts"},
        {"title": "Trial Balance", "path": "/trial_balance", "section": "Accounts"},
        {"title": "Profit & Loss", "path": "/pl", "section": "Accounts"},
        {"title": "Balance Sheet", "path": "/balance_sheet", "section": "Accounts"},
        {"title": "GST Report", "path": "/gst_report", "section": "Reports"},
        {"title": "Customer/Supplier Report", "path": "/cust_purch_report", "section": "Reports"},
        {"title": "TDS Form & Rules", "path": "/tds_report", "section": "Reports"},
        {"title": "Audit Log", "path": "/audit_log", "section": "Admin"},
        {"title": "Self Audit", "path": "/self_audit", "section": "Admin"},
        {"title": "Daily Rates", "path": "/rates", "section": "Masters"},
        {"title": "Reports Overview", "path": "/control_report", "section": "Reports"},
        {"title": "AI Assistant", "path": "/ai", "section": "Tools"},
        {"title": "User Settings", "path": "/user_settings", "section": "Admin"},
    ]

def ensure_grn_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS grn_head(
          grn_id INTEGER PRIMARY KEY AUTOINCREMENT,
          la_id INTEGER,
          grn_date TEXT,
          supplier TEXT,
          total REAL
        )
    """)
    con.execute("""
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

def ensure_sale_return_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS sale_return_head(
          sr_id INTEGER PRIMARY KEY AUTOINCREMENT,
          inv_id INTEGER,
          sr_date TEXT,
          customer TEXT,
          total REAL,
          reason TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS sale_return_body(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          sr_id INTEGER,
          item_name TEXT,
          section TEXT,
          qty REAL,
          rate REAL,
          amount REAL
        )
    """)

def ensure_purchase_return_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS purchase_return_head(
          pr_id INTEGER PRIMARY KEY AUTOINCREMENT,
          grn_id INTEGER,
          pr_date TEXT,
          supplier TEXT,
          total REAL,
          reason TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS purchase_return_body(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          pr_id INTEGER,
          item_name TEXT,
          section TEXT,
          qty REAL,
          rate REAL,
          amount REAL
        )
    """)

def ensure_user_settings_columns(con):
    for stmt in [
        "ALTER TABLE users_mast ADD COLUMN mfa_required INTEGER DEFAULT 0",
        "ALTER TABLE users_mast ADD COLUMN default_theme TEXT",
        "ALTER TABLE users_mast ADD COLUMN default_density TEXT",
    ]:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass

def ensure_company_context(con):
    ensure_company_core(con)
    row = con.execute("SELECT company_id,company_name FROM company_mast ORDER BY company_id LIMIT 1").fetchone()
    if row and "company_id" not in session:
        session["company_id"] = row["company_id"]
        session["company_name"] = row["company_name"]
    company_id = session.get("company_id")
    if company_id:
        fy = con.execute("""
            SELECT fy_id,fy_name,start_date,end_date
            FROM financial_years
            WHERE company_id=?
            ORDER BY start_date
        """, (company_id,)).fetchone()
        if fy:
            session.setdefault("fy_id", fy["fy_id"])
            session.setdefault("fy_name", fy["fy_name"])
            session.setdefault("fy_start", fy["start_date"])
            session.setdefault("fy_end", fy["end_date"])

def audit(con, table_name, action, pk="", details=""):
    ensure_audit_table(con)
    user_code = session.get("user","")
    company_id = session.get("company_id")
    division_id = session.get("division_id")
    fy_id = session.get("fy_id")
    ts = datetime.now().isoformat(sep=" ", timespec="seconds")
    con.execute(
        """
        INSERT INTO audit_log(log_time,user_code,table_name,action,pk,details,company_id,division_id,fy_id)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (ts, user_code, table_name, action, str(pk), details, company_id, division_id, fy_id),
    )

def send_sms(mobile:str, message:str):
    if not mobile:
        return
    print("SMS", mobile, message)

@app.before_request
def enforce_login():
    open_endpoints = {
        "login",
        "logout",
        "static",
        "so_preview", "so_accept", "so_decline",
        "la_preview", "la_accept", "la_decline",
        "po_preview", "po_accept", "po_decline",
        "two_factor",
        "public_rates",
    }
    if request.endpoint in open_endpoints or request.endpoint is None:
        return
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_company_context(con)
    now = int(time.time())
    last = session.get("last_seen", now)
    if now - last > 1800:
        session.clear()
        return redirect("/")
    session["last_seen"] = now

# ---------------- LOGIN ----------------
@app.route("/", methods=["GET","POST"])
def login():
    if request.method=="POST":
        con = db()
        ensure_user_settings_columns(con)
        ensure_company_core(con)
        u = con.execute(
            "SELECT * FROM users_mast WHERE username=?",
            (request.form["username"],)
        ).fetchone()
        if u and check_password_hash(u["password_hash"], request.form["password"]):
            require_mfa = 0
            if "mfa_required" in u.keys():
                require_mfa = u["mfa_required"] or 0
            if require_mfa:
                code = f"{secrets.randbelow(1000000):06d}"
                session["otp_user"] = u["user_code"]
                session["otp_role"] = u["role"]
                session["otp_code"] = code
                session["otp_created"] = int(time.time())
                mobile = u["mobile"] if "mobile" in u.keys() else None
                session["otp_mobile"] = mobile
                send_sms(mobile, f"Your ERP login code is {code}")
                return redirect("/two_factor")
            session["user"] = u["user_code"]
            session["role"] = u["role"]
            session["default_theme"] = u["default_theme"] if "default_theme" in u.keys() else None
            session["default_density"] = u["default_density"] if "default_density" in u.keys() else None
            session["last_seen"] = int(time.time())
            return redirect("/dashboard")
    return render_template("login.html")

@app.route("/user_settings", methods=["GET","POST"])
def user_settings():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_user_settings_columns(con)
    u = con.execute("""
        SELECT user_code,username,role,
               COALESCE(mfa_required,0) mfa_required,
               COALESCE(default_theme,'') default_theme,
               COALESCE(default_density,'') default_density
        FROM users_mast
        WHERE user_code=?
    """, (session["user"],)).fetchone()
    if not u:
        return redirect("/dashboard")
    if request.method == "POST":
        mfa_required = 1 if request.form.get("mfa_required") == "1" else 0
        default_theme = request.form.get("default_theme","").strip() or None
        default_density = request.form.get("default_density","").strip() or None
        con.execute("""
            UPDATE users_mast
            SET mfa_required=?, default_theme=?, default_density=?
            WHERE user_code=?
        """, (mfa_required, default_theme, default_density, session["user"]))
        con.commit()
        session["default_theme"] = default_theme
        session["default_density"] = default_density
        flash("User settings updated","success")
        return redirect("/user_settings")
    return render_template("user_settings.html", user=u)

@app.route("/company_context", methods=["GET","POST"])
def company_context():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_company_core(con)
    companies = con.execute("SELECT company_id,company_name FROM company_mast ORDER BY company_name").fetchall()
    current_company_id = session.get("company_id")
    if not current_company_id and companies:
        current_company_id = companies[0]["company_id"]
    if request.method == "POST":
        company_id = int(request.form.get("company_id") or current_company_id or 0)
        division_id_raw = request.form.get("division_id") or ""
        fy_id_raw = request.form.get("fy_id") or ""
        session["company_id"] = company_id
        row_c = con.execute("SELECT company_name FROM company_mast WHERE company_id=?", (company_id,)).fetchone()
        session["company_name"] = row_c["company_name"] if row_c else ""
        if fy_id_raw:
            fy_id = int(fy_id_raw)
            fy = con.execute("SELECT fy_id,fy_name,start_date,end_date FROM financial_years WHERE fy_id=?", (fy_id,)).fetchone()
            if fy:
                session["fy_id"] = fy["fy_id"]
                session["fy_name"] = fy["fy_name"]
                session["fy_start"] = fy["start_date"]
                session["fy_end"] = fy["end_date"]
        division_id = int(division_id_raw) if division_id_raw.isdigit() else None
        session["division_id"] = division_id
        return redirect("/company_context")
    current_company_id = session.get("company_id") or (companies[0]["company_id"] if companies else None)
    divisions = []
    years = []
    current_division_id = session.get("division_id")
    current_division_name = None
    current_fy_id = session.get("fy_id")
    current_fy_name = session.get("fy_name")
    if current_company_id:
        divisions = con.execute("SELECT division_id,division_name FROM division_mast WHERE company_id=? ORDER BY division_name", (current_company_id,)).fetchall()
        years = con.execute("SELECT fy_id,fy_name,start_date,end_date FROM financial_years WHERE company_id=? ORDER BY start_date", (current_company_id,)).fetchall()
        if current_division_id:
            row_d = con.execute("SELECT division_name FROM division_mast WHERE division_id=?", (current_division_id,)).fetchone()
            current_division_name = row_d["division_name"] if row_d else None
    current_company_name = session.get("company_name","")
    return render_template(
        "company_context.html",
        companies=companies,
        divisions=divisions,
        years=years,
        current_company_id=current_company_id,
        current_company_name=current_company_name,
        current_division_id=current_division_id,
        current_division_name=current_division_name,
        current_fy_id=current_fy_id,
        current_fy_name=current_fy_name,
    )

@app.route("/company_master", methods=["GET","POST"])
def company_master():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_company_core(con)
    companies = con.execute("SELECT company_id,company_name FROM company_mast ORDER BY company_name").fetchall()
    current_company_id = request.args.get("company_id", type=int)
    if not current_company_id and companies:
        current_company_id = companies[0]["company_id"]
    if request.method == "POST":
        action = request.form.get("action","")
        if action == "new_company":
            con.execute(
                "INSERT INTO company_mast(company_name,short_name) VALUES (?,?)",
                ("New Company","NEW"),
            )
            con.commit()
            return redirect("/company_master")
        if action == "save_company":
            cid = request.form.get("company_id")
            company_name = request.form.get("company_name","").strip()
            short_name = request.form.get("short_name","").strip()
            group_name = request.form.get("group_name","").strip()
            gstin = request.form.get("gstin","").strip()
            address = request.form.get("address","").strip()
            city = request.form.get("city","").strip()
            state = request.form.get("state","").strip()
            company_logo = request.form.get("company_logo","").strip()
            app_logo = request.form.get("app_logo","").strip()
            if cid:
                con.execute(
                    """
                    UPDATE company_mast
                    SET company_name=?,short_name=?,group_name=?,gstin=?,address=?,city=?,state=?,company_logo=?,app_logo=?
                    WHERE company_id=?
                    """,
                    (company_name,short_name,group_name,gstin,address,city,state,company_logo,app_logo,cid),
                )
            else:
                con.execute(
                    """
                    INSERT INTO company_mast
                    (company_name,short_name,group_name,gstin,address,city,state,company_logo,app_logo)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (company_name,short_name,group_name,gstin,address,city,state,company_logo,app_logo),
                )
            con.commit()
            return redirect("/company_master")
        if action == "add_year":
            cid = request.form.get("company_id")
            fy_name = request.form.get("fy_name","").strip()
            start_date = request.form.get("start_date","").strip()
            end_date = request.form.get("end_date","").strip()
            if cid and fy_name and start_date and end_date:
                con.execute(
                    """
                    INSERT INTO financial_years(company_id,fy_name,start_date,end_date)
                    VALUES (?,?,?,?)
                    """,
                    (cid,fy_name,start_date,end_date),
                )
                con.commit()
            return redirect(f"/company_master?company_id={cid}")
    company = None
    years = []
    if current_company_id:
        company = con.execute(
            """
            SELECT company_id,company_name,short_name,group_name,gstin,address,city,state,company_logo,app_logo
            FROM company_mast
            WHERE company_id=?
            """,
            (current_company_id,),
        ).fetchone()
        years = con.execute(
            "SELECT fy_id,fy_name,start_date,end_date FROM financial_years WHERE company_id=? ORDER BY start_date",
            (current_company_id,),
        ).fetchall()
    return render_template(
        "company_master.html",
        companies=companies,
        company=company or {},
        years=years,
        current_company_id=current_company_id,
    )

@app.route("/rates", methods=["GET","POST"])
def rates():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_daily_rates_table(con)
    if request.method == "POST":
        rate_date = request.form.get("date") or today()
        item_id = int(request.form.get("item_id"))
        sale_rate_raw = request.form.get("sale_rate","").strip()
        purchase_rate_raw = request.form.get("purchase_rate","").strip()
        sale_rate = float(sale_rate_raw) if sale_rate_raw else None
        purchase_rate = float(purchase_rate_raw) if purchase_rate_raw else None
        con.execute("""
            INSERT INTO daily_rates(rate_date,item_id,sale_rate,purchase_rate)
            VALUES (?,?,?,?)
            ON CONFLICT(rate_date,item_id)
            DO UPDATE SET
              sale_rate=excluded.sale_rate,
              purchase_rate=excluded.purchase_rate
        """, (rate_date, item_id, sale_rate, purchase_rate))
        con.commit()
        flash("Rates updated","success")
        return redirect("/rates")
    items = con.execute("SELECT item_id,item_name FROM item_mast ORDER BY item_name").fetchall()
    today_str = today()
    rows = con.execute("""
        SELECT d.rate_date,i.item_name,d.sale_rate,d.purchase_rate
        FROM daily_rates d
        JOIN item_mast i ON d.item_id=i.item_id
        WHERE d.rate_date=?
        ORDER BY i.item_name
    """, (today_str,)).fetchall()
    return render_template("rates.html", items=items, rows=rows, today=today_str)

@app.route("/public_rates")
def public_rates():
    con = db()
    ensure_daily_rates_table(con)
    rate_date = request.args.get("date") or today()
    rows = con.execute("""
        SELECT i.item_name,d.sale_rate,d.purchase_rate
        FROM daily_rates d
        JOIN item_mast i ON d.item_id=i.item_id
        WHERE d.rate_date=?
        ORDER BY i.item_name
    """, (rate_date,)).fetchall()
    return render_template("public_rates.html", rate_date=rate_date, rows=rows)

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

@app.route("/two_factor", methods=["GET","POST"])
def two_factor():
    if "otp_user" not in session:
        return redirect("/")
    wa_link = None
    if session.get("otp_code"):
        msg = f"Your ERP login code is {session['otp_code']}"
        wa_link = f"https://wa.me/?text={quote_plus(msg)}"
    if request.method == "POST":
        code = request.form.get("code","").strip()
        expected = session.get("otp_code")
        created = session.get("otp_created", 0)
        if expected and code == expected and int(time.time()) - created <= 300:
            session["user"] = session.pop("otp_user")
            session["role"] = session.pop("otp_role", None)
            session.pop("otp_code", None)
            session.pop("otp_created", None)
            session["last_seen"] = int(time.time())
            return redirect("/dashboard")
    return render_template("two_factor.html", wa_link=wa_link)

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
    invoice_months_total = con.execute("""
        SELECT substr(inv_date,1,7) ym, COALESCE(SUM(total),0) total
        FROM sale_invoice_head GROUP BY ym ORDER BY ym
    """).fetchall()
    invoice_months_count = con.execute("""
        SELECT substr(l.la_date,1,7) ym, COALESCE(SUM(b.qty),0) qty
        FROM sale_invoice_body b
        JOIN sale_invoice_head h ON b.inv_id=h.inv_id
        JOIN loading_advice_head l ON h.la_id=l.la_id
        WHERE h.la_id IS NOT NULL
        GROUP BY ym ORDER BY ym
    """).fetchall()
    lm = {r[0]: r[1] for r in loading_months}
    im_cnt = {r[0]: r[1] for r in invoice_months_count}
    la_labels_month = sorted(set(lm.keys()) | set(im_cnt.keys()))
    la_loading_month = [lm.get(k, 0) for k in la_labels_month]
    la_invoice_month_count = [im_cnt.get(k, 0) for k in la_labels_month]
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
    invoice_days_total = con.execute("""
        SELECT inv_date d, COALESCE(SUM(total),0) total
        FROM sale_invoice_head GROUP BY d ORDER BY d
    """).fetchall()
    invoice_years_total = con.execute("""
        SELECT substr(inv_date,1,4) y, COALESCE(SUM(total),0) total
        FROM sale_invoice_head GROUP BY y ORDER BY y
    """).fetchall()
    invoice_days_count = con.execute("""
        SELECT l.la_date d, COALESCE(SUM(b.qty),0) qty
        FROM sale_invoice_body b
        JOIN sale_invoice_head h ON b.inv_id=h.inv_id
        JOIN loading_advice_head l ON h.la_id=l.la_id
        WHERE h.la_id IS NOT NULL
        GROUP BY d ORDER BY d
    """).fetchall()
    invoice_years_count = con.execute("""
        SELECT substr(l.la_date,1,4) y, COALESCE(SUM(b.qty),0) qty
        FROM sale_invoice_body b
        JOIN sale_invoice_head h ON b.inv_id=h.inv_id
        JOIN loading_advice_head l ON h.la_id=l.la_id
        WHERE h.la_id IS NOT NULL
        GROUP BY y ORDER BY y
    """).fetchall()
    ld = {r[0]: r[1] for r in loading_days}
    idd_cnt = {r[0]: r[1] for r in invoice_days_count}
    la_labels_day = sorted(set(ld.keys()) | set(idd_cnt.keys()))
    la_loading_day = [ld.get(k, 0) for k in la_labels_day]
    la_invoice_day_count = [idd_cnt.get(k, 0) for k in la_labels_day]
    ly = {r[0]: r[1] for r in loading_years}
    iy_cnt = {r[0]: r[1] for r in invoice_years_count}
    la_labels_year = sorted(set(ly.keys()) | set(iy_cnt.keys()))
    la_loading_year = [ly.get(k, 0) for k in la_labels_year]
    la_invoice_year_count = [iy_cnt.get(k, 0) for k in la_labels_year]
    pending_pos = con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,i.item_name,p.qty,COALESCE(p.supplied_qty,0) supplied,
               p.rate,(p.qty-COALESCE(p.supplied_qty,0)) pending
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE (p.qty-COALESCE(p.supplied_qty,0))>0
        ORDER BY p.po_date DESC
    """).fetchall()
    pending_sos = con.execute("""
        SELECT s.so_id,s.so_date,a.acc_name,i.item_name,s.qty,COALESCE(s.supplied_qty,0) supplied,
               s.rate,(s.qty-COALESCE(s.supplied_qty,0)) pending
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE (s.qty-COALESCE(s.supplied_qty,0))>0
        ORDER BY s.so_date DESC
    """).fetchall()
    suppliers = [r[0] for r in con.execute("""
        SELECT DISTINCT a.acc_name
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE (p.qty-COALESCE(p.supplied_qty,0))>0
        ORDER BY a.acc_name
    """).fetchall()]
    customers = [r[0] for r in con.execute("""
        SELECT DISTINCT a.acc_name
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE (s.qty-COALESCE(s.supplied_qty,0))>0
        ORDER BY a.acc_name
    """).fetchall()]
    return render_template("dashboard.html",
        kpi=kpi,
        sale_months=sale_months,
        purchase_months=purchase_months,
        loading_months=loading_months,
        sale_days=sale_days,
        sale_years=sale_years,
        purchase_days=purchase_days,
        purchase_years=purchase_years,
        la_labels_month=la_labels_month,
        la_loading_month=la_loading_month,
        la_invoice_month_count=la_invoice_month_count,
        la_labels_day=la_labels_day,
        la_loading_day=la_loading_day,
        la_invoice_day_count=la_invoice_day_count,
        la_labels_year=la_labels_year,
        la_loading_year=la_loading_year,
        la_invoice_year_count=la_invoice_year_count,
        pending_pos=pending_pos,
        suppliers=suppliers,
        pending_sos=pending_sos,
        customers=customers
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

        cur=con.execute("""
        INSERT INTO sale_orders
        (so_date,acc_id,item_id,qty,rate,amount,user_code,company_id,division_id,fy_id)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,(request.form["date"],
             request.form["acc_id"],
             request.form["item_id"],
             qty,rate,amt,session["user"],
             session.get("company_id"),
             session.get("division_id"),
             session.get("fy_id")))
        so_id = cur.lastrowid
        audit(con, "sale_orders", "INSERT", so_id, f"amount={amt}")
        con.commit()
        flash("Sale Order saved successfully","success")
        return redirect("/sale")

    try:
        schemes = con.execute("""
            SELECT name,start_date,end_date,min_qty,reward_text
            FROM sale_schemes
            WHERE start_date<=? AND end_date>=?
            ORDER BY min_qty
        """, (today(), today())).fetchall()
    except sqlite3.OperationalError:
        schemes = []

    customers=con.execute("""
        SELECT acc_id,acc_name,mobile,city,state
        FROM acc_mast
        WHERE acc_type='Customer'
        ORDER BY acc_name
    """).fetchall()

    items=con.execute("SELECT * FROM item_mast ORDER BY item_name").fetchall()

    rows=con.execute("""
    SELECT so_id,so_date,acc_name,item_name,qty,rate,amount,status,
           acc_mast.mobile,
           COALESCE(whatsapp_sent,0),
           (qty-COALESCE(supplied_qty,0)) AS balance
    FROM sale_orders
    JOIN acc_mast ON sale_orders.acc_id=acc_mast.acc_id
    JOIN item_mast ON sale_orders.item_id=item_mast.item_id
    ORDER BY so_id DESC
    """).fetchall()

    field_config = load_field_config(con, "sale_order")

    return render_template("sale_order.html",
        customers=customers,
        items=items,
        rows=rows,
        today=today(),
        schemes=schemes,
        field_config=field_config
    )

@app.route("/sale_invoice")
def sale_invoice_page():
    if "user" not in session:
        return redirect("/")
    con = db()
    invoices = con.execute("""
        SELECT h.inv_id,h.inv_date,h.customer,
               COALESCE(SUM(b.qty),0) qty,
               h.total
        FROM sale_invoice_head h
        LEFT JOIN sale_invoice_body b ON h.inv_id=b.inv_id
        GROUP BY h.inv_id
        ORDER BY h.inv_id DESC
    """).fetchall()
    return render_template(
        "sale_invoice.html",
        today=today(),
        customer="",
        rows=[],
        invoices=invoices,
        companies=[],
        default_company_id=None,
        gst_mode="intra",
        party_state="",
        home_state="GJ",
    )
 

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

        cur=con.execute("""
        INSERT INTO purchase_orders
        (po_date,acc_id,item_id,qty,rate,amount,user_code,company_id,division_id,fy_id)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,(request.form["date"],
             request.form["acc_id"],
             request.form["item_id"],
             qty,rate,amt,session["user"],
             session.get("company_id"),
             session.get("division_id"),
             session.get("fy_id")))
        po_id = cur.lastrowid
        audit(con, "purchase_orders", "INSERT", po_id, f"amount={amt}")
        con.commit()
        flash("Purchase Order saved successfully","success")
        return redirect("/purchase")

    suppliers=con.execute("""
        SELECT acc_id,acc_name,mobile,city,state
        FROM acc_mast
        WHERE acc_type='Supplier'
        ORDER BY acc_name
    """).fetchall()

    items=con.execute("SELECT * FROM item_mast ORDER BY item_name").fetchall()

    rows=con.execute("""
        SELECT po_id,po_date,acc_name,item_name,qty,rate,amount,status,acc_mast.mobile,COALESCE(whatsapp_sent,0)
        FROM purchase_orders
        JOIN acc_mast ON purchase_orders.acc_id=acc_mast.acc_id
        JOIN item_mast ON purchase_orders.item_id=item_mast.item_id
        ORDER BY po_id DESC
    """).fetchall()

    field_config = load_field_config(con, "purchase_order")

    return render_template("purchase_order.html",
        suppliers=suppliers,
        items=items,
        rows=rows,
        today=today(),
        field_config=field_config
    )

def notify_whatsapp(con, kind, ref_id, party_name, mobile, message):
    ensure_notifications_table(con)
    user_code = session.get("user","")
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    con.execute("""
        INSERT INTO notifications
        (created_at,channel,direction,kind,ref_id,party_name,mobile,message,status,user_code)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (now,"WHATSAPP","OUT","SO" if kind=="SO" else "PO" if kind=="PO" else kind,ref_id,party_name,mobile,message,"SENT",user_code))

# ---------------- WHATSAPP SEND MARKERS ----------------
@app.route("/so_whatsapp/<int:so_id>")
def so_whatsapp(so_id:int):
    if "user" not in session:
        return redirect("/")
    con = db()
    row = con.execute("""
        SELECT s.so_id,s.so_date,a.acc_name,a.mobile,i.item_name,s.qty,s.rate,s.amount
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE s.so_id=?
    """,(so_id,)).fetchone()
    if not row:
        abort(404)
    con.execute("UPDATE sale_orders SET whatsapp_sent=1 WHERE so_id=?", (so_id,))
    try:
        schemes = con.execute("""
            SELECT name,min_qty,reward_text
            FROM sale_schemes
            WHERE start_date<=? AND end_date>=?
            ORDER BY min_qty
        """, (row[1], row[1])).fetchall()
    except sqlite3.OperationalError:
        schemes = []
    preview_url = url_for('so_preview', so_id=so_id, token=so_token(so_id), _external=True)
    msg = f"Sale Order #{row[0]} for {row[2]} ({row[4]}) Qty {row[5]} Rate {row[6]} Amount {row[7]}. Review and accept: {preview_url}"
    if schemes:
        parts = []
        for s in schemes:
            parts.append(f"{s[0]}: Qty {s[1]}+ Reward {s[2]}")
        msg = msg + " Schemes: " + "; ".join(parts)
    phone = row[3] or ""
    notify_whatsapp(con, "SO", row[0], row[2], phone, msg)
    con.commit()
    wa = f"https://wa.me/{phone}?text={quote_plus(msg)}" if phone else f"https://wa.me/?text={quote_plus(msg)}"
    return redirect(wa)

@app.route("/po_whatsapp/<int:po_id>")
def po_whatsapp(po_id:int):
    if "user" not in session:
        return redirect("/")
    con = db()
    row = con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,a.mobile,i.item_name,p.qty,p.rate,p.amount
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE p.po_id=?
    """,(po_id,)).fetchone()
    if not row:
        abort(404)
    con.execute("UPDATE purchase_orders SET whatsapp_sent=1 WHERE po_id=?", (po_id,))
    preview_url = url_for('po_preview', po_id=po_id, token=po_token(po_id), _external=True)
    msg = f"Purchase Order #{row[0]} for {row[2]} ({row[4]}) Qty {row[5]} Rate {row[6]} Amount {row[7]}. Review and accept: {preview_url}"
    phone = row[3] or ""
    notify_whatsapp(con, "PO", row[0], row[2], phone, msg)
    con.commit()
    wa = f"https://wa.me/{phone}?text={quote_plus(msg)}" if phone else f"https://wa.me/?text={quote_plus(msg)}"
    return redirect(wa)

# ---------------- ADD ACCOUNT ----------------
# ---------------- ADD ACCOUNT (Customer / Supplier) ----------------
@app.route("/add_account", methods=["POST"])
def add_account():

    if "user" not in session:
        return redirect("/")

    con = db()
    ensure_account_family_columns(con)

    name   = request.form["name"].strip()
    atype  = request.form["type"]   # Customer / Supplier
    mobile = request.form.get("mobile")
    pan    = request.form.get("pan")
    gstin  = request.form.get("gstin")
    address= request.form.get("address")
    city   = request.form.get("city")
    state  = request.form.get("state")
    print_name = (request.form.get("print_name") or "").strip() or name
    parent_raw = (request.form.get("parent_acc_id") or "").strip()
    parent_acc_id = int(parent_raw) if parent_raw.isdigit() else None

    # ---------------- DUPLICATE CHECK ----------------
    row = con.execute(
        "SELECT acc_id FROM acc_mast WHERE acc_name=?",
        (name,)
    ).fetchone()

    if row:
        return redirect(request.referrer)

    # ---------------- FIND GROUP ----------------
    if atype == "Customer":
        grp_row = con.execute("""
            SELECT group_id FROM acc_groups 
            WHERE group_name='Current Assets'
        """).fetchone()
    else:
        grp_row = con.execute("""
            SELECT group_id FROM acc_groups 
            WHERE group_name='Current Liabilities'
        """).fetchone()

    group_id = grp_row[0]

    # ---------------- CREATE LEDGER ----------------
    cur = con.execute("""
        INSERT INTO ledgers(ledger_name,group_id)
        VALUES (?,?)
    """,(name,group_id))

    ledger_id = cur.lastrowid

    # ---------------- INSERT ACCOUNT MASTER ----------------
    cur_acc = con.execute("""
        INSERT INTO acc_mast
        (acc_name,acc_type,ledger_id,mobile,pan,gstin,address,city,state,print_name,parent_acc_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """,(name,atype,ledger_id,mobile,pan,gstin,address,city,state,print_name,parent_acc_id))
    acc_id = cur_acc.lastrowid
    audit(con, "acc_mast", "INSERT", acc_id, f"type={atype}")
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
        cur = con.execute(
            "INSERT INTO item_mast(item_name) VALUES(?)",
            (request.form["name"],)
        )
        item_id = cur.lastrowid
        audit(con, "item_mast", "INSERT", item_id, "")
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
        (la_date,vehicle_no,user_code,company_id,division_id,fy_id)
        VALUES (?,?,?,?,?,?)
        """,(request.form["date"],
             request.form["vehicle"],
             session["user"],
             session.get("company_id"),
             session.get("division_id"),
             session.get("fy_id")))
        la_id=cur.lastrowid

        so_ids=request.form.getlist("so_id")
        po_ids=request.form.getlist("po_id")
        sections=request.form.getlist("section")
        batchs=request.form.getlist("batch")
        qtys=request.form.getlist("qty")
        customer_id = None
        supplier_id = None

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
            srow = con.execute("SELECT item_id,acc_id FROM sale_orders WHERE so_id=?", (so_id,)).fetchone()
            prow = con.execute("SELECT acc_id FROM purchase_orders WHERE po_id=?", (po_id,)).fetchone()
            if srow and prow:
                if customer_id is None:
                    customer_id = srow["acc_id"]
                if supplier_id is None:
                    supplier_id = prow["acc_id"]
                con.execute("""
                INSERT INTO item_ledger(la_id,la_date,item_id,so_id,po_id,customer_id,supplier_id,qty)
                VALUES (?,?,?,?,?,?,?,?)
                """,(la_id,request.form["date"],srow["item_id"],so_id,po_id,srow["acc_id"],prow["acc_id"],qty))

            con.execute("UPDATE sale_orders SET supplied_qty=supplied_qty+? WHERE so_id=?",
                        (qty,so_id))
            con.execute("UPDATE purchase_orders SET supplied_qty=supplied_qty+? WHERE po_id=?",
                        (qty,po_id))
            lock_sale_order(so_id)
            lock_purchase_order(po_id)
        if customer_id is not None or supplier_id is not None:
            con.execute(
                "UPDATE loading_advice_head SET customer_id=?, supplier_id=? WHERE la_id=?",
                (customer_id, supplier_id, la_id),
            )
        audit(con, "loading_advice_head", "INSERT", la_id, f"vehicle={request.form['vehicle']}")
        con.commit()
        flash(f"Loading Advice #{la_id} saved successfully","success")
        return redirect("/loading_advice")

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
        SELECT h.la_id,h.la_date,h.vehicle_no,
               COALESCE(h.whatsapp_sent,0),
               COALESCE(h.approval_status,''),
               cust.acc_name AS customer_name,
               sup.acc_name AS supplier_name
        FROM loading_advice_head h
        LEFT JOIN acc_mast cust ON h.customer_id=cust.acc_id
        LEFT JOIN acc_mast sup ON h.supplier_id=sup.acc_id
        ORDER BY h.la_id DESC
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
        today=today(),
        preselect_so_id=None
    )

@app.route("/loading_from_so/<int:so_id>")
def loading_from_so(so_id:int):
    if "user" not in session:
        return redirect("/")
    con = db()
    row = con.execute("""
        SELECT so_id,qty,COALESCE(supplied_qty,0) AS supplied
        FROM sale_orders
        WHERE so_id=?
    """, (so_id,)).fetchone()
    if not row:
        abort(404)
    balance = (row["qty"] or 0) - (row["supplied"] or 0)
    if balance <= 0:
        flash("Sale Order has no balance quantity for loading","warning")
        return redirect("/loading_advice")
    sales = con.execute("""
        SELECT s.so_id,s.so_date,a.acc_name,i.item_name,
               (s.qty-s.supplied_qty) AS bal,s.rate
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE (s.qty-s.supplied_qty)>0
        ORDER BY s.so_date
    """).fetchall()
    purchases = con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,i.item_name,
               (p.qty-p.supplied_qty) AS bal,p.rate
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE (p.qty-p.supplied_qty)>0
        ORDER BY p.po_date
    """).fetchall()
    las = con.execute("""
        SELECT h.la_id,h.la_date,h.vehicle_no,
               COALESCE(h.whatsapp_sent,0),
               COALESCE(h.approval_status,''),
               cust.acc_name AS customer_name,
               sup.acc_name AS supplier_name
        FROM loading_advice_head h
        LEFT JOIN acc_mast cust ON h.customer_id=cust.acc_id
        LEFT JOIN acc_mast sup ON h.supplier_id=sup.acc_id
        ORDER BY h.la_id DESC
    """).fetchall()
    la_rows = {}
    for la in las:
        la_rows[la["la_id"]] = con.execute("""
            SELECT section,qty,part
            FROM loading_advice_body
            WHERE la_id=?
        """, (la["la_id"],)).fetchall()
    return render_template("loading_advice.html",
        sales=sales,
        purchases=purchases,
        las=las,
        la_rows=la_rows,
        today=today(),
        preselect_so_id=so_id
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

@app.route("/la_preview/<int:la_id>", methods=["GET"])
def la_preview(la_id:int):
    token = request.args.get("token","")
    if token != la_token(la_id):
        abort(403)
    con = db()
    head = con.execute("""
        SELECT la_id,la_date,vehicle_no,COALESCE(approval_status,''),COALESCE(approval_note,'')
        FROM loading_advice_head WHERE la_id=?
    """,(la_id,)).fetchone()
    rows = con.execute("""
        SELECT section,qty,part FROM loading_advice_body WHERE la_id=?
    """,(la_id,)).fetchall()
    return render_template("la_preview.html", h=head, rows=rows)

@app.route("/loading_confirm/<int:la_id>", methods=["GET","POST"])
def loading_confirm(la_id):
    if "user" not in session:
        return redirect("/")
    con = db()
    head = con.execute("SELECT * FROM loading_advice_head WHERE la_id=?", (la_id,)).fetchone()
    if not head:
        return "Loading Advice not found"
    st = head["status"] if "status" in head.keys() else None
    if request.method == "POST":
        if st == "LOCKED":
            return "❌ Loading Advice Locked"
        vehicle = request.form.get("vehicle_no", head["vehicle_no"])
        con.execute("UPDATE loading_advice_head SET vehicle_no=? WHERE la_id=?", (vehicle, la_id))
        ids = request.form.getlist("id")
        qtys = request.form.getlist("qty")
        sections = request.form.getlist("section")
        for i in range(len(ids)):
            rid = int(ids[i])
            new_qty = float(qtys[i] or 0)
            sec = sections[i]
            old = con.execute("SELECT qty, so_id, po_id FROM loading_advice_body WHERE id=?", (rid,)).fetchone()
            if not old:
                continue
            delta = new_qty - float(old["qty"] or 0)
            con.execute("UPDATE loading_advice_body SET qty=?, section=? WHERE id=?", (new_qty, sec, rid))
            if delta != 0:
                con.execute("UPDATE sale_orders SET supplied_qty=COALESCE(supplied_qty,0)+? WHERE so_id=?", (delta, old["so_id"]))
                con.execute("UPDATE purchase_orders SET supplied_qty=COALESCE(supplied_qty,0)+? WHERE po_id=?", (delta, old["po_id"]))
        con.commit()
        flash("Loading confirmation saved","success")
        return redirect(url_for("loading_confirm", la_id=la_id))
    rows = con.execute("""
        SELECT b.id,
               b.section,
               b.qty,
               i.item_id,
               i.item_name,
               s.rate AS so_rate,
               p.rate AS po_rate,
               s.so_id,
               p.po_id
        FROM loading_advice_body b
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN purchase_orders p ON b.po_id=p.po_id
        JOIN item_mast i ON s.item_id=i.item_id
        WHERE b.la_id=?
        ORDER BY b.id
    """, (la_id,)).fetchall()
    parties = con.execute("""
        SELECT cust.acc_name AS customer, sup.acc_name AS supplier
        FROM loading_advice_head h
        LEFT JOIN acc_mast cust ON h.customer_id=cust.acc_id
        LEFT JOIN acc_mast sup ON h.supplier_id=sup.acc_id
        WHERE h.la_id=?
    """, (la_id,)).fetchone()
    rate_date = head["la_date"]
    enriched = []
    for r in rows:
        sale_rate = r["so_rate"] or 0
        purchase_rate = r["po_rate"] or 0
        dr = con.execute("""
            SELECT sale_rate,purchase_rate
            FROM daily_rates
            WHERE rate_date=? AND item_id=?
        """, (rate_date, r["item_id"])).fetchone()
        if dr:
            if dr["sale_rate"] is not None:
                sale_rate = dr["sale_rate"]
            if dr["purchase_rate"] is not None:
                purchase_rate = dr["purchase_rate"]
        enriched.append({
            "id": r["id"],
            "section": r["section"],
            "qty": r["qty"],
            "item_id": r["item_id"],
            "item_name": r["item_name"],
            "sale_rate": sale_rate,
            "purchase_rate": purchase_rate,
            "so_id": r["so_id"],
            "po_id": r["po_id"],
        })
    return render_template(
        "loading_confirm.html",
        head=head,
        rows=enriched,
        customer_name=parties["customer"] if parties else "",
        supplier_name=parties["supplier"] if parties else "",
    )

@app.route("/la_preview/<int:la_id>/accept", methods=["POST"])
def la_accept(la_id:int):
    token = request.args.get("token","")
    if token != la_token(la_id):
        abort(403)
    note = request.form.get("note","")
    con = db()
    con.execute("""
        UPDATE loading_advice_head SET approval_status='ACCEPTED', approval_note=?
        WHERE la_id=?
    """,(note,la_id))
    con.commit()
    return redirect(url_for('la_preview', la_id=la_id, token=la_token(la_id)))

@app.route("/la_preview/<int:la_id>/decline", methods=["POST"])
def la_decline(la_id:int):
    token = request.args.get("token","")
    if token != la_token(la_id):
        abort(403)
    note = request.form.get("note","")
    con = db()
    con.execute("""
        UPDATE loading_advice_head SET approval_status='DECLINED', approval_note=?
        WHERE la_id=?
    """,(note,la_id))
    con.commit()
    return redirect(url_for('la_preview', la_id=la_id, token=la_token(la_id)))

@app.route("/la_whatsapp/<int:la_id>")
def la_whatsapp(la_id:int):
    if "user" not in session:
        return redirect("/")
    con = db()
    head = con.execute("""
        SELECT h.la_id,h.la_date,h.vehicle_no,c.acc_name,c.mobile
        FROM loading_advice_head h
        LEFT JOIN acc_mast c ON h.customer_id=c.acc_id
        WHERE h.la_id=?
    """,(la_id,)).fetchone()
    if not head:
        abort(404)
    con.execute("UPDATE loading_advice_head SET whatsapp_sent=1 WHERE la_id=?", (la_id,))
    preview_url = url_for('la_preview', la_id=la_id, token=la_token(la_id), _external=True)
    msg = f"Loading Advice #{head[0]} Date {head[1]} Vehicle {head[2]}. Review and accept: {preview_url}"
    phone = head[4] or ""
    notify_whatsapp(con, "LA", head[0], head[3], phone, msg)
    con.commit()
    wa = f"https://wa.me/{phone}?text={quote_plus(msg)}" if phone else f"https://wa.me/?text={quote_plus(msg)}"
    return redirect(wa)

# ---------------- ITEM LEDGER ----------------
@app.route("/item_ledger")
def item_ledger():
    if "user" not in session:
        return redirect("/")
    con = db()
    items = con.execute("SELECT item_id,item_name FROM item_mast ORDER BY item_name").fetchall()
    selected_item = request.args.get("item_id", "")
    rows = []
    if selected_item:
        rows = con.execute("""
            SELECT h.la_date,i.item_name,
                   sup.acc_name supplier, cust.acc_name customer,
                   b.qty, b.so_id, b.po_id
            FROM loading_advice_body b
            JOIN loading_advice_head h ON b.la_id=h.la_id
            JOIN sale_orders s ON b.so_id=s.so_id
            JOIN purchase_orders p ON b.po_id=p.po_id
            JOIN item_mast i ON s.item_id=i.item_id
            JOIN acc_mast cust ON s.acc_id=cust.acc_id
            JOIN acc_mast sup ON p.acc_id=sup.acc_id
            WHERE i.item_id=?
            ORDER BY h.la_date DESC, b.id DESC
        """, (selected_item,)).fetchall()
    return render_template("item_ledger.html", items=items, selected_item=selected_item, rows=rows)

# ---------------- PURCHASE PIPELINE ----------------
@app.route("/purchase_pipeline")
def purchase_pipeline():
    if "user" not in session:
        return redirect("/")
    con = db()
    rows = con.execute("""
        SELECT a.acc_name supplier, i.item_name,
               (p.qty-COALESCE(p.supplied_qty,0)) pending_qty,
               p.rate,
               (p.qty-COALESCE(p.supplied_qty,0))*p.rate amount
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE (p.qty-COALESCE(p.supplied_qty,0))>0
        ORDER BY supplier, item_name
    """).fetchall()
    totals = con.execute("""
        SELECT COALESCE(SUM(p.qty-COALESCE(p.supplied_qty,0)),0) qty,
               COALESCE(SUM((p.qty-COALESCE(p.supplied_qty,0))*p.rate),0) amount
        FROM purchase_orders p
    """).fetchone()
    return render_template("purchase_pipeline.html", rows=rows, totals=totals)
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
    ensure_account_family_columns(con)
    if request.method == "POST":
        name = request.form["name"]
        acc_type = request.form["type"]
        tds_section = (request.form.get("tds_section") or "").strip().upper() or None
        tds_rate_raw = (request.form.get("tds_rate") or "").strip()
        try:
            tds_rate = float(tds_rate_raw) if tds_rate_raw else None
        except ValueError:
            tds_rate = None
        row = con.execute("SELECT acc_id FROM acc_mast WHERE acc_name=?", (name,)).fetchone()
        if not row:
            con.execute("""
                INSERT INTO acc_mast(acc_name,acc_type,tds_section,tds_rate)
                VALUES (?,?,?,?)
            """, (name, acc_type, tds_section, tds_rate))
            con.commit()
            flash("Account saved successfully","success")
    rows = con.execute("""
        SELECT acc_id,acc_name,acc_type,
               COALESCE(tds_section,'') AS tds_section,
               COALESCE(tds_rate,0) AS tds_rate
        FROM acc_mast
        ORDER BY acc_name
    """).fetchall()
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
            flash("Item saved successfully","success")
    rows = con.execute("SELECT * FROM item_mast").fetchall()
    return render_template("items.html", rows=rows)

# ---------------- CONTROL REPORT ----------------
@app.route("/control_report")
def control_report():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_notifications_table(con)
    company_id = session.get("company_id")
    so_total = con.execute("SELECT COALESCE(SUM(amount),0) FROM sale_orders WHERE company_id=?", (company_id,)).fetchone()[0]
    po_total = con.execute("SELECT COALESCE(SUM(amount),0) FROM purchase_orders WHERE company_id=?", (company_id,)).fetchone()[0]
    loaded_qty = con.execute("""
        SELECT COALESCE(SUM(b.qty),0)
        FROM loading_advice_body b
        JOIN sale_orders s ON b.so_id=s.so_id
        WHERE s.company_id=?
    """, (company_id,)).fetchone()[0]
    invoice_total = con.execute("SELECT COALESCE(SUM(total),0) FROM sale_invoice_head WHERE company_id=?", (company_id,)).fetchone()[0]
    summary = {
        "so_total": so_total,
        "po_total": po_total,
        "loaded_qty": loaded_qty,
        "invoice_total": invoice_total
    }
    items = []
    for i in con.execute("SELECT item_id,item_name FROM item_mast").fetchall():
        item_id, item_name = i[0], i[1]
        so_qty = con.execute("SELECT COALESCE(SUM(qty),0) FROM sale_orders WHERE item_id=? AND company_id=?", (item_id, company_id)).fetchone()[0]
        po_qty = con.execute("SELECT COALESCE(SUM(qty),0) FROM purchase_orders WHERE item_id=? AND company_id=?", (item_id, company_id)).fetchone()[0]
        loaded = con.execute("""
            SELECT COALESCE(SUM(b.qty),0)
            FROM loading_advice_body b
            JOIN sale_orders s ON b.so_id=s.so_id
            WHERE s.item_id=? AND s.company_id=?
        """, (item_id, company_id)).fetchone()[0]
        pending_so = con.execute("""
            SELECT COALESCE(SUM(qty - supplied_qty),0)
            FROM sale_orders WHERE item_id=? AND company_id=?
        """, (item_id, company_id)).fetchone()[0]
        pending_po = con.execute("""
            SELECT COALESCE(SUM(qty - supplied_qty),0)
            FROM purchase_orders WHERE item_id=? AND company_id=?
        """, (item_id, company_id)).fetchone()[0]
        items.append((item_name, so_qty, loaded, pending_so, po_qty, pending_po))
    recent_notifications = con.execute("""
        SELECT id,created_at,kind,ref_id,party_name,mobile,status
        FROM notifications
        ORDER BY id DESC
        LIMIT 10
    """).fetchall()
    return render_template("control_report.html", summary=summary, items=items, recent_notifications=recent_notifications)

@app.route("/notifications", methods=["GET","POST"])
def notifications():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_notifications_table(con)
    if request.method == "POST":
        notif_id = int(request.form.get("id") or 0)
        reply_text = request.form.get("reply_text","").strip()
        status = request.form.get("status","REPLIED")
        if notif_id:
            now = datetime.now().isoformat(sep=" ", timespec="seconds")
            con.execute("""
                UPDATE notifications
                SET status=?, reply_text=?, reply_time=?
                WHERE id=?
            """, (status, reply_text, now, notif_id))
            con.commit()
        return redirect(url_for("notifications"))
    rows = con.execute("""
        SELECT id,created_at,channel,direction,kind,ref_id,party_name,mobile,message,status,reply_text,reply_time
        FROM notifications
        ORDER BY id DESC
        LIMIT 200
    """).fetchall()
    return render_template("notifications.html", rows=rows)

@app.route("/ai_settings", methods=["GET","POST"])
def ai_settings():
    if "user" not in session:
        return redirect("/")
    if session.get("role") not in ["admin","manager"]:
        abort(403)
    con = db()
    ensure_ai_settings_table(con)
    if request.method == "POST":
        provider = request.form.get("default_provider","auto")
        if provider not in ("auto","openai","gemini"):
            provider = "auto"
        gemini_key_input = request.form.get("gemini_key","").strip()
        if gemini_key_input:
            con.execute("UPDATE ai_settings SET default_provider=?, gemini_key=? WHERE id=1", (provider, gemini_key_input))
        else:
            con.execute("UPDATE ai_settings SET default_provider=? WHERE id=1", (provider,))
        con.commit()
        flash("AI settings updated","success")
        return redirect("/ai_settings")
    row = con.execute("SELECT default_provider,gemini_key FROM ai_settings WHERE id=1").fetchone()
    default_provider = row["default_provider"] if row else "auto"
    try:
        from ai_engine import openai_client, gemini_model
        openai_available = bool(openai_client)
        gemini_available = bool(gemini_model)
    except Exception:
        openai_available = False
        gemini_available = False
    return render_template("ai_settings.html", default_provider=default_provider, openai_available=openai_available, gemini_available=gemini_available)

# ---------------- ACCOUNTS MENU PLACEHOLDERS ----------------
def _render_section(title):
    return render_template("accounts_section.html", title=title)

@app.route("/chart_of_accounts", methods=["GET","POST"])
def chart_of_accounts():
    if "user" not in session:
        return redirect("/")
    con = db()
    types = sorted({r[0] for r in con.execute("SELECT DISTINCT acc_type FROM acc_groups").fetchall()})
    if request.method == "POST":
        action = request.form.get("action","")
        selected_type = request.form.get("type") or (types[0] if types else "")
        if action == "add_ledger":
            name = request.form.get("ledger_name","").strip()
            group_id = int(request.form.get("group_id") or 0)
            if name and group_id:
                try:
                    cur = con.execute("INSERT INTO ledgers(ledger_name,group_id) VALUES (?,?)", (name, group_id))
                    ledger_id = cur.lastrowid
                    audit(con, "ledgers", "INSERT", ledger_id, "")
                    con.commit()
                    flash("Ledger added successfully","success")
                except sqlite3.IntegrityError:
                    flash("Ledger name already exists","info")
            return redirect(url_for("chart_of_accounts", type=selected_type))
        if action == "delete_ledger":
            ledger_id = int(request.form.get("ledger_id") or 0)
            if ledger_id:
                used = con.execute("SELECT COUNT(*) FROM vouchers_lines WHERE ledger_id=?", (ledger_id,)).fetchone()[0]
                if used:
                    flash("Cannot delete ledger used in vouchers","info")
                else:
                    con.execute("DELETE FROM ledgers WHERE ledger_id=?", (ledger_id,))
                    audit(con, "ledgers", "DELETE", ledger_id, "")
                    con.commit()
                    flash("Ledger deleted","success")
            return redirect(url_for("chart_of_accounts", type=selected_type))
    selected = request.args.get("type") or (types[0] if types else "")
    tree = []
    if selected:
        groups = con.execute("""
            SELECT group_id,group_name
            FROM acc_groups
            WHERE acc_type=?
            ORDER BY group_name
        """, (selected,)).fetchall()
        for g in groups:
            gid, gname = g[0], g[1]
            ledgers = con.execute("""
                SELECT ledger_id,ledger_name
                FROM ledgers
                WHERE group_id=?
                ORDER BY ledger_name
            """, (gid,)).fetchall()
            tree.append({"id": gid, "name": gname, "ledgers": ledgers})
    summary = []
    for t in types:
        cnt = con.execute("""
            SELECT COUNT(*)
            FROM ledgers l
            JOIN acc_groups g ON l.group_id=g.group_id
            WHERE g.acc_type=?
        """, (t,)).fetchone()[0]
        summary.append((t, cnt))
    return render_template("chart_of_accounts.html", types=types, selected=selected, tree=tree, summary=summary)

@app.route("/receipt", methods=["GET","POST"])
def receipt():
    if "user" not in session:
        return redirect("/")
    con = db()
    cash_bank = con.execute("SELECT ledger_id,ledger_name FROM ledgers WHERE ledger_name IN ('Cash','Bank') ORDER BY ledger_name").fetchall()
    parties = con.execute("SELECT acc_id,acc_name FROM acc_mast ORDER BY acc_name").fetchall()
    if request.method == "POST":
        v_date = request.form["date"]
        ledger_id = int(request.form["ledger_id"])
        acc_id = int(request.form["acc_id"])
        amount = float(request.form["amount"] or 0)
        narration = request.form.get("narration","")
        cur = con.execute("""
            INSERT INTO vouchers_head(v_date,v_type,narration,user_code,company_id,division_id,fy_id)
            VALUES (?,?,?,?,?,?,?)
        """, (v_date, "RECEIPT", narration, session["user"],
              session.get("company_id"),
              session.get("division_id"),
              session.get("fy_id")))
        v_id = cur.lastrowid
        con.execute("""
            INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
            VALUES (?,?,?,?,?)
        """, (v_id, ledger_id, None, amount, 0))
        con.execute("""
            INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
            VALUES (?,?,?,?,?)
        """, (v_id, None, acc_id, 0, amount))
        audit(con, "vouchers_head", "INSERT", v_id, f"RECEIPT {amount}")
        con.commit()
        flash(f"Receipt #{v_id} saved successfully","success")
        return redirect("/receipt")
    rows = con.execute("""
        SELECT h.v_id,h.v_date,h.narration,
               COALESCE(SUM(l.dr),0) dr, COALESCE(SUM(l.cr),0) cr
        FROM vouchers_head h
        LEFT JOIN vouchers_lines l ON h.v_id=l.v_id
        WHERE h.v_type='RECEIPT'
        GROUP BY h.v_id
        ORDER BY h.v_id DESC
        LIMIT 20
    """).fetchall()
    return render_template("accounts_receipt.html", today=today(), cash_bank=cash_bank, parties=parties, rows=rows)

@app.route("/payment", methods=["GET","POST"])
def payment():
    if "user" not in session:
        return redirect("/")
    con = db()
    cash_bank = con.execute("SELECT ledger_id,ledger_name FROM ledgers WHERE ledger_name IN ('Cash','Bank') ORDER BY ledger_name").fetchall()
    parties = con.execute("SELECT acc_id,acc_name FROM acc_mast ORDER BY acc_name").fetchall()
    if request.method == "POST":
        v_date = request.form["date"]
        ledger_id = int(request.form["ledger_id"])
        acc_id = int(request.form["acc_id"])
        amount = float(request.form["amount"] or 0)
        narration = request.form.get("narration","")
        cur = con.execute("""
            INSERT INTO vouchers_head(v_date,v_type,narration,user_code,company_id,division_id,fy_id)
            VALUES (?,?,?,?,?,?,?)
        """, (v_date, "PAYMENT", narration, session["user"],
              session.get("company_id"),
              session.get("division_id"),
              session.get("fy_id")))
        v_id = cur.lastrowid
        con.execute("""
            INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
            VALUES (?,?,?,?,?)
        """, (v_id, None, acc_id, amount, 0))
        con.execute("""
            INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
            VALUES (?,?,?,?,?)
        """, (v_id, ledger_id, None, 0, amount))
        audit(con, "vouchers_head", "INSERT", v_id, f"PAYMENT {amount}")
        con.commit()
        flash(f"Payment #{v_id} saved successfully","success")
        return redirect("/payment")
    rows = con.execute("""
        SELECT h.v_id,h.v_date,h.narration,
               COALESCE(SUM(l.dr),0) dr, COALESCE(SUM(l.cr),0) cr
        FROM vouchers_head h
        LEFT JOIN vouchers_lines l ON h.v_id=l.v_id
        WHERE h.v_type='PAYMENT'
        GROUP BY h.v_id
        ORDER BY h.v_id DESC
        LIMIT 20
    """).fetchall()
    return render_template("accounts_payment.html", today=today(), cash_bank=cash_bank, parties=parties, rows=rows)

@app.route("/journal", methods=["GET","POST"])
def journal():
    if "user" not in session:
        return redirect("/")
    con = db()
    all_ledgers = con.execute("SELECT ledger_id,ledger_name FROM ledgers ORDER BY ledger_name").fetchall()
    all_accounts = con.execute("SELECT acc_id,acc_name FROM acc_mast ORDER BY acc_name").fetchall()
    if request.method == "POST":
        v_date = request.form["date"]
        narration = request.form.get("narration","")
        dr_ledgers = request.form.getlist("dr_ledger_id")
        dr_accounts = request.form.getlist("dr_acc_id")
        dr_amounts = request.form.getlist("dr_amount")
        cr_ledgers = request.form.getlist("cr_ledger_id")
        cr_accounts = request.form.getlist("cr_acc_id")
        cr_amounts = request.form.getlist("cr_amount")
        cur = con.execute("""
            INSERT INTO vouchers_head(v_date,v_type,narration,user_code,company_id,division_id,fy_id)
            VALUES (?,?,?,?,?,?,?)
        """, (v_date, "JOURNAL", narration, session["user"],
              session.get("company_id"),
              session.get("division_id"),
              session.get("fy_id")))
        v_id = cur.lastrowid
        for i in range(len(dr_amounts)):
            amt = float(dr_amounts[i] or 0)
            if amt <= 0: continue
            lid = int(dr_ledgers[i]) if dr_ledgers[i] else None
            aid = int(dr_accounts[i]) if dr_accounts[i] else None
            con.execute("""
                INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
                VALUES (?,?,?,?,?)
            """, (v_id, lid, aid, amt, 0))
        for i in range(len(cr_amounts)):
            amt = float(cr_amounts[i] or 0)
            if amt <= 0: continue
            lid = int(cr_ledgers[i]) if cr_ledgers[i] else None
            aid = int(cr_accounts[i]) if cr_accounts[i] else None
            con.execute("""
                INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
                VALUES (?,?,?,?,?)
            """, (v_id, lid, aid, 0, amt))
        audit(con, "vouchers_head", "INSERT", v_id, "JOURNAL")
        con.commit()
        flash(f"Journal #{v_id} saved successfully","success")
        return redirect("/journal")
    rows = con.execute("""
        SELECT h.v_id,h.v_date,h.narration,
               COALESCE(SUM(l.dr),0) dr, COALESCE(SUM(l.cr),0) cr
        FROM vouchers_head h
        LEFT JOIN vouchers_lines l ON h.v_id=l.v_id
        WHERE h.v_type='JOURNAL'
        GROUP BY h.v_id
        ORDER BY h.v_id DESC
        LIMIT 20
    """).fetchall()
    return render_template("accounts_journal.html", today=today(), ledgers=all_ledgers, accounts=all_accounts, rows=rows)

@app.route("/credit_note", methods=["GET","POST"])
def credit_note():
    if "user" not in session:
        return redirect("/")
    con = db()
    sale_orders = con.execute("""
        SELECT s.so_id,s.so_date,a.acc_name,s.qty*s.rate amount
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        ORDER BY s.so_id DESC
        LIMIT 50
    """).fetchall()
    if request.method == "POST":
        so_id_raw = request.form.get("so_id") or ""
        if not so_id_raw:
            flash("Please select sale order","info")
            return redirect("/credit_note")
        so_id = int(so_id_raw)
        v_date = request.form["date"]
        amount = float(request.form.get("amount") or 0)
        reason = request.form.get("reason","")
        cust = con.execute("""
            SELECT a.acc_id,a.acc_name
            FROM sale_orders s
            JOIN acc_mast a ON s.acc_id=a.acc_id
            WHERE s.so_id=?
        """, (so_id,)).fetchone()
        if not cust:
            flash("Sale order not found","info")
            return redirect("/credit_note")
        cur = con.execute("""
            INSERT INTO credit_notes(so_id,cn_date,customer,amount,reason)
            VALUES (?,?,?,?,?)
        """, (so_id, v_date, cust["acc_name"], amount, reason))
        cn_id = cur.lastrowid
        cur = con.execute("""
            INSERT INTO vouchers_head(v_date,v_type,narration,user_code,company_id,division_id,fy_id)
            VALUES (?,?,?,?,?,?,?)
        """, (v_date, "CREDIT_NOTE", f"CN#{cn_id} SO{so_id} {reason}", session["user"],
              session.get("company_id"),
              session.get("division_id"),
              session.get("fy_id")))
        v_id = cur.lastrowid
        sales_ledger = con.execute("SELECT ledger_id FROM ledgers WHERE ledger_name='Sales'").fetchone()
        sales_id = sales_ledger[0] if sales_ledger else None
        con.execute("""
            INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
            VALUES (?,?,?,?,?)
        """, (v_id, None, cust["acc_id"], amount, 0))
        con.execute("""
            INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
            VALUES (?,?,?,?,?)
        """, (v_id, sales_id, None, 0, amount))
        audit(con, "credit_notes", "INSERT", cn_id, f"CN against SO{so_id} {amount}")
        con.commit()
        flash(f"Credit Note #{cn_id} saved successfully","success")
        return redirect("/credit_note")
    rows = con.execute("""
        SELECT id,cn_date,so_id,customer,amount,reason
        FROM credit_notes
        ORDER BY id DESC
        LIMIT 50
    """).fetchall()
    return render_template("credit_note.html", today=today(), sale_orders=sale_orders, rows=rows, customer=None)

@app.route("/debit_note", methods=["GET","POST"])
def debit_note():
    if "user" not in session:
        return redirect("/")
    con = db()
    purchase_orders = con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,p.qty*p.rate amount
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        ORDER BY p.po_id DESC
        LIMIT 50
    """).fetchall()
    if request.method == "POST":
        po_id_raw = request.form.get("po_id") or ""
        if not po_id_raw:
            flash("Please select purchase order","info")
            return redirect("/debit_note")
        po_id = int(po_id_raw)
        v_date = request.form["date"]
        amount = float(request.form.get("amount") or 0)
        reason = request.form.get("reason","")
        sup = con.execute("""
            SELECT a.acc_id,a.acc_name
            FROM purchase_orders p
            JOIN acc_mast a ON p.acc_id=a.acc_id
            WHERE p.po_id=?
        """, (po_id,)).fetchone()
        if not sup:
            flash("Purchase order not found","info")
            return redirect("/debit_note")
        cur = con.execute("""
            INSERT INTO debit_notes(po_id,dn_date,supplier,amount,reason)
            VALUES (?,?,?,?,?)
        """, (po_id, v_date, sup["acc_name"], amount, reason))
        dn_id = cur.lastrowid
        cur = con.execute("""
            INSERT INTO vouchers_head(v_date,v_type,narration,user_code,company_id,division_id,fy_id)
            VALUES (?,?,?,?,?,?,?)
        """, (v_date, "DEBIT_NOTE", f"DN#{dn_id} PO{po_id} {reason}", session["user"],
              session.get("company_id"),
              session.get("division_id"),
              session.get("fy_id")))
        v_id = cur.lastrowid
        purchase_ledger = con.execute("SELECT ledger_id FROM ledgers WHERE ledger_name='Purchase'").fetchone()
        purchase_id = purchase_ledger[0] if purchase_ledger else None
        con.execute("""
            INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
            VALUES (?,?,?,?,?)
        """, (v_id, purchase_id, None, amount, 0))
        con.execute("""
            INSERT INTO vouchers_lines(v_id,ledger_id,acc_id,dr,cr)
            VALUES (?,?,?,?,?)
        """, (v_id, None, sup["acc_id"], 0, amount))
        audit(con, "debit_notes", "INSERT", dn_id, f"DN against PO{po_id} {amount}")
        con.commit()
        flash(f"Debit Note #{dn_id} saved successfully","success")
        return redirect("/debit_note")
    rows = con.execute("""
        SELECT id,dn_date,po_id,supplier,amount,reason
        FROM debit_notes
        ORDER BY id DESC
        LIMIT 50
    """).fetchall()
    return render_template("debit_note.html", today=today(), purchase_orders=purchase_orders, rows=rows, supplier=None)

@app.route("/trial_balance")
def trial_balance():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    rows = []
    total_dr = 0.0
    total_cr = 0.0
    raw = con.execute("""
        WITH lines AS (
          SELECT h.v_date, l.ledger_id, l.acc_id, l.dr, l.cr
          FROM vouchers_lines l
          JOIN vouchers_head h ON h.v_id=l.v_id
          WHERE h.company_id=? AND h.v_date BETWEEN ? AND ?
        ),
        ledger_bal AS (
          SELECT 'LEDGER' AS kind, lg.ledger_name AS name, NULL AS acc_type,
                 COALESCE(SUM(lines.dr),0) AS dr,
                 COALESCE(SUM(lines.cr),0) AS cr
          FROM ledgers lg
          LEFT JOIN lines ON lines.ledger_id=lg.ledger_id
          GROUP BY lg.ledger_id, lg.ledger_name
        ),
        account_bal AS (
          SELECT 'ACCOUNT' AS kind, a.acc_name AS name, a.acc_type,
                 COALESCE(SUM(lines.dr),0) AS dr,
                 COALESCE(SUM(lines.cr),0) AS cr
          FROM acc_mast a
          LEFT JOIN lines ON lines.acc_id=a.acc_id
          GROUP BY a.acc_id, a.acc_name, a.acc_type
        )
        SELECT kind,name,acc_type,dr,cr
        FROM ledger_bal
        UNION ALL
        SELECT kind,name,acc_type,dr,cr
        FROM account_bal
    """, (company_id, from_date, to_date)).fetchall()
    for r in raw:
        dr = float(r["dr"] or 0)
        cr = float(r["cr"] or 0)
        if dr == 0 and cr == 0:
            continue
        total_dr += dr
        total_cr += cr
        rows.append(r)
    diff = round(total_dr - total_cr, 2)
    return render_template(
        "trial_balance.html",
        rows=rows,
        from_date=from_date,
        to_date=to_date,
        total_dr=total_dr,
        total_cr=total_cr,
        diff=diff,
    )

@app.route("/pl")
def pl():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")
    income_rows = []
    expense_rows = []
    total_income = 0.0
    total_expense = 0.0
    rows = con.execute("""
        WITH lines AS (
          SELECT h.v_date, l.ledger_id, l.dr, l.cr
          FROM vouchers_lines l
          JOIN vouchers_head h ON h.v_id=l.v_id
          WHERE h.company_id=? AND h.v_date BETWEEN ? AND ?
        )
        SELECT lg.ledger_name name, g.group_name, g.acc_type,
               COALESCE(SUM(lines.dr),0) dr,
               COALESCE(SUM(lines.cr),0) cr
        FROM ledgers lg
        JOIN acc_groups g ON lg.group_id=g.group_id
        LEFT JOIN lines ON lines.ledger_id=lg.ledger_id
        WHERE g.acc_type IN ('INCOME','EXPENSE')
        GROUP BY lg.ledger_id, lg.ledger_name, g.group_name, g.acc_type
        ORDER BY g.acc_type, g.group_name, lg.ledger_name
    """, (company_id, from_date, to_date)).fetchall()
    for r in rows:
        acc_type = r["acc_type"]
        dr = float(r["dr"] or 0)
        cr = float(r["cr"] or 0)
        if acc_type == "INCOME":
            amt = cr - dr
            if amt <= 0:
                continue
            total_income += amt
            income_rows.append({
                "name": r["name"],
                "group": r["group_name"],
                "amount": amt,
            })
        elif acc_type == "EXPENSE":
            amt = dr - cr
            if amt <= 0:
                continue
            total_expense += amt
            expense_rows.append({
                "name": r["name"],
                "group": r["group_name"],
                "amount": amt,
            })
    net_profit = total_income - total_expense
    return render_template(
        "pl.html",
        from_date=from_date,
        to_date=to_date,
        income_rows=income_rows,
        expense_rows=expense_rows,
        total_income=total_income,
        total_expense=total_expense,
        net_profit=net_profit,
    )

@app.route("/balance_sheet")
def balance_sheet():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")
    asset_rows = []
    liability_rows = []
    equity_rows = []
    total_assets = 0.0
    total_liabilities = 0.0
    total_equity = 0.0
    rows = con.execute("""
        WITH lines AS (
          SELECT h.v_date, l.ledger_id, l.dr, l.cr
          FROM vouchers_lines l
          JOIN vouchers_head h ON h.v_id=l.v_id
          WHERE h.company_id=? AND h.v_date BETWEEN ? AND ?
        )
        SELECT lg.ledger_name name, g.group_name, g.acc_type,
               COALESCE(SUM(lines.dr),0) dr,
               COALESCE(SUM(lines.cr),0) cr
        FROM ledgers lg
        JOIN acc_groups g ON lg.group_id=g.group_id
        LEFT JOIN lines ON lines.ledger_id=lg.ledger_id
        WHERE g.acc_type IN ('ASSET','LIABILITY','EQUITY')
        GROUP BY lg.ledger_id, lg.ledger_name, g.group_name, g.acc_type
        ORDER BY g.acc_type, g.group_name, lg.ledger_name
    """, (company_id, from_date, to_date)).fetchall()
    for r in rows:
        acc_type = r["acc_type"]
        dr = float(r["dr"] or 0)
        cr = float(r["cr"] or 0)
        if acc_type == "ASSET":
            amt = dr - cr
            if amt <= 0:
                continue
            total_assets += amt
            asset_rows.append({
                "name": r["name"],
                "group": r["group_name"],
                "amount": amt,
            })
        elif acc_type == "LIABILITY":
            amt = cr - dr
            if amt <= 0:
                continue
            total_liabilities += amt
            liability_rows.append({
                "name": r["name"],
                "group": r["group_name"],
                "amount": amt,
            })
        elif acc_type == "EQUITY":
            amt = cr - dr
            if amt <= 0:
                continue
            total_equity += amt
            equity_rows.append({
                "name": r["name"],
                "group": r["group_name"],
                "amount": amt,
            })
    income_expense_rows = con.execute("""
        WITH lines AS (
          SELECT h.v_date, l.ledger_id, l.dr, l.cr
          FROM vouchers_lines l
          JOIN vouchers_head h ON h.v_id=l.v_id
          WHERE h.company_id=? AND h.v_date BETWEEN ? AND ?
        )
        SELECT lg.ledger_name name, g.group_name, g.acc_type,
               COALESCE(SUM(lines.dr),0) dr,
               COALESCE(SUM(lines.cr),0) cr
        FROM ledgers lg
        JOIN acc_groups g ON lg.group_id=g.group_id
        LEFT JOIN lines ON lines.ledger_id=lg.ledger_id
        WHERE g.acc_type IN ('INCOME','EXPENSE')
        GROUP BY lg.ledger_id, lg.ledger_name, g.group_name, g.acc_type
        ORDER BY g.acc_type, g.group_name, lg.ledger_name
    """, (company_id, from_date, to_date)).fetchall()
    total_income = 0.0
    total_expense = 0.0
    for r in income_expense_rows:
        acc_type = r["acc_type"]
        dr = float(r["dr"] or 0)
        cr = float(r["cr"] or 0)
        if acc_type == "INCOME":
            amt = cr - dr
            if amt <= 0:
                continue
            total_income += amt
        elif acc_type == "EXPENSE":
            amt = dr - cr
            if amt <= 0:
                continue
            total_expense += amt
    net_profit = total_income - total_expense
    equity_with_profit = total_equity + net_profit
    total_liab_equity = total_liabilities + equity_with_profit
    diff = total_assets - total_liab_equity
    return render_template(
        "balance_sheet.html",
        from_date=from_date,
        to_date=to_date,
        asset_rows=asset_rows,
        liability_rows=liability_rows,
        equity_rows=equity_rows,
        total_assets=total_assets,
        total_liabilities=total_liabilities,
        total_equity=total_equity,
        net_profit=net_profit,
        equity_with_profit=equity_with_profit,
        total_liab_equity=total_liab_equity,
        diff=diff,
    )

@app.route("/gst_report")
def gst_report():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")
    home_state = "GJ"
    gst_rate = 0.18
    sales_raw = con.execute("""
        SELECT h.inv_id,h.inv_date,h.customer,h.total,
               a.state
        FROM sale_invoice_head h
        LEFT JOIN loading_advice_body b ON h.la_id=b.la_id
        LEFT JOIN sale_orders s ON b.so_id=s.so_id
        LEFT JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.company_id=? AND h.inv_date BETWEEN ? AND ?
        GROUP BY h.inv_id,h.inv_date,h.customer,h.total,a.state
        ORDER BY h.inv_date,h.inv_id
    """, (company_id, from_date, to_date)).fetchall()
    sales_rows = []
    out_total_value = 0.0
    out_total_igst = 0.0
    out_total_cgst = 0.0
    out_total_sgst = 0.0
    for r in sales_raw:
        value = float(r["total"] or 0)
        state = (r["state"] or "").strip().upper()
        gst = round(value * gst_rate, 2)
        igst = 0.0
        cgst = 0.0
        sgst = 0.0
        if state and state == home_state.upper():
            cgst = round(gst / 2.0, 2)
            sgst = gst - cgst
        else:
            igst = gst
        out_total_value += value
        out_total_igst += igst
        out_total_cgst += cgst
        out_total_sgst += sgst
        sales_rows.append({
            "inv_id": r["inv_id"],
            "inv_date": r["inv_date"],
            "customer": r["customer"],
            "state": state,
            "value": value,
            "igst": igst,
            "cgst": cgst,
            "sgst": sgst,
        })
    purchase_raw = con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name supplier,a.state,
               i.item_name,p.amount
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE p.company_id=? AND p.po_date BETWEEN ? AND ?
        ORDER BY p.po_date,p.po_id
    """, (company_id, from_date, to_date)).fetchall()
    purchase_rows = []
    in_total_value = 0.0
    in_total_igst = 0.0
    in_total_cgst = 0.0
    in_total_sgst = 0.0
    for r in purchase_raw:
        value = float(r["amount"] or 0)
        state = (r["state"] or "").strip().upper()
        gst = round(value * gst_rate, 2)
        igst = 0.0
        cgst = 0.0
        sgst = 0.0
        if state and state == home_state.upper():
            cgst = round(gst / 2.0, 2)
            sgst = gst - cgst
        else:
            igst = gst
        in_total_value += value
        in_total_igst += igst
        in_total_cgst += cgst
        in_total_sgst += sgst
        purchase_rows.append({
            "po_id": r["po_id"],
            "po_date": r["po_date"],
            "supplier": r["supplier"],
            "item": r["item_name"],
            "state": state,
            "value": value,
            "igst": igst,
            "cgst": cgst,
            "sgst": sgst,
        })
    out_total_gst = out_total_igst + out_total_cgst + out_total_sgst
    in_total_gst = in_total_igst + in_total_cgst + in_total_sgst
    net_gst_payable = out_total_gst - in_total_gst
    return render_template(
        "gst_report.html",
        from_date=from_date,
        to_date=to_date,
        sales_rows=sales_rows,
        purchase_rows=purchase_rows,
        out_total_value=out_total_value,
        out_total_igst=out_total_igst,
        out_total_cgst=out_total_cgst,
        out_total_sgst=out_total_sgst,
        out_total_gst=out_total_gst,
        in_total_value=in_total_value,
        in_total_igst=in_total_igst,
        in_total_cgst=in_total_cgst,
        in_total_sgst=in_total_sgst,
        in_total_gst=in_total_gst,
        net_gst_payable=net_gst_payable,
    )

@app.route("/search")
def global_search():
    if "user" not in session:
        return redirect("/")
    q = (request.args.get("q") or "").strip()
    catalog = get_routes_catalog()
    if not q:
        results = catalog[:10]
        return render_template("global_search.html", q=q, results=results)
    q_l = q.lower()
    matches = []
    for r in catalog:
        text = (r["title"] + " " + r["section"] + " " + r["path"]).lower()
        if q_l in text:
            matches.append(r)
    results = matches[:20]
    return render_template("global_search.html", q=q, results=results)

@app.route("/cust_purch_report")
def cust_purch_report():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_grn_tables(con)
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")
    selected_state = (request.args.get("state") or "").strip().upper()
    states = sorted({(r[0] or "").strip().upper() for r in con.execute("SELECT DISTINCT state FROM acc_mast WHERE state IS NOT NULL AND state<>''").fetchall()})
    sales_sql = """
        SELECT h.inv_id,h.inv_date,h.customer,
               COALESCE(SUM(b.qty),0) qty,
               COALESCE(SUM(b.amount),0) amount,
               a.state
        FROM sale_invoice_head h
        LEFT JOIN sale_invoice_body b ON h.inv_id=b.inv_id
        LEFT JOIN loading_advice_body lb ON h.la_id=lb.la_id
        LEFT JOIN sale_orders s ON lb.so_id=s.so_id
        LEFT JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.company_id=? AND h.inv_date BETWEEN ? AND ?
    """
    params = [company_id, from_date, to_date]
    if selected_state:
        sales_sql += " AND UPPER(COALESCE(a.state,''))=?"
        params.append(selected_state)
    sales_sql += """
        GROUP BY h.inv_id,h.inv_date,h.customer,a.state
        ORDER BY h.inv_date,h.inv_id
    """
    sales_rows_raw = con.execute(sales_sql, params).fetchall()
    sales_rows = []
    sales_totals = {"qty": 0.0, "amount": 0.0}
    for r in sales_rows_raw:
        qty = float(r["qty"] or 0)
        amount = float(r["amount"] or 0)
        avg_rate = (amount / qty) if qty else 0.0
        state = (r["state"] or "").strip().upper()
        sales_rows.append(type("R", (), {
            "inv_id": r["inv_id"],
            "inv_date": r["inv_date"],
            "customer": r["customer"],
            "state": state,
            "qty": qty,
            "avg_rate": avg_rate,
            "amount": amount,
        }))
        sales_totals["qty"] += qty
        sales_totals["amount"] += amount
    purch_sql = """
        SELECT h.grn_id,h.grn_date,h.supplier,
               COALESCE(SUM(b.qty),0) qty,
               COALESCE(SUM(b.amount),0) amount,
               a.state
        FROM grn_head h
        LEFT JOIN grn_body b ON h.grn_id=b.grn_id
        LEFT JOIN loading_advice_head lh ON h.la_id=lh.la_id
        LEFT JOIN purchase_orders p ON lh.supplier_id=p.acc_id
        LEFT JOIN acc_mast a ON lh.supplier_id=a.acc_id
        WHERE h.company_id=? AND h.grn_date BETWEEN ? AND ?
    """
    params2 = [company_id, from_date, to_date]
    if selected_state:
        purch_sql += " AND UPPER(COALESCE(a.state,''))=?"
        params2.append(selected_state)
    purch_sql += """
        GROUP BY h.grn_id,h.grn_date,h.supplier,a.state
        ORDER BY h.grn_date,h.grn_id
    """
    purch_rows_raw = con.execute(purch_sql, params2).fetchall()
    purch_rows = []
    purch_totals = {"qty": 0.0, "amount": 0.0}
    for r in purch_rows_raw:
        qty = float(r["qty"] or 0)
        amount = float(r["amount"] or 0)
        avg_rate = (amount / qty) if qty else 0.0
        state = (r["state"] or "").strip().upper()
        purch_rows.append(type("R", (), {
            "grn_id": r["grn_id"],
            "grn_date": r["grn_date"],
            "supplier": r["supplier"],
            "state": state,
            "qty": qty,
            "avg_rate": avg_rate,
            "amount": amount,
        }))
        purch_totals["qty"] += qty
        purch_totals["amount"] += amount
    return render_template(
        "cust_purch_report.html",
        from_date=from_date,
        to_date=to_date,
        states=states,
        selected_state=selected_state,
        sales_rows=sales_rows,
        purch_rows=purch_rows,
        sales_totals=sales_totals,
        purch_totals=purch_totals,
    )

@app.route("/tds_report")
def tds_report():
    if "user" not in session:
        return redirect("/")
    return render_template("tds_report.html")

@app.route("/audit_log")
def audit_log():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_audit_table(con)
    company_id = session.get("company_id")
    fy_id = session.get("fy_id")
    user_filter = request.args.get("user","")
    table_filter = request.args.get("table","")
    sql = """
        SELECT id,log_time,user_code,table_name,action,pk,details
        FROM audit_log
        WHERE 1=1
    """
    params = []
    if company_id is not None:
        sql += " AND (company_id IS NULL OR company_id=?)"
        params.append(company_id)
    if fy_id is not None:
        sql += " AND (fy_id IS NULL OR fy_id=?)"
        params.append(fy_id)
    if user_filter:
        sql += " AND user_code=?"
        params.append(user_filter)
    if table_filter:
        sql += " AND table_name=?"
        params.append(table_filter)
    sql += " ORDER BY id DESC LIMIT 200"
    rows = con.execute(sql, params).fetchall()
    users = [r[0] for r in con.execute("SELECT DISTINCT user_code FROM audit_log WHERE user_code<>'' ORDER BY user_code").fetchall()]
    tables = [r[0] for r in con.execute("SELECT DISTINCT table_name FROM audit_log ORDER BY table_name").fetchall()]
    return render_template("audit_log.html", rows=rows, users=users, tables=tables, user_filter=user_filter, table_filter=table_filter)

@app.route("/self_audit")
def self_audit():
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_company_core(con)
    issues = run_self_audit(con)
    return render_template("self_audit.html", issues=issues)
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
    ensure_account_family_columns(con)

    customer=con.execute("""
        SELECT a.acc_id,a.acc_name,
               COALESCE(a.print_name,a.acc_name) AS print_name,
               a.state,
               a.parent_acc_id
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

    # Determine group and possible invoice companies
    companies = []
    default_company_id = None
    if customer:
        group_id = customer["parent_acc_id"] or customer["acc_id"]
        default_company_id = customer["acc_id"]
        companies = con.execute("""
            SELECT acc_id,
                   acc_name,
                   COALESCE(print_name,acc_name) AS print_name,
                   state
            FROM acc_mast
            WHERE acc_id=? OR parent_acc_id=?
            ORDER BY acc_name
        """, (group_id, group_id)).fetchall()

    if request.method=="POST":

        total=float(request.form.get("grand_total",0))

        cur=con.execute("""
        INSERT INTO sale_invoice_head
        (la_id,inv_date,customer,total,company_id,division_id,fy_id)
        VALUES (?,?,?,?,?,?,?)
        """,(la_id,
             request.form["date"],
             request.form["customer"],
             total,
             session.get("company_id"),
             session.get("division_id"),
             session.get("fy_id")))

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
        audit(con, "sale_invoice_head", "INSERT", inv_id, f"total={total}")
        lock_loading_advice(la_id)
        con.commit()
        flash(f"Invoice #{inv_id} saved successfully","success")
        return redirect(f"/print_invoice/{inv_id}?created=1")

    home_state = "GJ"
    party_state = (customer["state"] or "").strip().upper() if customer and "state" in customer.keys() else ""
    gst_mode = "intra" if party_state and party_state == home_state.upper() else "inter"
    return render_template(
        "sale_invoice.html",
        la_id=la_id,
        rows=rows,
        customer=customer["print_name"] if customer else "",
        today=today(),
        party_state=party_state,
        home_state=home_state,
        gst_mode=gst_mode,
        companies=companies,
        default_company_id=default_company_id,
        invoices=con.execute("""
            SELECT h.inv_id,h.inv_date,h.customer,
                   COALESCE(SUM(b.qty),0) qty,
                   h.total
            FROM sale_invoice_head h
            LEFT JOIN sale_invoice_body b ON h.inv_id=b.inv_id
            GROUP BY h.inv_id
            ORDER BY h.inv_id DESC
        """).fetchall()
    )

# ---------------- GRN ----------------
@app.route("/grn/<int:la_id>", methods=["GET","POST"])
def grn(la_id):
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_grn_tables(con)
    supplier = con.execute("""
        SELECT a.acc_name,a.state
        FROM loading_advice_body b
        JOIN purchase_orders p ON b.po_id=p.po_id
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE b.la_id=?
        LIMIT 1
    """, (la_id,)).fetchone()
    rows = con.execute("""
        SELECT i.item_name,b.section,b.qty,p.rate
        FROM loading_advice_body b
        JOIN purchase_orders p ON b.po_id=p.po_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE b.la_id=?
    """, (la_id,)).fetchall()
    home_state = "GJ"
    party_state = (supplier["state"] or "").strip().upper() if supplier and "state" in supplier.keys() else ""
    gst_mode = "intra" if party_state and party_state == home_state.upper() else "inter"
    if request.method == "POST":
        total = float(request.form.get("grand_total", 0))
        cur = con.execute("""
        INSERT INTO grn_head
        (la_id,grn_date,supplier,total,company_id,division_id,fy_id)
        VALUES (?,?,?,?,?,?,?)
        """, (la_id,
              request.form["date"],
              request.form["supplier"],
              total,
              session.get("company_id"),
              session.get("division_id"),
              session.get("fy_id")))
        grn_id = cur.lastrowid
        items = request.form.getlist("item")
        sections = request.form.getlist("section")
        qtys = request.form.getlist("qty")
        rates = request.form.getlist("rate")
        amts = request.form.getlist("amount")
        for i in range(len(items)):
            rate = float(rates[i]) if rates[i] else 0
            amt = float(amts[i]) if amts[i] else 0
            con.execute("""
            INSERT INTO grn_body
            (grn_id,item_name,section,qty,rate,amount)
            VALUES (?,?,?,?,?,?)
            """, (grn_id,
                  items[i],
                  sections[i],
                  qtys[i],
                  rate,
                  amt))
        audit(con, "grn_head", "INSERT", grn_id, f"total={total}")
        con.commit()
        flash(f"GRN #{grn_id} saved successfully", "success")
        return redirect(f"/grn/{la_id}")
    grns = con.execute("""
        SELECT h.grn_id,h.grn_date,h.supplier,
               COALESCE(SUM(b.qty),0) qty,
               h.total
        FROM grn_head h
        LEFT JOIN grn_body b ON h.grn_id=b.grn_id
        GROUP BY h.grn_id
        ORDER BY h.grn_id DESC
    """).fetchall()
    return render_template(
        "grn.html",
        la_id=la_id,
        rows=rows,
        supplier=supplier["acc_name"] if supplier else "",
        today=today(),
        gst_mode=gst_mode,
        grns=grns,
    )

@app.route("/sale_return/<int:inv_id>", methods=["GET","POST"])
def sale_return(inv_id):
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_sale_return_tables(con)
    head = con.execute(
        "SELECT * FROM sale_invoice_head WHERE inv_id=?",
        (inv_id,)
    ).fetchone()
    if not head:
        return "Invoice not found"
    body = con.execute("""
        SELECT item_name,section,qty,rate
        FROM sale_invoice_body
        WHERE inv_id=?
    """, (inv_id,)).fetchall()
    if request.method == "POST":
        sr_date = request.form["sr_date"]
        reason = request.form.get("reason","")
        items = request.form.getlist("item")
        sections = request.form.getlist("section")
        qtys = request.form.getlist("qty")
        rates = request.form.getlist("rate")
        amts = request.form.getlist("amount")
        total = 0.0
        for a in amts:
            try:
                total += float(a or 0)
            except:
                pass
        cur = con.execute("""
            INSERT INTO sale_return_head(inv_id,sr_date,customer,total,reason)
            VALUES (?,?,?,?,?)
        """, (inv_id, sr_date, head["customer"], total, reason))
        sr_id = cur.lastrowid
        for i in range(len(items)):
            qty = float(qtys[i] or 0)
            rate = float(rates[i] or 0)
            amt = float(amts[i] or 0)
            if qty <= 0 and amt <= 0:
                continue
            con.execute("""
                INSERT INTO sale_return_body(sr_id,item_name,section,qty,rate,amount)
                VALUES (?,?,?,?,?,?)
            """, (sr_id, items[i], sections[i], qty, rate, amt))
        audit(con, "sale_return_head", "INSERT", sr_id, f"return_total={total}")
        con.commit()
        flash(f"Sale Return #{sr_id} saved successfully","success")
        return redirect(f"/sale_return/{inv_id}")
    returns = con.execute("""
        SELECT h.sr_id,h.sr_date,h.inv_id,h.customer,h.total,h.reason
        FROM sale_return_head h
        ORDER BY h.sr_id DESC
        LIMIT 50
    """).fetchall()
    return render_template(
        "sale_return.html",
        head=head,
        body=body,
        today=today(),
        returns=returns,
    )

@app.route("/purchase_return/<int:grn_id>", methods=["GET","POST"])
def purchase_return(grn_id):
    if "user" not in session:
        return redirect("/")
    con = db()
    ensure_grn_tables(con)
    ensure_purchase_return_tables(con)
    head = con.execute(
        "SELECT * FROM grn_head WHERE grn_id=?",
        (grn_id,)
    ).fetchone()
    if not head:
        return "GRN not found"
    body = con.execute("""
        SELECT item_name,section,qty,rate
        FROM grn_body
        WHERE grn_id=?
    """, (grn_id,)).fetchall()
    if request.method == "POST":
        pr_date = request.form["pr_date"]
        reason = request.form.get("reason","")
        items = request.form.getlist("item")
        sections = request.form.getlist("section")
        qtys = request.form.getlist("qty")
        rates = request.form.getlist("rate")
        amts = request.form.getlist("amount")
        total = 0.0
        for a in amts:
            try:
                total += float(a or 0)
            except:
                pass
        cur = con.execute("""
            INSERT INTO purchase_return_head(grn_id,pr_date,supplier,total,reason)
            VALUES (?,?,?,?,?)
        """, (grn_id, pr_date, head["supplier"], total, reason))
        pr_id = cur.lastrowid
        for i in range(len(items)):
            qty = float(qtys[i] or 0)
            rate = float(rates[i] or 0)
            amt = float(amts[i] or 0)
            if qty <= 0 and amt <= 0:
                continue
            con.execute("""
                INSERT INTO purchase_return_body(pr_id,item_name,section,qty,rate,amount)
                VALUES (?,?,?,?,?,?)
            """, (pr_id, items[i], sections[i], qty, rate, amt))
        audit(con, "purchase_return_head", "INSERT", pr_id, f"return_total={total}")
        con.commit()
        flash(f"Purchase Return #{pr_id} saved successfully","success")
        return redirect(f"/purchase_return/{grn_id}")
    returns = con.execute("""
        SELECT h.pr_id,h.pr_date,h.grn_id,h.supplier,h.total,h.reason
        FROM purchase_return_head h
        ORDER BY h.pr_id DESC
        LIMIT 50
    """).fetchall()
    return render_template(
        "purchase_return.html",
        head=head,
        body=body,
        today=today(),
        returns=returns,
    )

#-----------------PO PREVIEW----------------------------
@app.route("/po_preview/<int:po_id>", methods=["GET"])
def po_preview(po_id:int):
    token = request.args.get("token","")
    if token != po_token(po_id):
        abort(403)
    con = db()
    row = con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,a.mobile,i.item_name,p.qty,p.rate,p.amount,p.status
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE p.po_id=?
    """,(po_id,)).fetchone()
    if not row:
        abort(404)
    return render_template("po_preview.html", r=row)

@app.route("/po_preview/<int:po_id>/accept", methods=["POST"])
def po_accept(po_id:int):
    token = request.args.get("token","")
    if token != po_token(po_id):
        abort(403)
    con = db()
    con.execute("UPDATE purchase_orders SET status='ACCEPTED' WHERE po_id=?", (po_id,))
    con.commit()
    return render_template("po_preview.html", r=con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,a.mobile,i.item_name,p.qty,p.rate,p.amount,p.status
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE p.po_id=?
    """,(po_id,)).fetchone())

@app.route("/po_preview/<int:po_id>/decline", methods=["POST"])
def po_decline(po_id:int):
    token = request.args.get("token","")
    if token != po_token(po_id):
        abort(403)
    con = db()
    con.execute("UPDATE purchase_orders SET status='DECLINED' WHERE po_id=?", (po_id,))
    con.commit()
    return render_template("po_preview.html", r=con.execute("""
        SELECT p.po_id,p.po_date,a.acc_name,a.mobile,i.item_name,p.qty,p.rate,p.amount,p.status
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        JOIN item_mast i ON p.item_id=i.item_id
        WHERE p.po_id=?
    """,(po_id,)).fetchone())

#-----------------AI----------------------------
@app.route("/ai")
def ai_page():
    con = db()
    ensure_ai_settings_table(con)
    row = con.execute("SELECT default_provider FROM ai_settings WHERE id=1").fetchone()
    default_provider = row["default_provider"] if row else "auto"
    return render_template("ai_chat.html", default_provider=default_provider)


@app.route("/ask_ai", methods=["POST"])
def ask_ai():
    try:
        question = request.form["question"]
        provider = request.form.get("provider","auto")
        con = db()
        ensure_ai_settings_table(con)
        if provider == "auto":
            row = con.execute("SELECT default_provider FROM ai_settings WHERE id=1").fetchone()
            if row and row["default_provider"] and row["default_provider"] in ("auto","openai","gemini"):
                provider = row["default_provider"]
        sql = question_to_sql(question, provider=provider)
        df = run_sql(sql)
        answer = explain_result(df)
        return jsonify({"sql": sql, "answer": answer, "provider": provider})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ---------------- PRINT INVOICE ----------------
@app.route("/print_invoice/<int:inv_id>")
def print_invoice(inv_id):
    con = db()
    head = con.execute(
        "SELECT * FROM sale_invoice_head WHERE inv_id=?",
        (inv_id,)
    ).fetchone()
    body = con.execute("""
        SELECT item_name,section,qty,rate,amount
        FROM sale_invoice_body
        WHERE inv_id=?
    """, (inv_id,)).fetchall()
    home_state = "GJ"
    gst_rate = 0.18
    state_row = con.execute("""
        SELECT a.state
        FROM sale_invoice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.inv_id=?
        LIMIT 1
    """, (inv_id,)).fetchone()
    party_state = (state_row["state"] if state_row and state_row["state"] else "").strip().upper()
    taxable_total = 0.0
    cgst_total = 0.0
    sgst_total = 0.0
    igst_total = 0.0
    lines = []
    for r in body:
        item = r["item_name"]
        section = r["section"]
        qty = float(r["qty"] or 0)
        rate = float(r["rate"] or 0)
        amount = float(r["amount"] or 0)
        taxable = amount
        gst = round(taxable * gst_rate, 2)
        cgst = 0.0
        sgst = 0.0
        igst = 0.0
        if party_state and party_state == home_state.upper():
            cgst = round(gst / 2.0, 2)
            sgst = gst - cgst
        else:
            igst = gst
        taxable_total += taxable
        cgst_total += cgst
        sgst_total += sgst
        igst_total += igst
        lines.append({
            "item": item,
            "section": section,
            "qty": qty,
            "rate": rate,
            "amount": taxable,
            "cgst": cgst,
            "sgst": sgst,
            "igst": igst,
        })
    gst_total = cgst_total + sgst_total + igst_total
    grand_total = taxable_total + gst_total
    return render_template(
        "print_invoice.html",
        head=head,
        body_lines=lines,
        taxable_total=taxable_total,
        cgst_total=cgst_total,
        sgst_total=sgst_total,
        igst_total=igst_total,
        gst_total=gst_total,
        grand_total=grand_total,
        party_state=party_state,
    )

# ---------------- EXPORT LOADING EXCEL ----------------
@app.route("/export_loading_excel")
def export_loading_excel():
    con = db()

    rows = con.execute("""
        SELECT h.la_id,h.la_date,h.vehicle_no,
               b.so_id,b.po_id,b.section,b.part,b.qty
        FROM loading_advice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        ORDER BY h.la_id DESC
    """).fetchall()

    df = pd.DataFrame(rows)
    file = "loading_advice.xlsx"
    df.to_excel(file, index=False)

    return send_file(file, as_attachment=True)


# ---------------- EXPORT PURCHASE MOVEMENT EXCEL ----------------
@app.route("/export_purchase_movement")
def export_purchase_movement():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")

    opening_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            p.po_id,
            p.po_date,
            a.acc_name AS supplier,
            p.qty AS ordered_qty,
            p.rate AS base_rate,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN grn_head gh ON b.la_id=gh.la_id
                WHERE b.po_id=p.po_id
                  AND gh.grn_date < ?
                  AND gh.company_id=p.company_id
            ),0) AS loaded_before
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date <= ?
    """, (from_date, company_id, from_date)).fetchall()

    opening_data = []
    for r in opening_rows:
        ordered = float(r["ordered_qty"] or 0)
        loaded_before = float(r["loaded_before"] or 0)
        bal = ordered - loaded_before
        if bal <= 0:
            continue
        opening_data.append({
            "State": (r["state"] or "").strip().upper(),
            "PO No": r["po_id"],
            "PO Date": r["po_date"],
            "Supplier": r["supplier"],
            "Balance Qty": bal,
            "Base Rate": float(r["base_rate"] or 0),
            "Status": "OPENING",
        })

    trade_rows = con.execute("""
        SELECT
            gh.grn_id,
            gh.grn_date,
            gh.supplier,
            COALESCE(SUM(b.qty),0) AS qty,
            COALESCE(SUM(b.amount),0) AS amount,
            UPPER(COALESCE(a.state,'')) AS state
        FROM grn_head gh
        LEFT JOIN grn_body b ON gh.grn_id=b.grn_id
        LEFT JOIN loading_advice_body lb ON gh.la_id=lb.la_id
        LEFT JOIN purchase_orders p ON lb.po_id=p.po_id
        LEFT JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE gh.company_id=? AND gh.grn_date BETWEEN ? AND ?
        GROUP BY gh.grn_id,gh.grn_date,gh.supplier,a.state
        ORDER BY gh.grn_date,gh.grn_id
    """, (company_id, from_date, to_date)).fetchall()

    trade_data = []
    for r in trade_rows:
        qty = float(r["qty"] or 0)
        amount = float(r["amount"] or 0)
        base_rate = (amount / qty) if qty else 0.0
        trade_data.append({
            "State": (r["state"] or "").strip().upper(),
            "GRN No": r["grn_id"],
            "GRN Date": r["grn_date"],
            "Supplier": r["supplier"],
            "Recv Qty": qty,
            "Base Rate": base_rate,
            "Status": "RECEIVED",
        })

    new_po_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            p.po_id,
            p.po_date,
            a.acc_name AS supplier,
            p.qty AS ordered_qty,
            p.rate AS base_rate
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date BETWEEN ? AND ?
    """, (company_id, from_date, to_date)).fetchall()

    new_po_data = []
    for r in new_po_rows:
        ordered = float(r["ordered_qty"] or 0)
        new_po_data.append({
            "State": (r["state"] or "").strip().upper(),
            "PO No": r["po_id"],
            "PO Date": r["po_date"],
            "Supplier": r["supplier"],
            "Balance Qty": ordered,
            "Base Rate": float(r["base_rate"] or 0),
            "Status": "NEW",
        })

    closing_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            p.po_id,
            p.po_date,
            a.acc_name AS supplier,
            p.qty AS ordered_qty,
            p.rate AS base_rate,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN grn_head gh ON b.la_id=gh.la_id
                WHERE b.po_id=p.po_id
                  AND gh.grn_date <= ?
                  AND gh.company_id=p.company_id
            ),0) AS loaded_till
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date <= ?
    """, (to_date, company_id, to_date)).fetchall()

    closing_data = []
    for r in closing_rows:
        ordered = float(r["ordered_qty"] or 0)
        loaded_till = float(r["loaded_till"] or 0)
        bal = ordered - loaded_till
        if bal <= 0:
            continue
        closing_data.append({
            "State": (r["state"] or "").strip().upper(),
            "PO No": r["po_id"],
            "PO Date": r["po_date"],
            "Supplier": r["supplier"],
            "Balance Qty": bal,
            "Base Rate": float(r["base_rate"] or 0),
            "Status": "CLOSING",
        })

    df_opening = pd.DataFrame(opening_data)
    df_trade = pd.DataFrame(trade_data)
    df_new = pd.DataFrame(new_po_data)
    df_closing = pd.DataFrame(closing_data)

    file = f"purchase_movement_{from_date}_to_{to_date}.xlsx"
    with pd.ExcelWriter(file) as writer:
        df_opening.to_excel(writer, sheet_name="Opening", index=False)
        df_trade.to_excel(writer, sheet_name="Trade", index=False)
        df_new.to_excel(writer, sheet_name="New PO", index=False)
        df_closing.to_excel(writer, sheet_name="Closing", index=False)

    return send_file(file, as_attachment=True)


@app.route("/export_sale_movement")
def export_sale_movement():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")

    opening_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            s.so_id,
            s.so_date,
            a.acc_name AS customer,
            s.qty AS ordered_qty,
            s.rate AS base_rate,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN sale_invoice_head h ON b.la_id=h.la_id
                WHERE b.so_id=s.so_id
                  AND h.inv_date < ?
                  AND h.company_id=s.company_id
            ),0) AS invoiced_before
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date <= ?
    """, (from_date, company_id, from_date)).fetchall()

    opening_data = []
    for r in opening_rows:
        ordered = float(r["ordered_qty"] or 0)
        invoiced_before = float(r["invoiced_before"] or 0)
        bal = ordered - invoiced_before
        if bal <= 0:
            continue
        opening_data.append({
            "State": (r["state"] or "").strip().upper(),
            "SO No": r["so_id"],
            "SO Date": r["so_date"],
            "Customer": r["customer"],
            "Balance Qty": bal,
            "Base Rate": float(r["base_rate"] or 0),
            "Status": "OPENING",
        })

    trade_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            h.inv_id,
            h.inv_date,
            h.customer,
            COALESCE(SUM(b.qty),0) AS qty,
            COALESCE(SUM(b.amount),0) AS amount
        FROM sale_invoice_head h
        LEFT JOIN sale_invoice_body b ON h.inv_id=b.inv_id
        LEFT JOIN loading_advice_body lb ON h.la_id=lb.la_id
        LEFT JOIN sale_orders s ON lb.so_id=s.so_id
        LEFT JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.company_id=? AND h.inv_date BETWEEN ? AND ?
        GROUP BY a.state,h.inv_id,h.inv_date,h.customer
        ORDER BY h.inv_date,h.inv_id
    """, (company_id, from_date, to_date)).fetchall()

    trade_data = []
    for r in trade_rows:
        qty = float(r["qty"] or 0)
        amount = float(r["amount"] or 0)
        base_rate = (amount / qty) if qty else 0.0
        trade_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Invoice No": r["inv_id"],
            "Invoice Date": r["inv_date"],
            "Customer": r["customer"],
            "Qty": qty,
            "Base Rate": base_rate,
            "Status": "INVOICED",
        })

    new_so_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            s.so_id,
            s.so_date,
            a.acc_name AS customer,
            s.qty AS ordered_qty,
            s.rate AS base_rate
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date BETWEEN ? AND ?
    """, (company_id, from_date, to_date)).fetchall()

    new_so_data = []
    for r in new_so_rows:
        ordered = float(r["ordered_qty"] or 0)
        new_so_data.append({
            "State": (r["state"] or "").strip().upper(),
            "SO No": r["so_id"],
            "SO Date": r["so_date"],
            "Customer": r["customer"],
            "Balance Qty": ordered,
            "Base Rate": float(r["base_rate"] or 0),
            "Status": "NEW",
        })

    closing_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            s.so_id,
            s.so_date,
            a.acc_name AS customer,
            s.qty AS ordered_qty,
            s.rate AS base_rate,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN sale_invoice_head h ON b.la_id=h.la_id
                WHERE b.so_id=s.so_id
                  AND h.inv_date <= ?
                  AND h.company_id=s.company_id
            ),0) AS invoiced_till
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date <= ?
    """, (to_date, company_id, to_date)).fetchall()

    closing_data = []
    for r in closing_rows:
        ordered = float(r["ordered_qty"] or 0)
        invoiced_till = float(r["invoiced_till"] or 0)
        bal = ordered - invoiced_till
        if bal <= 0:
            continue
        closing_data.append({
            "State": (r["state"] or "").strip().upper(),
            "SO No": r["so_id"],
            "SO Date": r["so_date"],
            "Customer": r["customer"],
            "Balance Qty": bal,
            "Base Rate": float(r["base_rate"] or 0),
            "Status": "CLOSING",
        })

    df_opening = pd.DataFrame(opening_data)
    df_trade = pd.DataFrame(trade_data)
    df_new = pd.DataFrame(new_so_data)
    df_closing = pd.DataFrame(closing_data)

    file = f"sale_movement_{from_date}_to_{to_date}.xlsx"
    with pd.ExcelWriter(file) as writer:
        df_opening.to_excel(writer, sheet_name="Opening", index=False)
        df_trade.to_excel(writer, sheet_name="Trade", index=False)
        df_new.to_excel(writer, sheet_name="New SO", index=False)
        df_closing.to_excel(writer, sheet_name="Closing", index=False)

    return send_file(file, as_attachment=True)


@app.route("/order_timeline")
def order_timeline():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")

    base_params = [company_id, from_date, to_date]

    sales_orders = con.execute("""
        SELECT
            'SALE ORDER' AS tran_type,
            UPPER(COALESCE(a.state,'')) AS state,
            s.so_id AS order_no,
            s.so_date AS order_date,
            a.acc_name AS party,
            s.qty AS qty,
            s.rate AS rate,
            s.status AS raw_status,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN sale_invoice_head h ON b.la_id=h.la_id
                WHERE b.so_id=s.so_id
                  AND h.company_id=s.company_id
            ),0) AS invoiced_qty
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date BETWEEN ? AND ?
    """, base_params).fetchall()

    sale_invoices = con.execute("""
        SELECT
            'SALE INVOICE' AS tran_type,
            UPPER(COALESCE(a.state,'')) AS state,
            h.inv_id AS order_no,
            h.inv_date AS order_date,
            h.customer AS party,
            COALESCE(SUM(b.qty),0) AS qty,
            CASE
                WHEN COALESCE(SUM(b.qty),0) = 0 THEN 0
                ELSE COALESCE(SUM(b.amount),0) / COALESCE(SUM(b.qty),0)
            END AS rate,
            s.so_id AS order_ref,
            s.qty AS order_qty,
            COALESCE((
                SELECT SUM(b2.qty)
                FROM loading_advice_body b2
                JOIN sale_invoice_head h2 ON b2.la_id=h2.la_id
                WHERE b2.so_id=s.so_id
                  AND h2.company_id=s.company_id
            ),0) AS order_done
        FROM sale_invoice_head h
        LEFT JOIN sale_invoice_body b ON h.inv_id=b.inv_id
        LEFT JOIN loading_advice_body lb ON h.la_id=lb.la_id
        LEFT JOIN sale_orders s ON lb.so_id=s.so_id
        LEFT JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.company_id=? AND h.inv_date BETWEEN ? AND ?
        GROUP BY a.state,h.inv_id,h.inv_date,h.customer,s.so_id,s.qty
    """, base_params).fetchall()

    purchase_orders = con.execute("""
        SELECT
            'PURCHASE ORDER' AS tran_type,
            UPPER(COALESCE(a.state,'')) AS state,
            p.po_id AS order_no,
            p.po_date AS order_date,
            a.acc_name AS party,
            p.qty AS qty,
            p.rate AS rate,
            p.status AS raw_status,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN grn_head gh ON b.la_id=gh.la_id
                WHERE b.po_id=p.po_id
                  AND gh.company_id=p.company_id
            ),0) AS received_qty
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date BETWEEN ? AND ?
    """, base_params).fetchall()

    purchase_bills = con.execute("""
        SELECT
            'PURCHASE BILL' AS tran_type,
            UPPER(COALESCE(a.state,'')) AS state,
            gh.grn_id AS order_no,
            gh.grn_date AS order_date,
            gh.supplier AS party,
            COALESCE(SUM(b.qty),0) AS qty,
            CASE
                WHEN COALESCE(SUM(b.qty),0) = 0 THEN 0
                ELSE COALESCE(SUM(b.amount),0) / COALESCE(SUM(b.qty),0)
            END AS rate,
            p.po_id AS order_ref,
            p.qty AS order_qty,
            COALESCE((
                SELECT SUM(b2.qty)
                FROM loading_advice_body b2
                JOIN grn_head gh2 ON b2.la_id=gh2.la_id
                WHERE b2.po_id=p.po_id
                  AND gh2.company_id=p.company_id
            ),0) AS order_done
        FROM grn_head gh
        LEFT JOIN grn_body b ON gh.grn_id=b.grn_id
        LEFT JOIN loading_advice_body lb ON gh.la_id=lb.la_id
        LEFT JOIN purchase_orders p ON lb.po_id=p.po_id
        LEFT JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE gh.company_id=? AND gh.grn_date BETWEEN ? AND ?
        GROUP BY a.state,gh.grn_id,gh.grn_date,gh.supplier,p.po_id,p.qty
    """, base_params).fetchall()

    rows = []

    def map_status(tran_type, raw_status, qty, done_qty):
        raw = (raw_status or "").upper()
        if raw == "DECLINED":
            return "CANCELLED ORDER"
        if tran_type in ("SALE ORDER", "PURCHASE ORDER"):
            q = float(qty or 0)
            d = float(done_qty or 0)
            if q <= 0:
                return "NEW ORDER"
            if d <= 0:
                return "NEW ORDER"
            if d < q:
                return "RUNNING ORDER"
            return "CLOSE ORDER"
        return "POSTED"

    for r in sales_orders:
        bal = float(r["qty"] or 0) - float(r["invoiced_qty"] or 0)
        rows.append({
            "tran_type": r["tran_type"],
            "state": (r["state"] or "").strip().upper(),
            "order_no": r["order_no"],
            "order_date": r["order_date"],
            "party": r["party"],
            "qty": float(r["qty"] or 0),
            "rate": float(r["rate"] or 0),
            "order_ref": r["order_no"],
            "order_balance": bal,
            "status": map_status("SALE ORDER", r["raw_status"], r["qty"], r["invoiced_qty"]),
        })

    for r in sale_invoices:
        order_qty = float(r["order_qty"] or 0)
        order_done = float(r["order_done"] or 0)
        order_bal = order_qty - order_done
        rows.append({
            "tran_type": r["tran_type"],
            "state": (r["state"] or "").strip().upper(),
            "order_no": r["order_no"],
            "order_date": r["order_date"],
            "party": r["party"],
            "qty": float(r["qty"] or 0),
            "rate": float(r["rate"] or 0),
            "order_ref": r["order_ref"],
            "order_balance": order_bal,
            "status": "POSTED",
        })

    for r in purchase_orders:
        bal = float(r["qty"] or 0) - float(r["received_qty"] or 0)
        rows.append({
            "tran_type": r["tran_type"],
            "state": (r["state"] or "").strip().upper(),
            "order_no": r["order_no"],
            "order_date": r["order_date"],
            "party": r["party"],
            "qty": float(r["qty"] or 0),
            "rate": float(r["rate"] or 0),
            "order_ref": r["order_no"],
            "order_balance": bal,
            "status": map_status("PURCHASE ORDER", r["raw_status"], r["qty"], r["received_qty"]),
        })

    for r in purchase_bills:
        order_qty = float(r["order_qty"] or 0)
        order_done = float(r["order_done"] or 0)
        order_bal = order_qty - order_done
        rows.append({
            "tran_type": r["tran_type"],
            "state": (r["state"] or "").strip().upper(),
            "order_no": r["order_no"],
            "order_date": r["order_date"],
            "party": r["party"],
            "qty": float(r["qty"] or 0),
            "rate": float(r["rate"] or 0),
            "order_ref": r["order_ref"],
            "order_balance": order_bal,
            "status": "POSTED",
        })

    rows.sort(key=lambda x: (x["order_date"] or "", str(x["order_no"])))

    return render_template(
        "order_timeline.html",
        rows=rows,
        from_date=from_date,
        to_date=to_date,
    )


@app.route("/export_order_timeline")
def export_order_timeline():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")

    base_params = [company_id, from_date, to_date]

    sales_orders = con.execute("""
        SELECT
            'SALE ORDER' AS tran_type,
            UPPER(COALESCE(a.state,'')) AS state,
            s.so_id AS order_no,
            s.so_date AS order_date,
            a.acc_name AS party,
            s.qty AS qty,
            s.rate AS rate,
            s.status AS raw_status,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN sale_invoice_head h ON b.la_id=h.la_id
                WHERE b.so_id=s.so_id
                  AND h.company_id=s.company_id
            ),0) AS invoiced_qty
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date BETWEEN ? AND ?
    """, base_params).fetchall()

    sale_invoices = con.execute("""
        SELECT
            'SALE INVOICE' AS tran_type,
            UPPER(COALESCE(a.state,'')) AS state,
            h.inv_id AS order_no,
            h.inv_date AS order_date,
            h.customer AS party,
            COALESCE(SUM(b.qty),0) AS qty,
            CASE
                WHEN COALESCE(SUM(b.qty),0) = 0 THEN 0
                ELSE COALESCE(SUM(b.amount),0) / COALESCE(SUM(b.qty),0)
            END AS rate,
            s.so_id AS order_ref,
            s.qty AS order_qty,
            COALESCE((
                SELECT SUM(b2.qty)
                FROM loading_advice_body b2
                JOIN sale_invoice_head h2 ON b2.la_id=h2.la_id
                WHERE b2.so_id=s.so_id
                  AND h2.company_id=s.company_id
            ),0) AS order_done
        FROM sale_invoice_head h
        LEFT JOIN sale_invoice_body b ON h.inv_id=b.inv_id
        LEFT JOIN loading_advice_body lb ON h.la_id=lb.la_id
        LEFT JOIN sale_orders s ON lb.so_id=s.so_id
        LEFT JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.company_id=? AND h.inv_date BETWEEN ? AND ?
        GROUP BY a.state,h.inv_id,h.inv_date,h.customer,s.so_id,s.qty
    """, base_params).fetchall()

    purchase_orders = con.execute("""
        SELECT
            'PURCHASE ORDER' AS tran_type,
            UPPER(COALESCE(a.state,'')) AS state,
            p.po_id AS order_no,
            p.po_date AS order_date,
            a.acc_name AS party,
            p.qty AS qty,
            p.rate AS rate,
            p.status AS raw_status,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN grn_head gh ON b.la_id=gh.la_id
                WHERE b.po_id=p.po_id
                  AND gh.company_id=p.company_id
            ),0) AS received_qty
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date BETWEEN ? AND ?
    """, base_params).fetchall()

    purchase_bills = con.execute("""
        SELECT
            'PURCHASE BILL' AS tran_type,
            UPPER(COALESCE(a.state,'')) AS state,
            gh.grn_id AS order_no,
            gh.grn_date AS order_date,
            gh.supplier AS party,
            COALESCE(SUM(b.qty),0) AS qty,
            CASE
                WHEN COALESCE(SUM(b.qty),0) = 0 THEN 0
                ELSE COALESCE(SUM(b.amount),0) / COALESCE(SUM(b.qty),0)
            END AS rate,
            p.po_id AS order_ref,
            p.qty AS order_qty,
            COALESCE((
                SELECT SUM(b2.qty)
                FROM loading_advice_body b2
                JOIN grn_head gh2 ON b2.la_id=gh2.la_id
                WHERE b2.po_id=p.po_id
                  AND gh2.company_id=p.company_id
            ),0) AS order_done
        FROM grn_head gh
        LEFT JOIN grn_body b ON gh.grn_id=b.grn_id
        LEFT JOIN loading_advice_body lb ON gh.la_id=lb.la_id
        LEFT JOIN purchase_orders p ON lb.po_id=p.po_id
        LEFT JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE gh.company_id=? AND gh.grn_date BETWEEN ? AND ?
        GROUP BY a.state,gh.grn_id,gh.grn_date,gh.supplier,p.po_id,p.qty
    """, base_params).fetchall()

    def map_status(tran_type, raw_status, qty, done_qty):
        raw = (raw_status or "").upper()
        if raw == "DECLINED":
            return "CANCELLED ORDER"
        if tran_type in ("SALE ORDER", "PURCHASE ORDER"):
            q = float(qty or 0)
            d = float(done_qty or 0)
            if q <= 0:
                return "NEW ORDER"
            if d <= 0:
                return "NEW ORDER"
            if d < q:
                return "RUNNING ORDER"
            return "CLOSE ORDER"
        return "POSTED"

    rows = []

    for r in sales_orders:
        bal = float(r["qty"] or 0) - float(r["invoiced_qty"] or 0)
        rows.append({
            "Tran Type": r["tran_type"],
            "State": (r["state"] or "").strip().upper(),
            "Order No": r["order_no"],
            "Order Date": r["order_date"],
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
            "Order Ref": r["order_no"],
            "Order Balance": bal,
            "Status": map_status("SALE ORDER", r["raw_status"], r["qty"], r["invoiced_qty"]),
        })

    for r in sale_invoices:
        order_qty = float(r["order_qty"] or 0)
        order_done = float(r["order_done"] or 0)
        order_bal = order_qty - order_done
        rows.append({
            "Tran Type": r["tran_type"],
            "State": (r["state"] or "").strip().upper(),
            "Order No": r["order_no"],
            "Order Date": r["order_date"],
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
            "Order Ref": r["order_ref"],
            "Order Balance": order_bal,
            "Status": "POSTED",
        })

    for r in purchase_orders:
        bal = float(r["qty"] or 0) - float(r["received_qty"] or 0)
        rows.append({
            "Tran Type": r["tran_type"],
            "State": (r["state"] or "").strip().upper(),
            "Order No": r["order_no"],
            "Order Date": r["order_date"],
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
            "Order Ref": r["order_no"],
            "Order Balance": bal,
            "Status": map_status("PURCHASE ORDER", r["raw_status"], r["qty"], r["received_qty"]),
        })

    for r in purchase_bills:
        order_qty = float(r["order_qty"] or 0)
        order_done = float(r["order_done"] or 0)
        order_bal = order_qty - order_done
        rows.append({
            "Tran Type": r["tran_type"],
            "State": (r["state"] or "").strip().upper(),
            "Order No": r["order_no"],
            "Order Date": r["order_date"],
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
            "Order Ref": r["order_ref"],
            "Order Balance": order_bal,
            "Status": "POSTED",
        })

    rows.sort(key=lambda x: (x["Order Date"] or "", str(x["Order No"])))

    df = pd.DataFrame(rows)
    file = f"order_timeline_{from_date}_to_{to_date}.xlsx"
    with pd.ExcelWriter(file) as writer:
        df.to_excel(writer, sheet_name="Timeline", index=False)

    return send_file(file, as_attachment=True)


@app.route("/order_balance_sheet")
def order_balance_sheet():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    from_arg = request.args.get("from")
    to_arg = request.args.get("to")

    if not from_arg or not to_arg:
        so_minmax = con.execute(
            "SELECT MIN(inv_date) AS min_d, MAX(inv_date) AS max_d FROM sale_invoice_head",
        ).fetchone()
        po_minmax = con.execute(
            "SELECT MIN(grn_date) AS min_d, MAX(grn_date) AS max_d FROM grn_head",
        ).fetchone()
        dates = []
        if so_minmax and so_minmax["min_d"]:
            dates.append(so_minmax["min_d"])
            dates.append(so_minmax["max_d"])
        if po_minmax and po_minmax["min_d"]:
            dates.append(po_minmax["min_d"])
            dates.append(po_minmax["max_d"])
        if dates:
            from_date = min(dates)
            to_date = max(dates)
        else:
            from_date = today_str
            to_date = today_str
    else:
        from_date = from_arg
        to_date = to_arg

    sales_exec = con.execute("""
        SELECT
            h.inv_date AS exec_date,
            s.so_id,
            s.so_date,
            a.acc_name AS party,
            s.qty AS order_qty,
            s.status AS raw_status,
            COALESCE(SUM(b.qty),0) AS exec_qty_day
        FROM sale_invoice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.inv_date BETWEEN ? AND ?
        GROUP BY h.inv_date,s.so_id,s.so_date,a.acc_name,s.qty,s.status
    """, (from_date, to_date)).fetchall()

    purch_exec = con.execute("""
        SELECT
            gh.grn_date AS exec_date,
            p.po_id,
            p.po_date,
            a.acc_name AS party,
            p.qty AS order_qty,
            p.status AS raw_status,
            COALESCE(SUM(b.qty),0) AS exec_qty_day
        FROM grn_head gh
        JOIN loading_advice_body b ON gh.la_id=b.la_id
        JOIN purchase_orders p ON b.po_id=p.po_id
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE gh.grn_date BETWEEN ? AND ?
        GROUP BY gh.grn_date,p.po_id,p.po_date,a.acc_name,p.qty,p.status
    """, (from_date, to_date)).fetchall()

    sales_orders = {}
    for r in sales_exec:
        so_id = r["so_id"]
        if so_id not in sales_orders:
            sales_orders[so_id] = {
                "order_qty": float(r["order_qty"] or 0),
                "events": [],
            }
        sales_orders[so_id]["events"].append(
            (r["exec_date"], float(r["exec_qty_day"] or 0))
        )

    purch_orders = {}
    for r in purch_exec:
        po_id = r["po_id"]
        if po_id not in purch_orders:
            purch_orders[po_id] = {
                "order_qty": float(r["order_qty"] or 0),
                "events": [],
            }
        purch_orders[po_id]["events"].append(
            (r["exec_date"], float(r["exec_qty_day"] or 0))
        )

    date_map = {}

    for so_id, info in sales_orders.items():
        order_qty = info["order_qty"]
        events = sorted(info["events"], key=lambda x: x[0])
        cum = 0.0
        for exec_date, day_qty in events:
            cum += day_qty
            bal = order_qty - cum
            d = exec_date
            if d not in date_map:
                date_map[d] = {
                    "date": d,
                    "sales_order_qty": 0.0,
                    "sales_exec_qty": 0.0,
                    "sales_balance_qty": 0.0,
                    "purch_order_qty": 0.0,
                    "purch_exec_qty": 0.0,
                    "purch_balance_qty": 0.0,
                }
            date_map[d]["sales_order_qty"] += order_qty
            date_map[d]["sales_exec_qty"] += day_qty
            date_map[d]["sales_balance_qty"] += bal

    for po_id, info in purch_orders.items():
        order_qty = info["order_qty"]
        events = sorted(info["events"], key=lambda x: x[0])
        cum = 0.0
        for exec_date, day_qty in events:
            cum += day_qty
            bal = order_qty - cum
            d = exec_date
            if d not in date_map:
                date_map[d] = {
                    "date": d,
                    "sales_order_qty": 0.0,
                    "sales_exec_qty": 0.0,
                    "sales_balance_qty": 0.0,
                    "purch_order_qty": 0.0,
                    "purch_exec_qty": 0.0,
                    "purch_balance_qty": 0.0,
                }
            date_map[d]["purch_order_qty"] += order_qty
            date_map[d]["purch_exec_qty"] += day_qty
            date_map[d]["purch_balance_qty"] += bal

    rows = sorted(date_map.values(), key=lambda x: x["date"])

    return render_template(
        "order_balance_sheet.html",
        rows=rows,
        from_date=from_date,
        to_date=to_date,
    )


@app.route("/order_balance_sheet_detail")
def order_balance_sheet_detail():
    if "user" not in session:
        return redirect("/")
    side = request.args.get("side","sales")
    date = request.args.get("date")
    if not date:
        return redirect("/order_balance_sheet")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")

    def map_status(qty, exec_qty, raw_status):
        raw = (raw_status or "").upper()
        if raw == "DECLINED":
            return "CANCELLED ORDER"
        q = float(qty or 0)
        d = float(exec_qty or 0)
        if q <= 0:
            return "NEW ORDER"
        if d <= 0:
            return "NEW ORDER"
        if d < q:
            return "RUNNING ORDER"
        return "CLOSE ORDER"

    rows = []

    if side == "sales":
        data = con.execute("""
            SELECT
                UPPER(COALESCE(a.state,'')) AS state,
                s.so_id AS order_no,
                s.so_date AS order_date,
                a.acc_name AS party,
                s.qty AS ordered_qty,
                s.status AS raw_status,
                COALESCE((
                    SELECT SUM(b.qty)
                    FROM loading_advice_body b
                    JOIN sale_invoice_head h ON b.la_id=h.la_id
                    WHERE b.so_id=s.so_id
                      AND h.inv_date=?
                ),0) AS exec_qty_day,
                COALESCE((
                    SELECT SUM(b2.qty)
                    FROM loading_advice_body b2
                    JOIN sale_invoice_head h2 ON b2.la_id=h2.la_id
                    WHERE b2.so_id=s.so_id
                      AND h2.inv_date<=?
                ),0) AS exec_qty_till
            FROM sale_orders s
            JOIN loading_advice_body b0 ON s.so_id=b0.so_id
            JOIN sale_invoice_head h0 ON b0.la_id=h0.la_id
            JOIN acc_mast a ON s.acc_id=a.acc_id
            WHERE h0.inv_date=?
            GROUP BY a.state,s.so_id,s.so_date,a.acc_name,s.qty,s.status
        """, (date, date, date)).fetchall()
        for r in data:
            ordered = float(r["ordered_qty"] or 0)
            exec_day = float(r["exec_qty_day"] or 0)
            exec_till = float(r["exec_qty_till"] or 0)
            bal = ordered - exec_till
            rows.append({
                "state": (r["state"] or "").strip().upper(),
                "order_no": r["order_no"],
                "order_date": r["order_date"],
                "party": r["party"],
                "order_qty": ordered,
                "exec_qty": exec_day,
                "balance_qty": bal,
                "status": map_status(ordered, exec_till, r["raw_status"]),
            })
    else:
        data = con.execute("""
            SELECT
                UPPER(COALESCE(a.state,'')) AS state,
                p.po_id AS order_no,
                p.po_date AS order_date,
                a.acc_name AS party,
                p.qty AS ordered_qty,
                p.status AS raw_status,
                COALESCE((
                    SELECT SUM(b.qty)
                    FROM loading_advice_body b
                    JOIN grn_head gh ON b.la_id=gh.la_id
                    WHERE b.po_id=p.po_id
                      AND gh.grn_date=?
                ),0) AS exec_qty_day,
                COALESCE((
                    SELECT SUM(b2.qty)
                    FROM loading_advice_body b2
                    JOIN grn_head gh2 ON b2.la_id=gh2.la_id
                    WHERE b2.po_id=p.po_id
                      AND gh2.grn_date<=?
                ),0) AS exec_qty_till
            FROM purchase_orders p
            JOIN loading_advice_body b0 ON p.po_id=b0.po_id
            JOIN grn_head gh0 ON b0.la_id=gh0.la_id
            JOIN acc_mast a ON p.acc_id=a.acc_id
            WHERE gh0.grn_date=?
            GROUP BY a.state,p.po_id,p.po_date,a.acc_name,p.qty,p.status
        """, (date, date, date)).fetchall()
        for r in data:
            ordered = float(r["ordered_qty"] or 0)
            exec_day = float(r["exec_qty_day"] or 0)
            exec_till = float(r["exec_qty_till"] or 0)
            bal = ordered - exec_till
            rows.append({
                "state": (r["state"] or "").strip().upper(),
                "order_no": r["order_no"],
                "order_date": r["order_date"],
                "party": r["party"],
                "order_qty": ordered,
                "exec_qty": exec_day,
                "balance_qty": bal,
                "status": map_status(ordered, exec_till, r["raw_status"]),
            })

    return render_template(
        "order_balance_detail.html",
        side=side,
        date=date,
        from_date=from_date,
        to_date=to_date,
        rows=rows,
    )


@app.route("/export_order_balance")
def export_order_balance():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")

    sales_open_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            s.so_id,
            s.so_date,
            a.acc_name AS customer,
            s.qty AS ordered_qty,
            s.rate AS base_rate,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN sale_invoice_head h ON b.la_id=h.la_id
                WHERE b.so_id=s.so_id
                  AND h.inv_date < ?
                  AND h.company_id=s.company_id
            ),0) AS invoiced_before
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date <= ?
    """, (from_date, company_id, from_date)).fetchall()

    sales_open_data = []
    for r in sales_open_rows:
        ordered = float(r["ordered_qty"] or 0)
        invoiced_before = float(r["invoiced_before"] or 0)
        bal = ordered - invoiced_before
        if bal <= 0:
            continue
        sales_open_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Sales Order No": r["so_id"],
            "Sales Order Date": r["so_date"],
            "Customer Name": r["customer"],
            "Balance Quantity": bal,
            "Rate": float(r["base_rate"] or 0),
        })

    sales_tx_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            h.inv_id,
            h.inv_date,
            s.so_id,
            s.so_date,
            a.acc_name AS customer,
            COALESCE(SUM(b.qty),0) AS txn_qty,
            s.rate AS base_rate
        FROM sale_invoice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.company_id=? AND h.inv_date BETWEEN ? AND ?
        GROUP BY a.state,h.inv_id,h.inv_date,s.so_id,s.so_date,a.acc_name,s.rate
        ORDER BY h.inv_date,h.inv_id,s.so_id
    """, (company_id, from_date, to_date)).fetchall()

    sales_tx_data = []
    for r in sales_tx_rows:
        sales_tx_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Sales Invoice No": r["inv_id"],
            "Invoice Date": r["inv_date"],
            "Sales Order No": r["so_id"],
            "Sales Order Date": r["so_date"],
            "Customer Name": r["customer"],
            "Transaction Quantity": float(r["txn_qty"] or 0),
            "Base Rate": float(r["base_rate"] or 0),
        })

    sales_new_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            s.so_id,
            s.so_date,
            a.acc_name AS customer,
            s.qty AS booked_qty,
            s.rate AS base_rate
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date BETWEEN ? AND ?
        ORDER BY s.so_date,s.so_id
    """, (company_id, from_date, to_date)).fetchall()

    sales_new_data = []
    for r in sales_new_rows:
        sales_new_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Sales Order No": r["so_id"],
            "Sales Order Date": r["so_date"],
            "Customer Name": r["customer"],
            "Booked Quantity": float(r["booked_qty"] or 0),
            "Base Rate": float(r["base_rate"] or 0),
        })

    sales_close_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            s.so_id,
            s.so_date,
            a.acc_name AS customer,
            s.qty AS ordered_qty,
            s.rate AS base_rate,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN sale_invoice_head h ON b.la_id=h.la_id
                WHERE b.so_id=s.so_id
                  AND h.inv_date <= ?
                  AND h.company_id=s.company_id
            ),0) AS invoiced_till
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date <= ?
    """, (to_date, company_id, to_date)).fetchall()

    sales_close_data = []
    for r in sales_close_rows:
        ordered = float(r["ordered_qty"] or 0)
        invoiced_till = float(r["invoiced_till"] or 0)
        bal = ordered - invoiced_till
        if ordered == 0:
            continue
        if abs(bal) > 1e-6:
            continue
        sales_close_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Sales Order No": r["so_id"],
            "Sales Order Date": r["so_date"],
            "Customer Name": r["customer"],
            "Closing Quantity": 0.0,
            "Rate": float(r["base_rate"] or 0),
        })

    purch_open_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            p.po_id,
            p.po_date,
            a.acc_name AS supplier,
            p.qty AS ordered_qty,
            p.rate AS base_rate,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN grn_head gh ON b.la_id=gh.la_id
                WHERE b.po_id=p.po_id
                  AND gh.grn_date < ?
                  AND gh.company_id=p.company_id
            ),0) AS received_before
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date <= ?
    """, (from_date, company_id, from_date)).fetchall()

    purch_open_data = []
    for r in purch_open_rows:
        ordered = float(r["ordered_qty"] or 0)
        received_before = float(r["received_before"] or 0)
        bal = ordered - received_before
        if bal <= 0:
            continue
        purch_open_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Purchase Order No": r["po_id"],
            "Purchase Order Date": r["po_date"],
            "Supplier Name": r["supplier"],
            "Balance Quantity": bal,
            "Rate": float(r["base_rate"] or 0),
        })

    purch_tx_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            gh.grn_id,
            gh.grn_date,
            p.po_id,
            p.po_date,
            a.acc_name AS supplier,
            COALESCE(SUM(b.qty),0) AS txn_qty,
            p.rate AS base_rate
        FROM grn_head gh
        JOIN loading_advice_body b ON gh.la_id=b.la_id
        JOIN purchase_orders p ON b.po_id=p.po_id
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE gh.company_id=? AND gh.grn_date BETWEEN ? AND ?
        GROUP BY a.state,gh.grn_id,gh.grn_date,p.po_id,p.po_date,a.acc_name,p.rate
        ORDER BY gh.grn_date,gh.grn_id,p.po_id
    """, (company_id, from_date, to_date)).fetchall()

    purch_tx_data = []
    for r in purch_tx_rows:
        purch_tx_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Purchase Invoice No": r["grn_id"],
            "Invoice Date": r["grn_date"],
            "Purchase Order No": r["po_id"],
            "Purchase Order Date": r["po_date"],
            "Supplier Name": r["supplier"],
            "Transaction Quantity": float(r["txn_qty"] or 0),
            "Base Rate": float(r["base_rate"] or 0),
        })

    purch_new_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            p.po_id,
            p.po_date,
            a.acc_name AS supplier,
            p.qty AS booked_qty,
            p.rate AS base_rate
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date BETWEEN ? AND ?
        ORDER BY p.po_date,p.po_id
    """, (company_id, from_date, to_date)).fetchall()

    purch_new_data = []
    for r in purch_new_rows:
        purch_new_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Purchase Order No": r["po_id"],
            "Purchase Order Date": r["po_date"],
            "Supplier Name": r["supplier"],
            "Booked Quantity": float(r["booked_qty"] or 0),
            "Base Rate": float(r["base_rate"] or 0),
        })

    purch_close_rows = con.execute("""
        SELECT
            UPPER(COALESCE(a.state,'')) AS state,
            p.po_id,
            p.po_date,
            a.acc_name AS supplier,
            p.qty AS ordered_qty,
            p.rate AS base_rate,
            COALESCE((
                SELECT SUM(b.qty)
                FROM loading_advice_body b
                JOIN grn_head gh ON b.la_id=gh.la_id
                WHERE b.po_id=p.po_id
                  AND gh.grn_date <= ?
                  AND gh.company_id=p.company_id
            ),0) AS received_till
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date <= ?
    """, (to_date, company_id, to_date)).fetchall()

    purch_close_data = []
    for r in purch_close_rows:
        ordered = float(r["ordered_qty"] or 0)
        received_till = float(r["received_till"] or 0)
        bal = ordered - received_till
        if ordered == 0:
            continue
        if abs(bal) > 1e-6:
            continue
        purch_close_data.append({
            "State": (r["state"] or "").strip().upper(),
            "Purchase Order No": r["po_id"],
            "Purchase Order Date": r["po_date"],
            "Supplier Name": r["supplier"],
            "Closing Quantity": 0.0,
            "Rate": float(r["base_rate"] or 0),
        })

    df_sales_open = pd.DataFrame(sales_open_data)
    df_sales_tx = pd.DataFrame(sales_tx_data)
    df_sales_new = pd.DataFrame(sales_new_data)
    df_sales_close = pd.DataFrame(sales_close_data)
    df_purch_open = pd.DataFrame(purch_open_data)
    df_purch_tx = pd.DataFrame(purch_tx_data)
    df_purch_new = pd.DataFrame(purch_new_data)
    df_purch_close = pd.DataFrame(purch_close_data)

    if not df_sales_tx.empty:
        df_sales_tx_tmp = df_sales_tx.copy()
        df_sales_tx_tmp["Amount"] = df_sales_tx_tmp["Transaction Quantity"] * df_sales_tx_tmp["Base Rate"]
        grouped_sales = df_sales_tx_tmp.groupby(["State", "Customer Name"], as_index=False).agg({
            "Transaction Quantity": "sum",
            "Amount": "sum",
        })
        grouped_sales["Avg Rate"] = grouped_sales.apply(
            lambda row: (row["Amount"] / row["Transaction Quantity"]) if row["Transaction Quantity"] else 0.0,
            axis=1,
        )
        df_sales_summary = grouped_sales[["State", "Customer Name", "Transaction Quantity", "Avg Rate", "Amount"]]
    else:
        df_sales_summary = pd.DataFrame(columns=["State", "Customer Name", "Transaction Quantity", "Avg Rate", "Amount"])

    if not df_purch_tx.empty:
        df_purch_tx_tmp = df_purch_tx.copy()
        df_purch_tx_tmp["Amount"] = df_purch_tx_tmp["Transaction Quantity"] * df_purch_tx_tmp["Base Rate"]
        grouped_purch = df_purch_tx_tmp.groupby(["State", "Supplier Name"], as_index=False).agg({
            "Transaction Quantity": "sum",
            "Amount": "sum",
        })
        grouped_purch["Avg Rate"] = grouped_purch.apply(
            lambda row: (row["Amount"] / row["Transaction Quantity"]) if row["Transaction Quantity"] else 0.0,
            axis=1,
        )
        df_purch_summary = grouped_purch[["State", "Supplier Name", "Transaction Quantity", "Avg Rate", "Amount"]]
    else:
        df_purch_summary = pd.DataFrame(columns=["State", "Supplier Name", "Transaction Quantity", "Avg Rate", "Amount"])

    file = f"order_balance_{from_date}_to_{to_date}.xlsx"
    with pd.ExcelWriter(file) as writer:
        df_sales_open.to_excel(writer, sheet_name="Sales Opening", index=False)
        df_sales_tx.to_excel(writer, sheet_name="Sales Transactions", index=False)
        df_sales_new.to_excel(writer, sheet_name="Sales New Orders", index=False)
        df_sales_close.to_excel(writer, sheet_name="Sales Closing", index=False)
        df_purch_open.to_excel(writer, sheet_name="Purchase Opening", index=False)
        df_purch_tx.to_excel(writer, sheet_name="Purchase Transactions", index=False)
        df_purch_new.to_excel(writer, sheet_name="Purchase New Orders", index=False)
        df_purch_close.to_excel(writer, sheet_name="Purchase Closing", index=False)
        df_sales_summary.to_excel(writer, sheet_name="Sales State-Party", index=False)
        df_purch_summary.to_excel(writer, sheet_name="Purch State-Party", index=False)

    return send_file(file, as_attachment=True)


@app.route("/export_order_flow_pivot")
def export_order_flow_pivot():
    if "user" not in session:
        return redirect("/")
    con = db()
    today_str = today()
    fy_start = session.get("fy_start")
    fy_end = session.get("fy_end")
    from_date = request.args.get("from") or fy_start or today_str
    to_date = request.args.get("to") or fy_end or today_str
    company_id = session.get("company_id")

    rows = []

    sales_orders = con.execute("""
        SELECT
            s.so_id,
            s.so_date,
            a.acc_name AS party,
            UPPER(COALESCE(a.state,'')) AS state,
            s.qty,
            s.rate
        FROM sale_orders s
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE s.company_id=? AND s.so_date BETWEEN ? AND ?
    """, (company_id, from_date, to_date)).fetchall()

    for r in sales_orders:
        rows.append({
            "Side": "SALES",
            "Event": "ORDER",
            "Tran Date": r["so_date"],
            "Order No": r["so_id"],
            "Order Date": r["so_date"],
            "Doc Type": "SO",
            "Doc No": r["so_id"],
            "Doc Date": r["so_date"],
            "State": (r["state"] or "").strip().upper(),
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
        })

    purch_orders = con.execute("""
        SELECT
            p.po_id,
            p.po_date,
            a.acc_name AS party,
            UPPER(COALESCE(a.state,'')) AS state,
            p.qty,
            p.rate
        FROM purchase_orders p
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE p.company_id=? AND p.po_date BETWEEN ? AND ?
    """, (company_id, from_date, to_date)).fetchall()

    for r in purch_orders:
        rows.append({
            "Side": "PURCHASE",
            "Event": "ORDER",
            "Tran Date": r["po_date"],
            "Order No": r["po_id"],
            "Order Date": r["po_date"],
            "Doc Type": "PO",
            "Doc No": r["po_id"],
            "Doc Date": r["po_date"],
            "State": (r["state"] or "").strip().upper(),
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
        })

    sales_delivered = con.execute("""
        SELECT
            h.la_id,
            h.la_date,
            s.so_id,
            s.so_date,
            a.acc_name AS party,
            UPPER(COALESCE(a.state,'')) AS state,
            b.qty,
            s.rate
        FROM loading_advice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.company_id=? AND h.la_date BETWEEN ? AND ?
    """, (company_id, from_date, to_date)).fetchall()

    for r in sales_delivered:
        rows.append({
            "Side": "SALES",
            "Event": "DELIVERED",
            "Tran Date": r["la_date"],
            "Order No": r["so_id"],
            "Order Date": r["so_date"],
            "Doc Type": "LA",
            "Doc No": r["la_id"],
            "Doc Date": r["la_date"],
            "State": (r["state"] or "").strip().upper(),
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
        })

    purch_delivered = con.execute("""
        SELECT
            h.la_id,
            h.la_date,
            p.po_id,
            p.po_date,
            a.acc_name AS party,
            UPPER(COALESCE(a.state,'')) AS state,
            b.qty,
            p.rate
        FROM loading_advice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        JOIN purchase_orders p ON b.po_id=p.po_id
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE h.company_id=? AND h.la_date BETWEEN ? AND ?
    """, (company_id, from_date, to_date)).fetchall()

    for r in purch_delivered:
        rows.append({
            "Side": "PURCHASE",
            "Event": "DELIVERED",
            "Tran Date": r["la_date"],
            "Order No": r["po_id"],
            "Order Date": r["po_date"],
            "Doc Type": "LA",
            "Doc No": r["la_id"],
            "Doc Date": r["la_date"],
            "State": (r["state"] or "").strip().upper(),
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
        })

    sales_invoiced = con.execute("""
        SELECT
            h.inv_id,
            h.inv_date,
            s.so_id,
            s.so_date,
            a.acc_name AS party,
            UPPER(COALESCE(a.state,'')) AS state,
            COALESCE(SUM(b.qty),0) AS qty,
            s.rate
        FROM sale_invoice_head h
        JOIN loading_advice_body b ON h.la_id=b.la_id
        JOIN sale_orders s ON b.so_id=s.so_id
        JOIN acc_mast a ON s.acc_id=a.acc_id
        WHERE h.company_id=? AND h.inv_date BETWEEN ? AND ?
        GROUP BY h.inv_id,h.inv_date,s.so_id,s.so_date,a.acc_name,a.state,s.rate
    """, (company_id, from_date, to_date)).fetchall()

    for r in sales_invoiced:
        rows.append({
            "Side": "SALES",
            "Event": "INVOICED",
            "Tran Date": r["inv_date"],
            "Order No": r["so_id"],
            "Order Date": r["so_date"],
            "Doc Type": "INV",
            "Doc No": r["inv_id"],
            "Doc Date": r["inv_date"],
            "State": (r["state"] or "").strip().upper(),
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
        })

    purch_invoiced = con.execute("""
        SELECT
            gh.grn_id,
            gh.grn_date,
            p.po_id,
            p.po_date,
            a.acc_name AS party,
            UPPER(COALESCE(a.state,'')) AS state,
            COALESCE(SUM(b.qty),0) AS qty,
            p.rate
        FROM grn_head gh
        JOIN loading_advice_body b ON gh.la_id=b.la_id
        JOIN purchase_orders p ON b.po_id=p.po_id
        JOIN acc_mast a ON p.acc_id=a.acc_id
        WHERE gh.company_id=? AND gh.grn_date BETWEEN ? AND ?
        GROUP BY gh.grn_id,gh.grn_date,p.po_id,p.po_date,a.acc_name,a.state,p.rate
    """, (company_id, from_date, to_date)).fetchall()

    for r in purch_invoiced:
        rows.append({
            "Side": "PURCHASE",
            "Event": "INVOICED",
            "Tran Date": r["grn_date"],
            "Order No": r["po_id"],
            "Order Date": r["po_date"],
            "Doc Type": "GRN",
            "Doc No": r["grn_id"],
            "Doc Date": r["grn_date"],
            "State": (r["state"] or "").strip().upper(),
            "Party": r["party"],
            "Qty": float(r["qty"] or 0),
            "Rate": float(r["rate"] or 0),
        })

    rows.sort(key=lambda x: (x["Tran Date"] or "", x["Side"], x["Event"], str(x["Order No"]), str(x["Doc No"])))

    df = pd.DataFrame(rows)
    file = f"order_flow_pivot_{from_date}_to_{to_date}.xlsx"
    with pd.ExcelWriter(file) as writer:
        df.to_excel(writer, sheet_name="Flow", index=False)

    return send_file(file, as_attachment=True)

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
    audit(con, "sale_orders", "DELETE", id)
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
    audit(con, "purchase_orders", "DELETE", id)
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
        audit(con, "sale_orders", "UPDATE", id, f"amount={amt}")
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
        audit(con, "purchase_orders", "UPDATE", id, f"amount={amt}")
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
