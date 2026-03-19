try:
    from openai import OpenAI
except Exception:
    OpenAI = None
try:
    import google.generativeai as genai
except Exception:
    genai = None
import sqlite3
import os
import pandas as pd
import typing

openai_client = None
gemini_model = None

def _init_openai():
    global openai_client
    if openai_client is not None:
        return
    key = os.environ.get("OPENAI_API_KEY")
    if OpenAI and key:
        try:
            openai_client = OpenAI(api_key=key)
        except Exception:
            openai_client = None

def _init_gemini():
    global gemini_model
    if gemini_model is not None:
        return
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not key:
        try:
            con = get_db()
            try:
                row = con.execute("SELECT gemini_key FROM ai_settings WHERE id=1").fetchone()
            finally:
                con.close()
            if row and row[0]:
                key = row[0]
        except Exception:
            key = None
    if genai and key:
        try:
            genai.configure(api_key=key)
            gemini_model = genai.GenerativeModel("gemini-2.0-flash")
        except Exception:
            gemini_model = None
DB_PATH = "database/trading.db"

def get_db():
    return sqlite3.connect(DB_PATH)

def question_to_sql(question, provider: str = "auto"):
    con = get_db()
    tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    schema_parts = []
    for t in tables:
        cols = con.execute(f"PRAGMA table_info('{t}')").fetchall()
        col_list = ", ".join([c[1] for c in cols])
        schema_parts.append(f"{t}({col_list})")
    con.close()
    schema = "\n".join(schema_parts)
    prompt = f"You are an expert SQLite developer.\n\nDatabase schema:\n{schema}\n\nConvert this question into valid, read-only SQLite SQL. Use SELECT only. Return only SQL.\n\nQuestion: {question}"
    prov = (provider or "auto").lower()

    if prov == "openai":
        _init_openai()
        if not openai_client:
            return "SELECT 'OpenAI API key not configured' AS error"
        response = openai_client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}])
        return response.choices[0].message.content.strip()

    if prov == "gemini":
        _init_gemini()
        if not gemini_model:
            return "SELECT 'Gemini API key not configured' AS error"
        try:
            resp = gemini_model.generate_content(prompt)
        except Exception as e:
            msg = str(e)
            if "quota" in msg.lower() or "429" in msg:
                _init_openai()
                if openai_client:
                    response = openai_client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}])
                    return response.choices[0].message.content.strip()
                return "SELECT 'Gemini quota exceeded, please check plan and billing' AS error"
            safe_msg = msg.replace("'", " ")
            return "SELECT 'Gemini error: " + safe_msg + "' AS error"
        text = ""
        try:
            text = resp.text or ""
        except Exception:
            text = ""
        return text.strip() or "SELECT 'Gemini did not return SQL' AS error"

    _init_openai()
    if openai_client:
        response = openai_client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}])
        return response.choices[0].message.content.strip()

    _init_gemini()
    if gemini_model:
        resp = gemini_model.generate_content(prompt)
        text = ""
        try:
            text = resp.text or ""
        except Exception:
            text = ""
        return text.strip() or "SELECT 'Gemini did not return SQL' AS error"

    return "SELECT 'No AI provider configured' AS error"

def run_sql(sql: str) -> pd.DataFrame:
    s = (sql or "").strip().lower()
    if not s.startswith("select"):
        return pd.DataFrame({"error": ["Only SELECT queries are allowed"]})
    con = get_db()
    try:
        df = pd.read_sql_query(sql, con)
        return df
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})
    finally:
        con.close()

def explain_result(df: pd.DataFrame) -> str:
    if "error" in df.columns and len(df) == 1:
        return str(df.iloc[0]["error"])
    rows = len(df)
    cols = list(df.columns)
    if rows == 0:
        return "No rows found."
    head = df.head(5)
    preview = head.to_string(index=False)
    return f"Rows: {rows}\nColumns: {', '.join(cols)}\nPreview:\n{preview}"
