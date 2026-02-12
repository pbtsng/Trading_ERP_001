try:
    from openai import OpenAI
except Exception:
    OpenAI = None
import sqlite3
import os
import pandas as pd
import typing

client = None
api_key = os.environ.get("OPENAI_API_KEY")
if OpenAI and api_key:
    try:
        client = OpenAI(api_key=api_key)
    except Exception:
        client = None
DB_PATH = "database/trading.db"

def get_db():
    return sqlite3.connect(DB_PATH)

def question_to_sql(question):
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
    if not client:
        return "SELECT 'OpenAI API key not configured' AS error"
    response = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}])
    return response.choices[0].message.content.strip()

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
