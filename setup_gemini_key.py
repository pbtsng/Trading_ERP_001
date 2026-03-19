import sqlite3
import os

DB_PATH = "database/trading.db"

def setup_gemini_key():
    con = sqlite3.connect(DB_PATH)
    cursor = con.cursor()
    
    # Create ai_settings table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_settings (
            id INTEGER PRIMARY KEY,
            gemini_key TEXT
        )
    """)
    
    # Check if row exists
    row = cursor.execute("SELECT id FROM ai_settings WHERE id=1").fetchone()
    
    # Get API key from user
    api_key = input("Enter your Gemini API key: ").strip()
    
    if row:
        cursor.execute("UPDATE ai_settings SET gemini_key=? WHERE id=1", (api_key,))
    else:
        cursor.execute("INSERT INTO ai_settings (id, gemini_key) VALUES (1, ?)", (api_key,))
    
    con.commit()
    con.close()
    print("✓ Gemini API key configured successfully!")

if __name__ == "__main__":
    setup_gemini_key()
