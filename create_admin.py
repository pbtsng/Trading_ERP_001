import sqlite3
from werkzeug.security import generate_password_hash

con = sqlite3.connect("database/trading.db")
cur = con.cursor()

username = "admin"
password = "1234"
role = "admin"

cur.execute("""
INSERT INTO users_mast (user_code,username,password_hash,role)
VALUES (?,?,?,?)
""", ("U001", username, generate_password_hash(password), role))

con.commit()
con.close()

print("Admin user created")
print("Username:", username)
print("Password:", password)
