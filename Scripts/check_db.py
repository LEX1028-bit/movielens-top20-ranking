from pathlib import Path
import sqlite3

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "data" / "movielens.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("DB file:", DB_PATH)
print("DB exists:", DB_PATH.exists())

# 1) 列出所有表
tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()
print("Tables:", tables)

# 2) 每张表多少行
for (t,) in tables:
    n = cur.execute(f"SELECT COUNT(*) FROM {t};").fetchone()[0]
    print(f"{t}: {n} rows")

conn.close()
