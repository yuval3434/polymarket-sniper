import sqlite3

conn = sqlite3.connect("trading_bot.db")
c = conn.cursor()

# List tables
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("Tables:", c.fetchall())

# Get recent trades
try:
    c.execute("SELECT * FROM trades ORDER BY rowid DESC LIMIT 5")
    cols = [d[0] for d in c.description]
    print("\nColumns:", cols)
    for row in c.fetchall():
        print(dict(zip(cols, row)))
except Exception as e:
    print(f"trades table error: {e}")

# Check shadow trades too
try:
    c.execute("SELECT * FROM shadow_trades ORDER BY rowid DESC LIMIT 5")
    cols = [d[0] for d in c.description]
    print("\nShadow trades columns:", cols)
    for row in c.fetchall():
        print(dict(zip(cols, row)))
except Exception as e:
    print(f"shadow_trades error: {e}")

conn.close()
