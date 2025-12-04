import sqlite3

DB = 'censoescolar.db'

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    print('Tables found:', tables)
    for t in tables:
        name = t[0]
        try:
            cur.execute(f"SELECT COUNT(*) FROM {name}")
            cnt = cur.fetchone()[0]
        except Exception as e:
            cnt = f'error: {e}'
        print(f"{name}: {cnt}")
    conn.close()

if __name__ == '__main__':
    main()
