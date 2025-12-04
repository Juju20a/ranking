import sqlite3
import os

DB_CANDIDATES = ['censoescolar.db', 'censo_escolar.db']

for db in DB_CANDIDATES:
    print('---')
    print('DB candidate:', db)
    if not os.path.exists(db):
        print('  Not found')
        continue
    abspath = os.path.abspath(db)
    size = os.path.getsize(db)
    print('  Path:', abspath)
    print('  Size (bytes):', size)
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cur.fetchall()]
        print('  Tables:', tables)
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                cnt = cur.fetchone()[0]
            except Exception as e:
                cnt = f'error: {e}'
            print(f"    {t}: {cnt}")
    except Exception as e:
        print('  Error reading schema:', e)
    conn.close()
