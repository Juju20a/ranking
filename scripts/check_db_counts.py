import sqlite3

DB = 'censoescolar.db'

conn = sqlite3.connect(DB)
cur = conn.cursor()

for t in ['tb_instituicao', 'tb_instituicao_year', 'tb_usuario']:
    try:
        cur.execute(f"SELECT COUNT(1) FROM {t}")
        print(t, cur.fetchone()[0])
    except Exception as e:
        print(t, 'error:', e)

conn.close()
