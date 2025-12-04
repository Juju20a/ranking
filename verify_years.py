#!/usr/bin/env python
import sqlite3

# Conectar ao banco
c = sqlite3.connect('censoescolar.db')
cur = c.cursor()

# Verificar anos disponíveis
cur.execute('SELECT DISTINCT nu_ano_censo FROM tb_instituicao_year ORDER BY nu_ano_censo')
anos = [row[0] for row in cur.fetchall()]

print(f"Anos disponíveis no banco: {anos}\n")

# Para cada ano, pegar top 5
for ano in anos:
    cur.execute('''
        SELECT no_entidade, qt_mat_total 
        FROM tb_instituicao_year 
        WHERE nu_ano_censo = ? 
        ORDER BY qt_mat_total DESC 
        LIMIT 5
    ''', (ano,))
    
    rows = cur.fetchall()
    print(f"=== Ranking {ano} ===")
    print(f"Total de instituições: ", end="")
    cur.execute('SELECT COUNT(*) FROM tb_instituicao_year WHERE nu_ano_censo = ?', (ano,))
    print(cur.fetchone()[0])
    print(f"Top 5:")
    
    for i, (nome, mat) in enumerate(rows, 1):
        print(f"  {i}. {nome[:50]} - Matrículas: {mat}")
    print()

c.close()
