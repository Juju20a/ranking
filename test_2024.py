#!/usr/bin/env python
import requests
import json

r = requests.get('http://127.0.0.1:5000/instituicoesensino/ranking/2024')
print(f'Status: {r.status_code}')

if r.status_code == 200:
    data = r.json()
    print(f'\nTop 5 instituições (2024):')
    for d in data[:5]:
        print(f'  Rank {d["nu_ranking"]}: {d["no_entidade"][:50]} - Matrículas: {d["qt_mat_total"]}')
    print(f'\nTotal de registros retornados: {len(data)}')
else:
    print(f'Erro: {r.text}')
