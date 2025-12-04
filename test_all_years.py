#!/usr/bin/env python
import requests
import json

years = [2022, 2023, 2024]

for ano in years:
    r = requests.get(f'http://127.0.0.1:5000/instituicoesensino/ranking/{ano}')
    if r.status_code == 200:
        data = r.json()
        print(f'\n=== Ranking {ano} ===')
        print(f'Total de registros: {len(data)}')
        print(f'Top 3:')
        for d in data[:3]:
            print(f'  Rank {d["nu_ranking"]}: {d["no_entidade"][:40]} - Mat: {d["qt_mat_total"]}')
    else:
        print(f'Erro ao carregar {ano}: {r.status_code}')
