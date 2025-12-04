#!/usr/bin/env python
import urllib.request
import json

years = [2022, 2023, 2024]

for ano in years:
    url = f'http://127.0.0.1:5000/instituicoesensino/ranking/{ano}'
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            print(f'\n=== Ranking {ano} (via API) ===')
            print(f'Status: {response.status}')
            print(f'Total de registros retornados: {len(data)}')
            if len(data) > 0:
                print(f'\nTop 3:')
                for item in data[:3]:
                    print(f'  Rank {item["nu_ranking"]}: {item["no_entidade"][:45]} - Mat: {item["qt_mat_total"]}')
    except Exception as e:
        print(f'Erro ao acessar {ano}: {e}')
