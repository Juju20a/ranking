#!/usr/bin/env python
"""Teste rápido de POST"""
import urllib.request
import json

url = "http://127.0.0.1:5000/usuarios"
data = {
    'nome': 'Teste User',
    'cpf': '11122233344',
    'nascimento': '2000-01-01'
}

req = urllib.request.Request(url, method='POST')
req.add_header('Content-Type', 'application/json')

try:
    response = urllib.request.urlopen(req, json.dumps(data).encode('utf-8'), timeout=5)
    print(f"✓ POST /usuarios - Status: {response.status}")
    resp_data = json.loads(response.read().decode())
    print(f"Resposta: {resp_data}")
except urllib.error.HTTPError as e:
    print(f"✗ POST /usuarios - Status: {e.code}")
    print(f"Erro: {e.reason}")
except Exception as e:
    print(f"Erro: {e}")
