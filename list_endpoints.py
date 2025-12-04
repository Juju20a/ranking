#!/usr/bin/env python
"""Listar todos os endpoints registrados no Flask"""
import sys
sys.path.insert(0, 'c:\\Users\\USUÁRIO\\Downloads\\CensoEscolarData-main')

from app import app

print("Endpoints registrados:")
for rule in app.url_map.iter_rules():
    if 'usuarios' in rule.rule or 'instituicoes' in rule.rule or 'ranking' in rule.rule:
        print(f"  {rule.rule} -> {list(rule.methods)}")
