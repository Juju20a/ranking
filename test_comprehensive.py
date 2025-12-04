#!/usr/bin/env python
"""
Script de teste para validar os endpoints POST, PUT, DELETE com persistência JSON.
Testa tanto usuarios quanto instituicoesensino.
"""
import urllib.request
import json
import time

BASE_URL = "http://127.0.0.1:5000"

def make_request(method, endpoint, data=None):
    """Faz requisição HTTP genérica."""
    url = BASE_URL + endpoint
    req = urllib.request.Request(url)
    req.add_header('Content-Type', 'application/json')
    req.get_method = lambda: method
    
    try:
        body = None
        if data:
            body = json.dumps(data).encode('utf-8')
        
        response = urllib.request.urlopen(req, body, timeout=5)
        resp_data = json.loads(response.read().decode())
        return response.status, resp_data
    except urllib.error.HTTPError as e:
        try:
            resp_data = json.loads(e.read().decode())
        except:
            resp_data = {"error": e.reason}
        return e.code, resp_data
    except Exception as e:
        return 500, {"error": str(e)}


def test_usuarios():
    """Testa endpoints de usuários."""
    print("\n" + "="*60)
    print("TESTE: USUÁRIOS (JSON + DB)")
    print("="*60)
    
    # POST: Criar usuário 1
    print("\n[1] POST /usuarios - Criar novo usuário")
    status, resp = make_request('POST', '/usuarios', {
        'nome': 'João Silva',
        'cpf': '12345678901',
        'nascimento': '1990-01-15'
    })
    print(f"Status: {status} | Resposta: {resp}")
    usuario_id = resp.get('id', 1)
    
    # POST: Criar usuário 2
    print("\n[2] POST /usuarios - Criar segundo usuário")
    status, resp = make_request('POST', '/usuarios', {
        'nome': 'Maria Santos',
        'cpf': '98765432101',
        'nascimento': '1995-06-20'
    })
    print(f"Status: {status} | Resposta: {resp}")
    
    # GET: Listar todos
    print("\n[3] GET /usuarios - Listar todos")
    status, resp = make_request('GET', '/usuarios')
    print(f"Status: {status} | Total: {len(resp) if isinstance(resp, list) else 'erro'}")
    if isinstance(resp, list) and len(resp) > 0:
        print(f"  Primeiro: {resp[0].get('nome')} ({resp[0].get('cpf')})")
    
    # PUT: Atualizar usuário
    print(f"\n[4] PUT /usuarios/{usuario_id} - Atualizar usuário")
    status, resp = make_request('PUT', f'/usuarios/{usuario_id}', {
        'nome': 'João Silva Junior',
        'nascimento': '1990-01-16'
    })
    print(f"Status: {status} | Resposta: {resp}")
    
    # DELETE: Deletar usuário
    print(f"\n[5] DELETE /usuarios/{usuario_id} - Deletar usuário")
    status, resp = make_request('DELETE', f'/usuarios/{usuario_id}')
    print(f"Status: {status} | Resposta: {resp}")
    
    # Verificar após deleção
    print(f"\n[6] GET /usuarios - Verificar após deleção")
    status, resp = make_request('GET', '/usuarios')
    print(f"Status: {status} | Total: {len(resp) if isinstance(resp, list) else 'erro'}")


def test_instituicoes():
    """Testa endpoints de instituições."""
    print("\n" + "="*60)
    print("TESTE: INSTITUIÇÕES (JSON + DB)")
    print("="*60)
    
    # POST: Criar instituição 1
    print("\n[1] POST /instituicoesensino - Criar nova instituição")
    status, resp = make_request('POST', '/instituicoesensino', {
        'codigo': '99999999',
        'nome': 'ESCOLA TESTE 1',
        'co_uf': 28,
        'co_municipio': 2801505,
        'qt_mat_bas': 100,
        'qt_mat_prof': 50,
        'qt_mat_esp': 10
    })
    print(f"Status: {status} | Resposta: {resp}")
    codigo_inst = resp.get('codigo', '99999999')
    
    # POST: Criar instituição 2
    print("\n[2] POST /instituicoesensino - Criar segunda instituição")
    status, resp = make_request('POST', '/instituicoesensino', {
        'codigo': '88888888',
        'nome': 'ESCOLA TESTE 2',
        'co_uf': 27,
        'co_municipio': 2704302,
        'qt_mat_bas': 200
    })
    print(f"Status: {status} | Resposta: {resp}")
    
    # GET: Listar com paginação
    print("\n[3] GET /instituicoesensino - Listar (limit=5)")
    status, resp = make_request('GET', '/instituicoesensino?limit=5&offset=0')
    print(f"Status: {status} | Total retornado: {len(resp) if isinstance(resp, list) else 'erro'}")
    
    # GET: Buscar uma específica
    print(f"\n[4] GET /instituicoesensino/{codigo_inst} - Buscar específica")
    status, resp = make_request('GET', f'/instituicoesensino/{codigo_inst}')
    print(f"Status: {status} | Nome: {resp.get('nome')}")
    
    # PUT: Atualizar instituição
    print(f"\n[5] PUT /instituicoesensino/{codigo_inst} - Atualizar")
    status, resp = make_request('PUT', f'/instituicoesensino/{codigo_inst}', {
        'nome': 'ESCOLA TESTE 1 - ATUALIZADO',
        'qt_mat_bas': 150
    })
    print(f"Status: {status} | Resposta: {resp}")
    
    # DELETE: Deletar instituição
    print(f"\n[6] DELETE /instituicoesensino/{codigo_inst} - Deletar")
    status, resp = make_request('DELETE', f'/instituicoesensino/{codigo_inst}')
    print(f"Status: {status} | Resposta: {resp}")
    
    # Verificar após deleção
    print(f"\n[7] GET /instituicoesensino/{codigo_inst} - Verificar após deleção")
    status, resp = make_request('GET', f'/instituicoesensino/{codigo_inst}')
    print(f"Status: {status} | Esperado 404: {status == 404}")


def test_ranking():
    """Testa endpoint ranking com os 3 anos."""
    print("\n" + "="*60)
    print("TESTE: RANKING ENDPOINT")
    print("="*60)
    
    for ano in [2022, 2023, 2024]:
        print(f"\n[{ano}] GET /instituicoesensino/ranking/{ano}")
        status, resp = make_request('GET', f'/instituicoesensino/ranking/{ano}')
        if status == 200 and isinstance(resp, list):
            print(f"Status: {status} | Total: {len(resp)}")
            if len(resp) > 0:
                top1 = resp[0]
                print(f"  Top 1: {top1.get('no_entidade')} - {top1.get('qt_mat_total')} matrículas - Rank: {top1.get('nu_ranking')}")
        else:
            print(f"Status: {status} | Erro: {resp}")


if __name__ == '__main__':
    print("\n" + "="*60)
    print("TESTES COMPLETOS: GET, POST, PUT, DELETE + RANKING")
    print("="*60)
    
    test_usuarios()
    time.sleep(1)
    test_instituicoes()
    time.sleep(1)
    test_ranking()
    
    print("\n" + "="*60)
    print("TESTES CONCLUÍDOS")
    print("="*60)
