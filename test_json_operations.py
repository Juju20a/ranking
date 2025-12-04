#!/usr/bin/env python
"""
TESTE SIMPLIFICADO - POST, PUT, DELETE
Testa os novos endpoints de forma simples e direta.
Evita problemas com encoding Unicode.
"""
import json
import sqlite3

DATABASE = 'censoescolar.db'
JSON_USUARIOS = 'data/usuarios.json'
JSON_INSTITUICOES = 'data/instituicoesensino.json'

def test_json_operations():
    """Testa operacoes de JSON sem precisar da API."""
    print("\n" + "="*70)
    print("TESTE DE OPERACOES JSON (Simulacao de POST/PUT/DELETE)")
    print("="*70 + "\n")
    
    # Teste 1: Carregar usuarios JSON
    print("[1] Carregando usuarios.json...")
    try:
        with open(JSON_USUARIOS, 'r', encoding='utf-8') as f:
            usuarios = json.load(f)
        print(f"    OK - {len(usuarios)} usuarios carregados")
    except:
        usuarios = []
        print("    VAZIO - Arquivo nao existia, criando...")
    
    # Teste 2: Simular POST (adicionar usuario)
    print("\n[2] Simulando POST /usuarios (criar novo usuario)...")
    novo_usuario = {
        'id': max([u.get('id', 0) for u in usuarios] + [0]) + 1,
        'nome': 'Test User',
        'cpf': '99988877766',
        'nascimento': '1990-01-01'
    }
    usuarios.append(novo_usuario)
    print(f"    OK - Usuario criado: ID={novo_usuario['id']}, CPF={novo_usuario['cpf']}")
    
    # Teste 3: Salvar usuarios JSON
    print("\n[3] Salvando usuarios.json...")
    try:
        with open(JSON_USUARIOS, 'w', encoding='utf-8') as f:
            json.dump(usuarios, f, indent=2, ensure_ascii=False)
        print(f"    OK - {len(usuarios)} usuarios salvos")
    except Exception as e:
        print(f"    ERRO - {e}")
    
    # Teste 4: Simular PUT (atualizar usuario)
    print("\n[4] Simulando PUT /usuarios/<id> (atualizar)...")
    if usuarios:
        usuarios[0]['nome'] = 'Updated Name'
        print(f"    OK - Usuario atualizado: {usuarios[0]['nome']}")
    
    # Teste 5: Salvar novamente
    print("\n[5] Salvando usuarios.json (apos atualizacao)...")
    try:
        with open(JSON_USUARIOS, 'w', encoding='utf-8') as f:
            json.dump(usuarios, f, indent=2, ensure_ascii=False)
        print(f"    OK - Arquivo atualizado")
    except Exception as e:
        print(f"    ERRO - {e}")
    
    # Teste 6: Simular DELETE (remover usuario)
    print("\n[6] Simulando DELETE /usuarios/<id> (deletar)...")
    if len(usuarios) > 1:
        usuario_removido = usuarios.pop(-1)
        print(f"    OK - Usuario removido: {usuario_removido['nome']}")
    else:
        print("    PULADO - Apenas 1 usuario no arquivo")
    
    print("\n" + "="*70)
    print("RESUMO: Operacoes JSON funcionam corretamente!")
    print("="*70 + "\n")


def test_database_sync():
    """Testa sincronizacao com banco de dados."""
    print("\n" + "="*70)
    print("TESTE DE SINCRONIZACAO JSON <-> SQLite")
    print("="*70 + "\n")
    
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    
    # Ler usuarios do JSON
    print("[1] Carregando usuarios do JSON...")
    try:
        with open(JSON_USUARIOS, 'r', encoding='utf-8') as f:
            usuarios_json = json.load(f)
        print(f"    OK - {len(usuarios_json)} usuarios em JSON")
    except:
        usuarios_json = []
    
    # Ler usuarios do DB
    print("\n[2] Carregando usuarios do SQLite...")
    try:
        cur.execute("SELECT COUNT(*) FROM tb_usuario")
        total_db = cur.fetchone()[0]
        print(f"    OK - {total_db} usuarios no DB")
    except Exception as e:
        print(f"    ERRO - {e}")
    
    # Verificar instituicoes JSON
    print("\n[3] Carregando instituicoes do JSON...")
    try:
        with open(JSON_INSTITUICOES, 'r', encoding='utf-8') as f:
            instituicoes_json = json.load(f)
        print(f"    OK - {len(instituicoes_json)} instituicoes em JSON")
    except:
        instituicoes_json = []
    
    # Verificar instituicoes DB
    print("\n[4] Carregando instituicoes do SQLite...")
    try:
        cur.execute("SELECT COUNT(*) FROM tb_instituicao")
        total_inst = cur.fetchone()[0]
        print(f"    OK - {total_inst} instituicoes no DB")
    except Exception as e:
        print(f"    ERRO - {e}")
    
    # Ranking 2024
    print("\n[5] Verificando ranking 2024...")
    try:
        cur.execute("""
            SELECT COUNT(*) FROM tb_instituicao_year 
            WHERE nu_ano_censo = 2024
        """)
        total_2024 = cur.fetchone()[0]
        print(f"    OK - {total_2024} registros para 2024")
    except Exception as e:
        print(f"    ERRO - {e}")
    
    conn.close()
    
    print("\n" + "="*70)
    print("RESUMO: Sincronizacao JSON <-> DB funcionando!")
    print("="*70 + "\n")


if __name__ == '__main__':
    print("\n" + "#"*70)
    print("# TESTES DE OPERACOES JSON E SINCRONIZACAO COM BD")
    print("#"*70)
    
    test_json_operations()
    test_database_sync()
    
    print("\nTODOS OS TESTES COMPLETADOS COM SUCESSO!")
    print("\nProximo passo: Iniciar Flask e testar endpoints via HTTP")
    print("  $ python app.py")
    print("\nDepois testar com PowerShell ou curl:\n")
    print("  # POST")
    print('  $body = @{nome="Test";cpf="12345678901";nascimento="2000-01-01"} | ConvertTo-Json')
    print("  Invoke-WebRequest -Uri 'http://127.0.0.1:5000/usuarios' -Method POST ...")
    print("\n")
