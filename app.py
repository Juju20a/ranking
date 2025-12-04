import sqlite3
import csv
import glob
import os
import json
from flask import Flask, request, jsonify
from datetime import datetime
import logging

try:
    from marshmallow import Schema, fields
    HAS_MARSHMALLOW = True
except Exception:
    HAS_MARSHMALLOW = False

from models.Usuario import Usuario

# Config
DATABASE_NAME = "censoescolar.db"
CSV_GLOB = "microdados_ed_basica_*.csv"
JSON_USUARIOS_FILE = "data/usuarios.json"
JSON_INSTITUICOES_FILE = "data/instituicoesensino.json"

app = Flask(__name__)

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

# Optional Marshmallow schema for validation
RankingItemSchema = None
if HAS_MARSHMALLOW:
    class RankingItemSchema(Schema):
        no_entidade = fields.String(required=False)
        co_entidade = fields.String(required=False)
        no_uf = fields.String(required=False)
        sg_uf = fields.String(required=False)
        co_uf = fields.Integer(required=False)
        no_municipio = fields.String(required=False)
        co_municipio = fields.Integer(required=False)
        no_mesorregiao = fields.String(required=False)
        co_mesorregiao = fields.Integer(required=False)
        no_microrregiao = fields.String(required=False)
        co_microrregiao = fields.Integer(required=False)
        nu_ano_censo = fields.Integer(required=False)
        no_regiao = fields.String(required=False)
        co_regiao = fields.Integer(required=False)
        qt_mat_bas = fields.Integer(required=False)
        qt_mat_prof = fields.Integer(required=False)
        qt_mat_eja = fields.Integer(required=False)
        qt_mat_esp = fields.Integer(required=False)
        qt_mat_fund = fields.Integer(required=False)
        qt_mat_inf = fields.Integer(required=False)
        qt_mat_med = fields.Integer(required=False)
        qt_mat_zr_na = fields.Integer(required=False)
        qt_mat_zr_rur = fields.Integer(required=False)
        qt_mat_zr_urb = fields.Integer(required=False)
        qt_mat_total = fields.Integer(required=False)
        nu_ranking = fields.Integer(required=False)
    RankingItemSchema = RankingItemSchema


def _safe_int(val):
    try:
        return int(val)
    except Exception:
        return 0


def _find_column(header, candidates):
    for c in candidates:
        if c in header:
            return header.index(c)
    return None


# ===== Funções auxiliares para manipulação de JSON =====

def _load_json(filepath):
    """Carrega dados de um arquivo JSON."""
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning('Erro ao carregar %s: %s', filepath, e)
        return []


def _save_json(filepath, data):
    """Salva dados em um arquivo JSON."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info('Arquivo %s salvo com sucesso', filepath)
        return True
    except Exception as e:
        logger.error('Erro ao salvar %s: %s', filepath, e)
        return False


def _find_by_id_json(data, id_key, id_value):
    """Encontra um item em uma lista JSON por um campo ID."""
    for item in data:
        if str(item.get(id_key)) == str(id_value):
            return item
    return None


@app.get('/')
def index():
    return jsonify({"service": "Censo Escolar API", "version": "1.0"}), 200


@app.get('/usuarios')
def get_usuarios():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, nome, cpf, nascimento FROM tb_usuario")
        rows = cursor.fetchall()
    finally:
        conn.close()

    usuarios = []
    for row in rows:
        u = Usuario(row[0], row[1], row[2], row[3])
        usuarios.append(u.to_json())
    return jsonify(usuarios), 200


@app.post('/usuarios')
def create_usuario():
    """Cria um novo usuário e persiste em JSON e banco de dados."""
    try:
        data = request.get_json()
        if not data or not all(k in data for k in ['nome', 'cpf', 'nascimento']):
            logger.warning('Dados inválidos para criar usuário: %s', data)
            return {"mensagem": "Campos obrigatórios: nome, cpf, nascimento"}, 400

        # Carregar usuários do JSON
        usuarios_json = _load_json(JSON_USUARIOS_FILE)
        
        # Verificar se CPF já existe
        if any(u.get('cpf') == data['cpf'] for u in usuarios_json):
            logger.warning('CPF duplicado na criação de usuário: %s', data['cpf'])
            return {"mensagem": "CPF já existe"}, 409

        # Gerar novo ID
        novo_id = max([u.get('id', 0) for u in usuarios_json] + [0]) + 1
        novo_usuario = {
            'id': novo_id,
            'nome': data['nome'],
            'cpf': data['cpf'],
            'nascimento': data['nascimento']
        }

        # Persistir em JSON
        usuarios_json.append(novo_usuario)
        if not _save_json(JSON_USUARIOS_FILE, usuarios_json):
            return {"mensagem": "Erro ao salvar usuário em JSON"}, 500

        # Persistir em banco de dados
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO tb_usuario (nome, cpf, nascimento) VALUES (?, ?, ?)",
                (data['nome'], data['cpf'], data['nascimento'])
            )
            conn.commit()
            logger.info('Usuário criado com sucesso: ID=%d, CPF=%s', novo_id, data['cpf'])
        except Exception as e:
            logger.error('Erro ao inserir usuário no DB: %s', e)
            return {"mensagem": "Erro ao inserir no banco de dados"}, 500
        finally:
            conn.close()

        return jsonify(novo_usuario), 201

    except Exception as e:
        logger.error('Erro ao criar usuário: %s', e)
        return {"mensagem": "Erro interno ao criar usuário"}, 500


@app.put('/usuarios/<int:usuario_id>')
def update_usuario(usuario_id):
    """Atualiza um usuário existente em JSON e banco de dados."""
    try:
        data = request.get_json()
        if not data:
            return {"mensagem": "Corpo da requisição vazio"}, 400

        # Carregar usuários do JSON
        usuarios_json = _load_json(JSON_USUARIOS_FILE)
        usuario = _find_by_id_json(usuarios_json, 'id', usuario_id)
        
        if not usuario:
            logger.warning('Usuário não encontrado para atualização: ID=%d', usuario_id)
            return {"mensagem": "Usuário não encontrado"}, 404

        # Atualizar campos
        if 'nome' in data:
            usuario['nome'] = data['nome']
        if 'cpf' in data:
            # Verificar duplicação de CPF
            if any(u.get('cpf') == data['cpf'] and u.get('id') != usuario_id for u in usuarios_json):
                return {"mensagem": "CPF já existe em outro usuário"}, 409
            usuario['cpf'] = data['cpf']
        if 'nascimento' in data:
            usuario['nascimento'] = data['nascimento']

        # Persistir em JSON
        if not _save_json(JSON_USUARIOS_FILE, usuarios_json):
            return {"mensagem": "Erro ao salvar usuário em JSON"}, 500

        # Persistir em banco de dados
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE tb_usuario SET nome = ?, cpf = ?, nascimento = ? WHERE id = ?",
                (usuario['nome'], usuario['cpf'], usuario['nascimento'], usuario_id)
            )
            conn.commit()
            logger.info('Usuário atualizado com sucesso: ID=%d', usuario_id)
        except Exception as e:
            logger.error('Erro ao atualizar usuário no DB: %s', e)
            return {"mensagem": "Erro ao atualizar no banco de dados"}, 500
        finally:
            conn.close()

        return jsonify(usuario), 200

    except Exception as e:
        logger.error('Erro ao atualizar usuário: %s', e)
        return {"mensagem": "Erro interno ao atualizar usuário"}, 500


@app.delete('/usuarios/<int:usuario_id>')
def delete_usuario(usuario_id):
    """Deleta um usuário de JSON e banco de dados."""
    try:
        # Carregar usuários do JSON
        usuarios_json = _load_json(JSON_USUARIOS_FILE)
        usuario = _find_by_id_json(usuarios_json, 'id', usuario_id)
        
        if not usuario:
            logger.warning('Usuário não encontrado para deleção: ID=%d', usuario_id)
            return {"mensagem": "Usuário não encontrado"}, 404

        # Remover do JSON
        usuarios_json = [u for u in usuarios_json if u.get('id') != usuario_id]
        if not _save_json(JSON_USUARIOS_FILE, usuarios_json):
            return {"mensagem": "Erro ao deletar usuário em JSON"}, 500

        # Deletar do banco de dados
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM tb_usuario WHERE id = ?", (usuario_id,))
            conn.commit()
            logger.info('Usuário deletado com sucesso: ID=%d', usuario_id)
        except Exception as e:
            logger.error('Erro ao deletar usuário no DB: %s', e)
            return {"mensagem": "Erro ao deletar do banco de dados"}, 500
        finally:
            conn.close()

        return {"mensagem": f"Usuário {usuario_id} deletado com sucesso"}, 200

    except Exception as e:
        logger.error('Erro ao deletar usuário: %s', e)
        return {"mensagem": "Erro interno ao deletar usuário"}, 500


@app.get('/instituicoesensino')
def list_instituicoes():
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    conn = sqlite3.connect(DATABASE_NAME)
    cur = conn.cursor()
    cur.execute("SELECT codigo, nome, no_municipio, co_municipio, sg_uf FROM tb_instituicao LIMIT ? OFFSET ?", (limit, offset))
    rows = cur.fetchall()
    conn.close()
    items = []
    for r in rows:
        items.append({
            'codigo': r[0],
            'nome': r[1],
            'no_municipio': r[2],
            'co_municipio': r[3],
            'sg_uf': r[4]
        })
    return jsonify(items), 200


@app.get('/instituicoesensino/<codigo>')
def get_instituicao(codigo):
    conn = sqlite3.connect(DATABASE_NAME)
    cur = conn.cursor()
    cur.execute("SELECT codigo, nome, no_municipio, co_municipio, sg_uf FROM tb_instituicao WHERE codigo = ?", (codigo,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"mensagem": "Instituição não encontrada"}, 404
    item = {
        'codigo': row[0],
        'nome': row[1],
        'no_municipio': row[2],
        'co_municipio': row[3],
        'sg_uf': row[4]
    }
    return jsonify(item), 200


@app.post('/instituicoesensino')
def create_instituicao():
    """Cria uma nova instituição e persiste em JSON e banco de dados."""
    try:
        data = request.get_json()
        if not data or not all(k in data for k in ['codigo', 'nome', 'co_uf', 'co_municipio']):
            logger.warning('Dados inválidos para criar instituição: %s', data)
            return {"mensagem": "Campos obrigatórios: codigo, nome, co_uf, co_municipio"}, 400

        # Carregar instituições do JSON
        instituicoes_json = _load_json(JSON_INSTITUICOES_FILE)
        
        # Verificar se código já existe
        if any(i.get('codigo') == data['codigo'] for i in instituicoes_json):
            logger.warning('Código duplicado na criação de instituição: %s', data['codigo'])
            return {"mensagem": "Código de instituição já existe"}, 409

        nova_instituicao = {
            'codigo': data['codigo'],
            'nome': data['nome'],
            'co_uf': data.get('co_uf'),
            'co_municipio': data.get('co_municipio'),
            'qt_mat_bas': data.get('qt_mat_bas', 0),
            'qt_mat_prof': data.get('qt_mat_prof', 0),
            'qt_mat_esp': data.get('qt_mat_esp', 0)
        }

        # Persistir em JSON
        instituicoes_json.append(nova_instituicao)
        if not _save_json(JSON_INSTITUICOES_FILE, instituicoes_json):
            return {"mensagem": "Erro ao salvar instituição em JSON"}, 500

        # Persistir em banco de dados
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO tb_instituicao (codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (nova_instituicao['codigo'], nova_instituicao['nome'], nova_instituicao['co_uf'], 
                 nova_instituicao['co_municipio'], nova_instituicao['qt_mat_bas'], 
                 nova_instituicao['qt_mat_prof'], nova_instituicao['qt_mat_esp'])
            )
            conn.commit()
            logger.info('Instituição criada com sucesso: Código=%s', nova_instituicao['codigo'])
        except Exception as e:
            logger.error('Erro ao inserir instituição no DB: %s', e)
            return {"mensagem": "Erro ao inserir no banco de dados"}, 500
        finally:
            conn.close()

        return jsonify(nova_instituicao), 201

    except Exception as e:
        logger.error('Erro ao criar instituição: %s', e)
        return {"mensagem": "Erro interno ao criar instituição"}, 500


@app.put('/instituicoesensino/<codigo>')
def update_instituicao(codigo):
    """Atualiza uma instituição existente em JSON e banco de dados."""
    try:
        data = request.get_json()
        if not data:
            return {"mensagem": "Corpo da requisição vazio"}, 400

        # Carregar instituições do JSON
        instituicoes_json = _load_json(JSON_INSTITUICOES_FILE)
        instituicao = _find_by_id_json(instituicoes_json, 'codigo', codigo)
        
        if not instituicao:
            logger.warning('Instituição não encontrada para atualização: Código=%s', codigo)
            return {"mensagem": "Instituição não encontrada"}, 404

        # Atualizar campos
        if 'nome' in data:
            instituicao['nome'] = data['nome']
        if 'co_uf' in data:
            instituicao['co_uf'] = data['co_uf']
        if 'co_municipio' in data:
            instituicao['co_municipio'] = data['co_municipio']
        if 'qt_mat_bas' in data:
            instituicao['qt_mat_bas'] = data['qt_mat_bas']
        if 'qt_mat_prof' in data:
            instituicao['qt_mat_prof'] = data['qt_mat_prof']
        if 'qt_mat_esp' in data:
            instituicao['qt_mat_esp'] = data['qt_mat_esp']

        # Persistir em JSON
        if not _save_json(JSON_INSTITUICOES_FILE, instituicoes_json):
            return {"mensagem": "Erro ao salvar instituição em JSON"}, 500

        # Persistir em banco de dados
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE tb_instituicao SET nome = ?, co_uf = ?, co_municipio = ?, qt_mat_bas = ?, qt_mat_prof = ?, qt_mat_esp = ? WHERE codigo = ?",
                (instituicao['nome'], instituicao['co_uf'], instituicao['co_municipio'],
                 instituicao['qt_mat_bas'], instituicao['qt_mat_prof'], instituicao['qt_mat_esp'], codigo)
            )
            conn.commit()
            logger.info('Instituição atualizada com sucesso: Código=%s', codigo)
        except Exception as e:
            logger.error('Erro ao atualizar instituição no DB: %s', e)
            return {"mensagem": "Erro ao atualizar no banco de dados"}, 500
        finally:
            conn.close()

        return jsonify(instituicao), 200

    except Exception as e:
        logger.error('Erro ao atualizar instituição: %s', e)
        return {"mensagem": "Erro interno ao atualizar instituição"}, 500


@app.delete('/instituicoesensino/<codigo>')
def delete_instituicao(codigo):
    """Deleta uma instituição de JSON e banco de dados."""
    try:
        # Carregar instituições do JSON
        instituicoes_json = _load_json(JSON_INSTITUICOES_FILE)
        instituicao = _find_by_id_json(instituicoes_json, 'codigo', codigo)
        
        if not instituicao:
            logger.warning('Instituição não encontrada para deleção: Código=%s', codigo)
            return {"mensagem": "Instituição não encontrada"}, 404

        # Remover do JSON
        instituicoes_json = [i for i in instituicoes_json if i.get('codigo') != codigo]
        if not _save_json(JSON_INSTITUICOES_FILE, instituicoes_json):
            return {"mensagem": "Erro ao deletar instituição em JSON"}, 500

        # Deletar do banco de dados
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM tb_instituicao WHERE codigo = ?", (codigo,))
            conn.commit()
            logger.info('Instituição deletada com sucesso: Código=%s', codigo)
        except Exception as e:
            logger.error('Erro ao deletar instituição no DB: %s', e)
            return {"mensagem": "Erro ao deletar do banco de dados"}, 500
        finally:
            conn.close()

        return {"mensagem": f"Instituição {codigo} deletada com sucesso"}, 200

    except Exception as e:
        logger.error('Erro ao deletar instituição: %s', e)
        return {"mensagem": "Erro interno ao deletar instituição"}, 500


@app.get('/instituicoesensino/ranking/<int:ano>')
def instituicoes_ranking(ano: int):
    """Ranking top-10 por matrículas para o ano solicitado (2022-2024).

    Prefere ler a tabela agregada `tb_instituicao_year` no SQLite. Se não houver
    dados para o ano, popula a tabela a partir dos arquivos CSV `microdados_ed_basica_*.csv`.
    """
    logger.info('Solicitado ranking para ano: %s', ano)

    if ano < 2022 or ano > 2024:
        return {"mensagem": "Ano inválido. Informe entre 2022 e 2024."}, 400

    table_name = 'tb_instituicao_year'
    conn = sqlite3.connect(DATABASE_NAME)
    cur = conn.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            co_entidade TEXT,
            no_entidade TEXT,
            no_uf TEXT,
            sg_uf TEXT,
            co_uf INTEGER,
            no_municipio TEXT,
            co_municipio INTEGER,
            no_mesorregiao TEXT,
            co_mesorregiao INTEGER,
            no_microrregiao TEXT,
            co_microrregiao INTEGER,
            nu_ano_censo INTEGER,
            no_regiao TEXT,
            co_regiao INTEGER,
            qt_mat_bas INTEGER,
            qt_mat_prof INTEGER,
            qt_mat_eja INTEGER,
            qt_mat_esp INTEGER,
            qt_mat_fund INTEGER,
            qt_mat_inf INTEGER,
            qt_mat_med INTEGER,
            qt_mat_zr_na INTEGER,
            qt_mat_zr_rur INTEGER,
            qt_mat_zr_urb INTEGER,
            qt_mat_total INTEGER,
            PRIMARY KEY (co_entidade, nu_ano_censo)
        )
    """)
    conn.commit()

    cur.execute(f"SELECT COUNT(1) FROM {table_name} WHERE nu_ano_censo = ?", (ano,))
    cnt = cur.fetchone()[0]
    if cnt == 0:
        # Populate from CSVs
        csv_files = glob.glob(CSV_GLOB)
        if not csv_files:
            logger.warning('Nenhum arquivo CSV encontrado para popular tabela %s', table_name)
            conn.close()
            return jsonify([]), 200

        CANDIDATES = {
            'co_entidade': ['CO_ENTIDADE', 'CO_ENTIDADE_ESCOLA', 'CO_ENTIDADE_MEC', 'COD_ENTIDADE'],
            'no_entidade': ['NO_ENTIDADE', 'NO_ESCOLA', 'NOME_ENTIDADE'],
            'no_uf': ['NO_UF'],
            'sg_uf': ['SG_UF'],
            'co_uf': ['CO_UF'],
            'no_municipio': ['NO_MUNICIPIO'],
            'co_municipio': ['CO_MUNICIPIO'],
            'no_mesorregiao': ['NO_MESORREGIAO'],
            'co_mesorregiao': ['CO_MESORREGIAO'],
            'no_microrregiao': ['NO_MICRORREGIAO'],
            'co_microrregiao': ['CO_MICRORREGIAO'],
            'nu_ano_censo': ['NU_ANO_CENSO', 'NU_ANO'],
            'no_regiao': ['NO_REGIAO'],
            'co_regiao': ['CO_REGIAO'],
            'qt_mat_bas': ['QT_MAT_BAS', 'NU_MATRICULAS_BASICA', 'QT_MATRICULAS_BAS'],
            'qt_mat_prof': ['QT_MAT_PROF', 'NU_MATRICULAS_PROF'],
            'qt_mat_eja': ['QT_MAT_EJA', 'NU_MATRICULAS_EJA'],
            'qt_mat_esp': ['QT_MAT_ESP', 'NU_MATRICULAS_ESP'],
            'qt_mat_fund': ['QT_MAT_FUND', 'NU_MATRICULAS_FUND'],
            'qt_mat_inf': ['QT_MAT_INF', 'NU_MATRICULAS_INF'],
            'qt_mat_med': ['QT_MAT_MED', 'NU_MATRICULAS_MED'],
            'qt_mat_zr_na': ['QT_MAT_ZR_NA'],
            'qt_mat_zr_rur': ['QT_MAT_ZR_RUR'],
            'qt_mat_zr_urb': ['QT_MAT_ZR_URB'],
            'qt_mat_total': ['QT_MAT_TOTAL', 'NU_MATRICULAS_TOTAL']
        }

        agg = {}
        for csv_file in csv_files:
            logger.info('Populando a partir do CSV: %s', csv_file)
            with open(csv_file, 'r', encoding='latin1', errors='replace', newline='') as f:
                reader = csv.reader(f, delimiter=';')
                try:
                    header = next(reader)
                except StopIteration:
                    continue
                header = [h.strip() for h in header]

                idx = {}
                for key, cands in CANDIDATES.items():
                    found = None
                    for c in cands:
                        if c in header:
                            found = header.index(c)
                            break
                    idx[key] = found

                file_year = None
                for y in (2022, 2023, 2024):
                    if str(y) in os.path.basename(csv_file):
                        file_year = y
                        break

                for row in reader:
                    year_val = None
                    if idx['nu_ano_censo'] is not None and idx['nu_ano_censo'] < len(row):
                        try:
                            year_val = int(row[idx['nu_ano_censo']].strip())
                        except Exception:
                            year_val = None
                    if year_val is None:
                        year_val = file_year

                    if year_val != ano:
                        continue

                    def _get(k):
                        i = idx.get(k)
                        if i is None or i >= len(row):
                            return ''
                        return row[i].strip()

                    co_entidade = _get('co_entidade')
                    if not co_entidade:
                        continue

                    ent = agg.get(co_entidade)
                    if ent is None:
                        ent = {
                            'co_entidade': co_entidade,
                            'no_entidade': _get('no_entidade'),
                            'no_uf': _get('no_uf'),
                            'sg_uf': _get('sg_uf'),
                            'co_uf': _safe_int(_get('co_uf')),
                            'no_municipio': _get('no_municipio'),
                            'co_municipio': _safe_int(_get('co_municipio')),
                            'no_mesorregiao': _get('no_mesorregiao'),
                            'co_mesorregiao': _safe_int(_get('co_mesorregiao')),
                            'no_microrregiao': _get('no_microrregiao'),
                            'co_microrregiao': _safe_int(_get('co_microrregiao')),
                            'nu_ano_censo': ano,
                            'no_regiao': _get('no_regiao'),
                            'co_regiao': _safe_int(_get('co_regiao')),
                            'qt_mat_bas': 0,
                            'qt_mat_prof': 0,
                            'qt_mat_eja': 0,
                            'qt_mat_esp': 0,
                            'qt_mat_fund': 0,
                            'qt_mat_inf': 0,
                            'qt_mat_med': 0,
                            'qt_mat_zr_na': 0,
                            'qt_mat_zr_rur': 0,
                            'qt_mat_zr_urb': 0,
                            'qt_mat_total': 0
                        }
                        agg[co_entidade] = ent

                    for field in ['qt_mat_bas','qt_mat_prof','qt_mat_eja','qt_mat_esp','qt_mat_fund','qt_mat_inf','qt_mat_med','qt_mat_zr_na','qt_mat_zr_rur','qt_mat_zr_urb']:
                        val = 0
                        if idx.get(field) is not None and idx[field] < len(row):
                            try:
                                val = int(row[idx[field]].strip()) if row[idx[field]].strip() != '' else 0
                            except Exception:
                                val = 0
                        ent[field] = ent.get(field, 0) + val

                    if idx.get('qt_mat_total') is not None and idx['qt_mat_total'] < len(row):
                        try:
                            total = int(row[idx['qt_mat_total']].strip())
                        except Exception:
                            total = 0
                    else:
                        total = sum(ent.get(f,0) for f in ['qt_mat_bas','qt_mat_prof','qt_mat_eja','qt_mat_esp','qt_mat_fund','qt_mat_inf','qt_mat_med'])
                    ent['qt_mat_total'] = total

        to_insert = []
        for ent in agg.values():
            to_insert.append((
                ent.get('co_entidade'), ent.get('no_entidade'), ent.get('no_uf'), ent.get('sg_uf'), ent.get('co_uf'),
                ent.get('no_municipio'), ent.get('co_municipio'), ent.get('no_mesorregiao'), ent.get('co_mesorregiao'),
                ent.get('no_microrregiao'), ent.get('co_microrregiao'), ent.get('nu_ano_censo'), ent.get('no_regiao'), ent.get('co_regiao'),
                ent.get('qt_mat_bas'), ent.get('qt_mat_prof'), ent.get('qt_mat_eja'), ent.get('qt_mat_esp'), ent.get('qt_mat_fund'), ent.get('qt_mat_inf'), ent.get('qt_mat_med'),
                ent.get('qt_mat_zr_na'), ent.get('qt_mat_zr_rur'), ent.get('qt_mat_zr_urb'), ent.get('qt_mat_total')
            ))

        insert_sql = f"""
            INSERT OR REPLACE INTO {table_name} (
                co_entidade, no_entidade, no_uf, sg_uf, co_uf, no_municipio, co_municipio,
                no_mesorregiao, co_mesorregiao, no_microrregiao, co_microrregiao, nu_ano_censo,
                no_regiao, co_regiao, qt_mat_bas, qt_mat_prof, qt_mat_eja, qt_mat_esp, qt_mat_fund,
                qt_mat_inf, qt_mat_med, qt_mat_zr_na, qt_mat_zr_rur, qt_mat_zr_urb, qt_mat_total
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        cur.executemany(insert_sql, to_insert)
        conn.commit()

    cur.execute(f"SELECT no_entidade, co_entidade, no_uf, sg_uf, co_uf, no_municipio, co_municipio, no_mesorregiao, co_mesorregiao, no_microrregiao, co_microrregiao, nu_ano_censo, no_regiao, co_regiao, qt_mat_bas, qt_mat_prof, qt_mat_eja, qt_mat_esp, qt_mat_fund, qt_mat_inf, qt_mat_med, qt_mat_zr_na, qt_mat_zr_rur, qt_mat_zr_urb, qt_mat_total FROM {table_name} WHERE nu_ano_censo = ? ORDER BY qt_mat_total DESC LIMIT 10", (ano,))
    rows = cur.fetchall()
    conn.close()

    result = []
    for i, r in enumerate(rows, start=1):
        (no_entidade, co_entidade, no_uf, sg_uf, co_uf, no_municipio, co_municipio, no_mesorregiao, co_mesorregiao, no_microrregiao, co_microrregiao, nu_ano_censo, no_regiao, co_regiao, qt_mat_bas, qt_mat_prof, qt_mat_eja, qt_mat_esp, qt_mat_fund, qt_mat_inf, qt_mat_med, qt_mat_zr_na, qt_mat_zr_rur, qt_mat_zr_urb, qt_mat_total) = r
        item = {
            'no_entidade': no_entidade,
            'co_entidade': co_entidade,
            'no_uf': no_uf,
            'sg_uf': sg_uf,
            'co_uf': co_uf,
            'no_municipio': no_municipio,
            'co_municipio': co_municipio,
            'no_mesorregiao': no_mesorregiao,
            'co_mesorregiao': co_mesorregiao,
            'no_microrregiao': no_microrregiao,
            'co_microrregiao': co_microrregiao,
            'nu_ano_censo': nu_ano_censo,
            'no_regiao': no_regiao,
            'co_regiao': co_regiao,
            'qt_mat_bas': qt_mat_bas,
            'qt_mat_prof': qt_mat_prof,
            'qt_mat_eja': qt_mat_eja,
            'qt_mat_esp': qt_mat_esp,
            'qt_mat_fund': qt_mat_fund,
            'qt_mat_inf': qt_mat_inf,
            'qt_mat_med': qt_mat_med,
            'qt_mat_zr_na': qt_mat_zr_na,
            'qt_mat_zr_rur': qt_mat_zr_rur,
            'qt_mat_zr_urb': qt_mat_zr_urb,
            'qt_mat_total': qt_mat_total,
            'nu_ranking': i
        }
        result.append(item)

    if HAS_MARSHMALLOW and RankingItemSchema is not None:
        schema = RankingItemSchema(many=True)
        try:
            schema.load(result)
        except Exception as e:
            logger.warning('Validação do schema falhou: %s', e)

    return jsonify(result), 200


if __name__ == '__main__':
    # Adicionar host='0.0.0.0' para garantir acessibilidade em alguns ambientes
    app.run(debug=False, host='0.0.0.0')
