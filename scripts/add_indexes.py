#!/usr/bin/env python
"""
Script para adicionar índices ao banco de dados SQLite para melhorar performance.
Otimiza queries do endpoint ranking e paginação.
"""
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DATABASE = 'censoescolar.db'

def add_indexes():
    """Adiciona índices ao banco de dados."""
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    
    indexes = [
        # Índices para tabela tb_instituicao
        ("idx_tb_instituicao_codigo", "tb_instituicao", "codigo"),
        ("idx_tb_instituicao_co_uf", "tb_instituicao", "co_uf"),
        ("idx_tb_instituicao_co_municipio", "tb_instituicao", "co_municipio"),
        
        # Índices para tabela tb_usuario
        ("idx_tb_usuario_cpf", "tb_usuario", "cpf"),
        
        # Índices para tabela tb_instituicao_year (ranking)
        ("idx_tb_inst_year_ano", "tb_instituicao_year", "nu_ano_censo"),
        ("idx_tb_inst_year_ano_matriculas", "tb_instituicao_year", "nu_ano_censo, qt_mat_total DESC"),
        ("idx_tb_inst_year_co_entidade", "tb_instituicao_year", "co_entidade"),
    ]
    
    for idx_name, table, columns in indexes:
        try:
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})"
            cur.execute(sql)
            logger.info(f"Índice criado: {idx_name} em {table}({columns})")
        except Exception as e:
            logger.warning(f"Erro ao criar índice {idx_name}: {e}")
    
    # Analisar tabelas para otimizar queries
    try:
        cur.execute("ANALYZE")
        logger.info("Análise de tabelas concluída (ANALYZE)")
    except Exception as e:
        logger.warning(f"Erro ao executar ANALYZE: {e}")
    
    conn.commit()
    conn.close()
    logger.info("Índices adicionados com sucesso!")

if __name__ == '__main__':
    print("\n" + "="*60)
    print("CRIAÇÃO DE ÍNDICES - BANCO DE DADOS CENSO ESCOLAR")
    print("="*60 + "\n")
    add_indexes()
    print("\n✓ Processo concluído!\n")
