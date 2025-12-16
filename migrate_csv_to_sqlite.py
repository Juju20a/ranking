"""
Script: Migrate CSV microdados (Censo Escolar 2022-2024) into SQLite using pandas chunked read.

Usage:
    python migrate_csv_to_sqlite.py --csv microdados_ed_basica_2024.csv --db censoescolar.db --chunk 200000

Notes:
- By default, includes ALL Brazil data (no regional filter).
- Attempts to detect column names; if CSV uses different names adjust the `CANDIDATE_COLUMNS` mapping.
- It will insert only if the `codigo` (entity code) does not already exist for that year.
- Calculates qt_mat_total automatically during migration.
"""

import argparse
import sqlite3
import os
import pandas as pd

DEFAULT_DB = "censoescolar.db"
DEFAULT_CSV = "microdados_ed_basica_2024.csv"
DEFAULT_CHUNK = 200000

# Candidate column names that might exist in different CSV versions.
CANDIDATE_COLUMNS = {
    'codigo': ['CO_ENTIDADE', 'CO_ENTIDADE_ESCOLA', 'CO_ENTIDADE_MEC', 'COD_ENTIDADE', 'CO_ENTIDADE_ENSINO', 'CO_ENTIDADE_CURSO'],
    'nome': ['NO_ENTIDADE', 'NO_ESCOLA', 'NOME_ENTIDADE'],
    'co_uf': ['CO_UF'],
    'no_uf': ['NO_UF'],
    'sg_uf': ['SG_UF'],
    'co_municipio': ['CO_MUNICIPIO'],
    'no_municipio': ['NO_MUNICIPIO'],
    'co_mesorregiao': ['CO_MESORREGIAO'],
    'no_mesorregiao': ['NO_MESORREGIAO'],
    'co_microrregiao': ['CO_MICRORREGIAO'],
    'no_microrregiao': ['NO_MICRORREGIAO'],
    'co_regiao': ['CO_REGIAO'],
    'no_regiao': ['NO_REGIAO'],
    'nu_ano_censo': ['NU_ANO_CENSO', 'NU_ANO'],
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


def find_column(columns, candidates):
    for c in candidates:
        if c in columns:
            return c
    return None


def load_schema(db_path: str, schema_file: str = 'schema.sql'):
    conn = sqlite3.connect(db_path)
    with open(schema_file, 'r', encoding='utf-8') as f:
        conn.executescript(f.read())
    conn.close()


def migrate_csv(csv_file: str, db_path: str, chunk_size: int = DEFAULT_CHUNK,
                filter_nordeste=False, sep=';', fast=False, dry_run=False,
                encoding='latin1'):

    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    load_schema(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Criar tabela tb_instituicao_year se não existir
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tb_instituicao_year (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            co_entidade TEXT NOT NULL,
            no_entidade TEXT,
            co_uf INTEGER,
            no_uf TEXT,
            sg_uf TEXT,
            co_municipio INTEGER,
            no_municipio TEXT,
            co_mesorregiao INTEGER,
            no_mesorregiao TEXT,
            co_microrregiao INTEGER,
            no_microrregiao TEXT,
            co_regiao INTEGER,
            no_regiao TEXT,
            nu_ano_censo INTEGER NOT NULL,
            qt_mat_bas INTEGER DEFAULT 0,
            qt_mat_prof INTEGER DEFAULT 0,
            qt_mat_eja INTEGER DEFAULT 0,
            qt_mat_esp INTEGER DEFAULT 0,
            qt_mat_fund INTEGER DEFAULT 0,
            qt_mat_inf INTEGER DEFAULT 0,
            qt_mat_med INTEGER DEFAULT 0,
            qt_mat_zr_na INTEGER DEFAULT 0,
            qt_mat_zr_rur INTEGER DEFAULT 0,
            qt_mat_zr_urb INTEGER DEFAULT 0,
            qt_mat_total INTEGER DEFAULT 0,
            UNIQUE(co_entidade, nu_ano_censo)
        )
    """)
    conn.commit()

    inserted_total = 0
    skipped_total = 0
    processed_total = 0

    columns_printed = False
    chunk_idx = 0

    # Detectar ano do arquivo CSV pelo nome
    file_year = None
    for y in (2022, 2023, 2024):
        if str(y) in os.path.basename(csv_file):
            file_year = y
            break

    if fast:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = OFF;")
        conn.execute("PRAGMA temp_store = MEMORY;")

    # --- READ CSV WITH SAFE ENCODING ---
    for chunk in pd.read_csv(
            csv_file,
            sep=sep,
            chunksize=chunk_size,
            dtype=str,
            low_memory=True,
            encoding=encoding,
            encoding_errors='replace'
        ):

        processed_total += len(chunk)
        cols = chunk.columns.tolist()

        # Detectar todas as colunas necessárias
        codigo_col = find_column(cols, CANDIDATE_COLUMNS['codigo'])
        nome_col = find_column(cols, CANDIDATE_COLUMNS['nome'])
        co_uf_col = find_column(cols, CANDIDATE_COLUMNS['co_uf'])
        no_uf_col = find_column(cols, CANDIDATE_COLUMNS['no_uf'])
        sg_uf_col = find_column(cols, CANDIDATE_COLUMNS['sg_uf'])
        co_municipio_col = find_column(cols, CANDIDATE_COLUMNS['co_municipio'])
        no_municipio_col = find_column(cols, CANDIDATE_COLUMNS['no_municipio'])
        co_mesorregiao_col = find_column(cols, CANDIDATE_COLUMNS['co_mesorregiao'])
        no_mesorregiao_col = find_column(cols, CANDIDATE_COLUMNS['no_mesorregiao'])
        co_microrregiao_col = find_column(cols, CANDIDATE_COLUMNS['co_microrregiao'])
        no_microrregiao_col = find_column(cols, CANDIDATE_COLUMNS['no_microrregiao'])
        co_regiao_col = find_column(cols, CANDIDATE_COLUMNS['co_regiao'])
        no_regiao_col = find_column(cols, CANDIDATE_COLUMNS['no_regiao'])
        nu_ano_censo_col = find_column(cols, CANDIDATE_COLUMNS['nu_ano_censo'])
        qt_mat_bas_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_bas'])
        qt_mat_prof_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_prof'])
        qt_mat_eja_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_eja'])
        qt_mat_esp_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_esp'])
        qt_mat_fund_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_fund'])
        qt_mat_inf_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_inf'])
        qt_mat_med_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_med'])
        qt_mat_zr_na_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_zr_na'])
        qt_mat_zr_rur_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_zr_rur'])
        qt_mat_zr_urb_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_zr_urb'])
        qt_mat_total_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_total'])

        if not columns_printed:
            print("Detected CSV columns (first chunk):", cols)
            print("\nDetected mapping:")
            print(f"  codigo: {codigo_col}")
            print(f"  nome: {nome_col}")
            print(f"  co_uf: {co_uf_col}, no_uf: {no_uf_col}, sg_uf: {sg_uf_col}")
            print(f"  co_municipio: {co_municipio_col}, no_municipio: {no_municipio_col}")
            print(f"  co_mesorregiao: {co_mesorregiao_col}, no_mesorregiao: {no_mesorregiao_col}")
            print(f"  co_microrregiao: {co_microrregiao_col}, no_microrregiao: {no_microrregiao_col}")
            print(f"  co_regiao: {co_regiao_col}, no_regiao: {no_regiao_col}")
            print(f"  nu_ano_censo: {nu_ano_censo_col}")
            print(f"  qt_mat_bas: {qt_mat_bas_col}, qt_mat_prof: {qt_mat_prof_col}")
            print(f"  qt_mat_eja: {qt_mat_eja_col}, qt_mat_esp: {qt_mat_esp_col}")
            print(f"  qt_mat_fund: {qt_mat_fund_col}, qt_mat_inf: {qt_mat_inf_col}, qt_mat_med: {qt_mat_med_col}")
            print(f"  qt_mat_zr_*: na={qt_mat_zr_na_col}, rur={qt_mat_zr_rur_col}, urb={qt_mat_zr_urb_col}")
            print(f"  qt_mat_total: {qt_mat_total_col}")
            print(f"  file_year (from filename): {file_year}")
            columns_printed = True

        if not codigo_col or not nome_col or not co_uf_col:
            print("Cannot identify essential columns in CSV chunk; skipping chunk.")
            continue

        # Filter only Nordeste (CO_UF 21..29) - desabilitado por padrão
        if filter_nordeste:
            try:
                chunk[co_uf_col] = pd.to_numeric(chunk[co_uf_col], errors='coerce')
                chunk = chunk[chunk[co_uf_col].between(21, 29, inclusive='both')]
            except Exception:
                chunk = chunk[chunk[co_uf_col].astype(str).str.startswith(tuple(str(x) for x in range(21, 30)))]

        insert_rows = []
        insert_rows_year = []

        def safe_int(val):
            """Converte valor para inteiro de forma segura."""
            if pd.isna(val) or val == '' or val is None:
                return 0
            try:
                return int(float(str(val).strip()))
            except (ValueError, TypeError):
                return 0

        def safe_str(val):
            """Converte valor para string de forma segura."""
            if pd.isna(val) or val is None:
                return ''
            return str(val).strip()

        for idx, row in chunk.iterrows():
            codigo = row.get(codigo_col)
            if pd.isna(codigo):
                skipped_total += 1
                continue

            # Detectar ano do censo
            ano_censo = None
            if nu_ano_censo_col and not pd.isna(row.get(nu_ano_censo_col)):
                ano_censo = safe_int(row.get(nu_ano_censo_col))
            if not ano_censo or ano_censo < 2022 or ano_censo > 2024:
                ano_censo = file_year

            # Verificar duplicação na tabela tb_instituicao
            cursor.execute("SELECT id FROM tb_instituicao WHERE codigo = ?", (str(codigo),))
            exists_instituicao = cursor.fetchone() is not None

            # Verificar duplicação na tabela tb_instituicao_year
            cursor.execute("SELECT id FROM tb_instituicao_year WHERE co_entidade = ? AND nu_ano_censo = ?", (str(codigo), ano_censo))
            exists_year = cursor.fetchone() is not None

            nome = safe_str(row.get(nome_col))
            co_uf_val = safe_int(row.get(co_uf_col))
            no_uf_val = safe_str(row.get(no_uf_col)) if no_uf_col else ''
            sg_uf_val = safe_str(row.get(sg_uf_col)) if sg_uf_col else ''
            co_mun_val = safe_int(row.get(co_municipio_col))
            no_mun_val = safe_str(row.get(no_municipio_col)) if no_municipio_col else ''
            co_mesorregiao_val = safe_int(row.get(co_mesorregiao_col)) if co_mesorregiao_col else 0
            no_mesorregiao_val = safe_str(row.get(no_mesorregiao_col)) if no_mesorregiao_col else ''
            co_microrregiao_val = safe_int(row.get(co_microrregiao_col)) if co_microrregiao_col else 0
            no_microrregiao_val = safe_str(row.get(no_microrregiao_col)) if no_microrregiao_col else ''
            co_regiao_val = safe_int(row.get(co_regiao_col)) if co_regiao_col else 0
            no_regiao_val = safe_str(row.get(no_regiao_col)) if no_regiao_col else ''

            qt_mat_bas = safe_int(row.get(qt_mat_bas_col)) if qt_mat_bas_col else 0
            qt_mat_prof = safe_int(row.get(qt_mat_prof_col)) if qt_mat_prof_col else 0
            qt_mat_eja = safe_int(row.get(qt_mat_eja_col)) if qt_mat_eja_col else 0
            qt_mat_esp = safe_int(row.get(qt_mat_esp_col)) if qt_mat_esp_col else 0
            qt_mat_fund = safe_int(row.get(qt_mat_fund_col)) if qt_mat_fund_col else 0
            qt_mat_inf = safe_int(row.get(qt_mat_inf_col)) if qt_mat_inf_col else 0
            qt_mat_med = safe_int(row.get(qt_mat_med_col)) if qt_mat_med_col else 0
            qt_mat_zr_na = safe_int(row.get(qt_mat_zr_na_col)) if qt_mat_zr_na_col else 0
            qt_mat_zr_rur = safe_int(row.get(qt_mat_zr_rur_col)) if qt_mat_zr_rur_col else 0
            qt_mat_zr_urb = safe_int(row.get(qt_mat_zr_urb_col)) if qt_mat_zr_urb_col else 0

            # Calcular qt_mat_total
            if qt_mat_total_col and not pd.isna(row.get(qt_mat_total_col)):
                qt_mat_total = safe_int(row.get(qt_mat_total_col))
            else:
                # Calcular total somando os campos de matrícula
                qt_mat_total = qt_mat_bas + qt_mat_prof + qt_mat_eja + qt_mat_esp + qt_mat_fund + qt_mat_inf + qt_mat_med

            # Inserir na tabela tb_instituicao (compatibilidade)
            if not exists_instituicao:
                insert_rows.append((str(codigo), nome, co_uf_val, no_uf_val, sg_uf_val, co_mun_val, no_mun_val, qt_mat_bas, qt_mat_prof, qt_mat_esp))

            # Inserir na tabela tb_instituicao_year (ranking por ano)
            if not exists_year and ano_censo:
                insert_rows_year.append((
                    str(codigo), nome, co_uf_val, no_uf_val, sg_uf_val, 
                    co_mun_val, no_mun_val, co_mesorregiao_val, no_mesorregiao_val,
                    co_microrregiao_val, no_microrregiao_val, co_regiao_val, no_regiao_val,
                    ano_censo, qt_mat_bas, qt_mat_prof, qt_mat_eja, qt_mat_esp,
                    qt_mat_fund, qt_mat_inf, qt_mat_med, qt_mat_zr_na, qt_mat_zr_rur, qt_mat_zr_urb, qt_mat_total
                ))

        if insert_rows and not dry_run:
            before = conn.total_changes
            cursor.executemany("""
                INSERT OR IGNORE INTO tb_instituicao
                (codigo, nome, co_uf, no_uf, sg_uf, co_municipio, no_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_rows)
            conn.commit()
            after = conn.total_changes
            inserted_total += (after - before)

        if insert_rows_year and not dry_run:
            cursor.executemany("""
                INSERT OR IGNORE INTO tb_instituicao_year
                (co_entidade, no_entidade, co_uf, no_uf, sg_uf, co_municipio, no_municipio,
                 co_mesorregiao, no_mesorregiao, co_microrregiao, no_microrregiao, co_regiao, no_regiao,
                 nu_ano_censo, qt_mat_bas, qt_mat_prof, qt_mat_eja, qt_mat_esp, qt_mat_fund, qt_mat_inf, qt_mat_med,
                 qt_mat_zr_na, qt_mat_zr_rur, qt_mat_zr_urb, qt_mat_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_rows_year)
            conn.commit()

        chunk_idx += 1
        print(f"Chunk {chunk_idx}: processed={len(chunk)}, inserted_inst={len(insert_rows)}, inserted_year={len(insert_rows_year)}")

    conn.close()
    print(f"\nFinished!")
    print(f"Processed rows: {processed_total}")
    print(f"Inserted: {inserted_total}")
    print(f"Skipped: {skipped_total}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Migrate CSV microdados (Censo Escolar 2022-2024) to SQLite using pandas chunked read')

    parser.add_argument('--csv', required=True, help='Path to CSV file with microdados')
    parser.add_argument('--db', default=DEFAULT_DB, help='SQLite database path')
    parser.add_argument('--chunk', default=DEFAULT_CHUNK, type=int, help='Chunk size')
    parser.add_argument('--sep', default=';', help='CSV separator (default ;)')
    parser.add_argument('--encoding', default='latin1', help='Encoding do CSV (default latin1)')
    parser.add_argument('--fast', action='store_true', help='Enable fast SQLite PRAGMA settings')
    parser.add_argument('--dry-run', action='store_true', help='No DB insert, only preview')
    parser.add_argument('--filter-nordeste', dest='filter_nordeste', action='store_true',
                        help='Filter only CO_UF=21..29 (Nordeste). Default: include all Brazil')

    args = parser.parse_args()

    migrate_csv(
        args.csv,
        args.db,
        chunk_size=args.chunk,
        filter_nordeste=args.filter_nordeste,
        sep=args.sep,
        fast=args.fast,
        dry_run=args.dry_run,
        encoding=args.encoding
    )
