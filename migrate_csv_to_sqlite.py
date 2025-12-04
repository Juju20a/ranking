"""
Script: Migrate CSV microdados (Censo Escolar 2024) into SQLite using pandas chunked read.

Usage:
    python migrate_csv_to_sqlite.py --csv microdados_ed_basica_2024.csv --db censoescolar.db --chunk 200000

Notes:
- Filters rows by CO_UF codes 21..29 (Nordeste states) by default.
- Attempts to detect column names; if CSV uses different names adjust the `CANDIDATE_COLUMNS` mapping.
- It will insert only if the `codigo` (entity code) does not already exist.
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
    'co_municipio': ['CO_MUNICIPIO'],
    'qt_mat_bas': ['QT_MAT_BAS', 'NU_MATRICULAS_BASICA', 'QT_MATRICULAS_BAS'],
    'qt_mat_prof': ['QT_MAT_PROF', 'NU_MATRICULAS_PROF'],
    'qt_mat_esp': ['QT_MAT_ESP', 'NU_MATRICULAS_ESP']
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
                filter_nordeste=True, sep=';', fast=False, dry_run=False,
                encoding='latin1'):

    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    load_schema(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted_total = 0
    skipped_total = 0
    processed_total = 0

    columns_printed = False
    chunk_idx = 0

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

        codigo_col = find_column(cols, CANDIDATE_COLUMNS['codigo'])
        nome_col = find_column(cols, CANDIDATE_COLUMNS['nome'])
        co_uf_col = find_column(cols, CANDIDATE_COLUMNS['co_uf'])
        co_municipio_col = find_column(cols, CANDIDATE_COLUMNS['co_municipio'])
        qt_mat_bas_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_bas'])
        qt_mat_prof_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_prof'])
        qt_mat_esp_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_esp'])

        if not columns_printed:
            print("Detected CSV columns (first chunk):", cols)
            print("\nDetected mapping:")
            print(f"  codigo: {codigo_col}")
            print(f"  nome: {nome_col}")
            print(f"  co_uf: {co_uf_col}")
            print(f"  co_municipio: {co_municipio_col}")
            print(f"  qt_mat_bas: {qt_mat_bas_col}")
            print(f"  qt_mat_prof: {qt_mat_prof_col}")
            print(f"  qt_mat_esp: {qt_mat_esp_col}")
            columns_printed = True

        if not codigo_col or not nome_col or not co_uf_col:
            print("Cannot identify essential columns in CSV chunk; skipping chunk.")
            continue

        # Filter only Nordeste (CO_UF 21..29)
        if filter_nordeste:
            try:
                chunk[co_uf_col] = pd.to_numeric(chunk[co_uf_col], errors='coerce')
                chunk = chunk[chunk[co_uf_col].between(21, 29, inclusive='both')]
            except Exception:
                chunk = chunk[chunk[co_uf_col].astype(str).str.startswith(tuple(str(x) for x in range(21, 30)))]

        insert_rows = []

        for idx, row in chunk.iterrows():
            codigo = row.get(codigo_col)
            if pd.isna(codigo):
                skipped_total += 1
                continue

            cursor.execute("SELECT id FROM tb_instituicao WHERE codigo = ?", (str(codigo),))
            if cursor.fetchone() is not None:
                skipped_total += 1
                continue

            nome = row.get(nome_col) or ""
            co_uf_val = int(row.get(co_uf_col)) if row.get(co_uf_col, "").isdigit() else 0
            co_mun_val = int(row.get(co_municipio_col)) if row.get(co_municipio_col, "").isdigit() else 0
            qt_mat_bas = int(row.get(qt_mat_bas_col)) if row.get(qt_mat_bas_col, "").isdigit() else 0
            qt_mat_prof = int(row.get(qt_mat_prof_col)) if row.get(qt_mat_prof_col, "").isdigit() else 0
            qt_mat_esp = int(row.get(qt_mat_esp_col)) if row.get(qt_mat_esp_col, "").isdigit() else 0

            insert_rows.append((str(codigo), nome, co_uf_val, co_mun_val, qt_mat_bas, qt_mat_prof, qt_mat_esp))

        if insert_rows and not dry_run:
            before = conn.total_changes
            cursor.executemany("""
                INSERT OR IGNORE INTO tb_instituicao
                (codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, insert_rows)
            conn.commit()
            after = conn.total_changes
            inserted_total += (after - before)

        chunk_idx += 1
        print(f"Chunk {chunk_idx}: processed={len(chunk)}, inserted={len(insert_rows)}")

    conn.close()
    print(f"\nFinished!")
    print(f"Processed rows: {processed_total}")
    print(f"Inserted: {inserted_total}")
    print(f"Skipped: {skipped_total}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Migrate CSV microdados (Censo Escolar) to SQLite using pandas chunked read')

    parser.add_argument('--csv', required=True, help='Path to CSV file with microdados')
    parser.add_argument('--db', default=DEFAULT_DB, help='SQLite database path')
    parser.add_argument('--chunk', default=DEFAULT_CHUNK, type=int, help='Chunk size')
    parser.add_argument('--sep', default=';', help='CSV separator (default ;)')
    parser.add_argument('--encoding', default='latin1', help='Encoding do CSV (default latin1)')
    parser.add_argument('--fast', action='store_true', help='Enable fast SQLite PRAGMA settings')
    parser.add_argument('--dry-run', action='store_true', help='No DB insert, only preview')
    parser.add_argument('--no-filter', dest='filter_nordeste', action='store_false',
                        help='Do NOT filter CO_UF=21..29 (Nordeste)')

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
