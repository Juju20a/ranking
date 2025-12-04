import argparse
import csv
import os
import sqlite3

CANDIDATE_COLUMNS = {
    'codigo': ['CO_ENTIDADE', 'CO_ENTIDADE_ESCOLA', 'CO_ENTIDADE_MEC', 'COD_ENTIDADE', 'CO_ENTIDADE_ENSINO', 'CO_ENTIDADE_CURSO'],
    'nome': ['NO_ENTIDADE', 'NO_ESCOLA', 'NOME_ENTIDADE', 'NO_ENTIDADE_ESCOLA'],
    'co_uf': ['CO_UF'],
    'co_municipio': ['CO_MUNICIPIO'],
    'qt_mat_bas': ['QT_MAT_BAS', 'NU_MATRICULAS_BASICA', 'QT_MATRICULAS_BAS'],
    'qt_mat_prof': ['QT_MAT_PROF', 'NU_MATRICULAS_PROF'],
    'qt_mat_esp': ['QT_MAT_ESP', 'NU_MATRICULAS_ESP']
}


def find_column_index(header, candidates):
    for c in candidates:
        if c in header:
            return header.index(c)
    return None


def migrate(csv_file, db_path, chunk_size=50000, filter_nordeste=True, encoding='latin1', limit=None):
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    inserted = 0
    skipped = 0
    processed = 0

    with open(csv_file, 'r', encoding=encoding, errors='replace', newline='') as f:
        # Assume separator is ; (as in original project)
        reader = csv.reader(f, delimiter=';')
        try:
            header = next(reader)
        except StopIteration:
            print('CSV vazio')
            return

        # Normalize header (strip)
        header = [h.strip() for h in header]

        codigo_i = find_column_index(header, CANDIDATE_COLUMNS['codigo'])
        nome_i = find_column_index(header, CANDIDATE_COLUMNS['nome'])
        co_uf_i = find_column_index(header, CANDIDATE_COLUMNS['co_uf'])
        co_mun_i = find_column_index(header, CANDIDATE_COLUMNS['co_municipio'])
        qt_mat_bas_i = find_column_index(header, CANDIDATE_COLUMNS['qt_mat_bas'])
        qt_mat_prof_i = find_column_index(header, CANDIDATE_COLUMNS['qt_mat_prof'])
        qt_mat_esp_i = find_column_index(header, CANDIDATE_COLUMNS['qt_mat_esp'])

        print('Detected mapping:')
        print('  codigo:', header[codigo_i] if codigo_i is not None else None)
        print('  nome:', header[nome_i] if nome_i is not None else None)
        print('  co_uf:', header[co_uf_i] if co_uf_i is not None else None)
        print('  co_municipio:', header[co_mun_i] if co_mun_i is not None else None)
        print('  qt_mat_bas:', header[qt_mat_bas_i] if qt_mat_bas_i is not None else None)
        print('  qt_mat_prof:', header[qt_mat_prof_i] if qt_mat_prof_i is not None else None)
        print('  qt_mat_esp:', header[qt_mat_esp_i] if qt_mat_esp_i is not None else None)

        if codigo_i is None or nome_i is None or co_uf_i is None:
            print('Não foi possível identificar colunas essenciais (codigo/nome/co_uf). Abortando.')
            return

        batch = []
        for row in reader:
            processed += 1
            if limit and processed > limit:
                break

            # Safe index access
            def get(i):
                if i is None or i >= len(row):
                    return ''
                return row[i].strip()

            codigo = get(codigo_i)
            nome = get(nome_i)
            co_uf = get(co_uf_i)
            co_mun = get(co_mun_i)
            qt_mat_bas = get(qt_mat_bas_i)
            qt_mat_prof = get(qt_mat_prof_i)
            qt_mat_esp = get(qt_mat_esp_i)

            if codigo == '':
                skipped += 1
                continue

            # Filter Nordeste (21..29)
            co_uf_int = None
            try:
                co_uf_int = int(co_uf)
            except Exception:
                co_uf_int = None

            if filter_nordeste and co_uf_int is not None:
                if not (21 <= co_uf_int <= 29):
                    skipped += 1
                    continue

            co_uf_val = co_uf_int if co_uf_int is not None else 0
            co_mun_val = int(co_mun) if co_mun.isdigit() else 0
            qt_mat_bas_val = int(qt_mat_bas) if qt_mat_bas.isdigit() else 0
            qt_mat_prof_val = int(qt_mat_prof) if qt_mat_prof.isdigit() else 0
            qt_mat_esp_val = int(qt_mat_esp) if qt_mat_esp.isdigit() else 0

            batch.append((str(codigo), nome, co_uf_val, co_mun_val, qt_mat_bas_val, qt_mat_prof_val, qt_mat_esp_val))

            if len(batch) >= chunk_size:
                before = conn.total_changes
                cur.executemany('''INSERT OR IGNORE INTO tb_instituicao
                    (codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''', batch)
                conn.commit()
                after = conn.total_changes
                inserted += (after - before)
                print(f'Processed {processed} rows, inserted so far: {inserted}')
                batch = []

        # Final flush
        if batch:
            before = conn.total_changes
            cur.executemany('''INSERT OR IGNORE INTO tb_instituicao
                (codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp)
                VALUES (?, ?, ?, ?, ?, ?, ?)''', batch)
            conn.commit()
            after = conn.total_changes
            inserted += (after - before)

    conn.close()
    print('\nFinished')
    print('Processed:', processed)
    print('Inserted:', inserted)
    print('Skipped:', skipped)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simple CSV -> SQLite migrator (no pandas)')
    parser.add_argument('--csv', required=True)
    parser.add_argument('--db', default='censoescolar.db')
    parser.add_argument('--chunk', type=int, default=50000)
    parser.add_argument('--no-filter', dest='filter_nordeste', action='store_false')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of rows processed (0 = all)')
    args = parser.parse_args()

    migrate(args.csv, args.db, chunk_size=args.chunk, filter_nordeste=args.filter_nordeste, limit=(args.limit or None))
