import sqlite3
import json
#Extraer desde SQLite solo los datos recién descargados
def leer_jsons_recientes_por_ticker(sqlite_path, tabla, tickers):
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    placeholders = ",".join(["?"] * len(tickers))
    query = f"SELECT raw_json FROM {tabla} WHERE ticker IN ({placeholders})"

    cur.execute(query, tickers)
    registros = [json.loads(row[0]) for row in cur.fetchall() if row[0]]

    conn.close()
    return registros
from psycopg2.extras import execute_values

def insertar_en_postgres(conn_pg, tabla_destino, filas):
    if not filas:
        print(f"⚠️ Nada que insertar en {tabla_destino}.")
        return

    columnas = list(filas[0].keys())
    valores = [tuple(f[c] for c in columnas) for f in filas]
    columnas_str = ",".join(columnas)

    query = f"""
        INSERT INTO {tabla_destino} ({columnas_str})
        VALUES %s
        ON CONFLICT (ticker, anio) DO NOTHING;
    """

    with conn_pg.cursor() as cur:
        execute_values(cur, query, valores)

    conn_pg.commit()
    print(f"✅ Insertados {len(valores)} registros en {tabla_destino}")
