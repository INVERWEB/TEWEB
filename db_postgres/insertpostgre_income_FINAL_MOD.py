import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import json
from pathlib import Path
from normalizadores.normalize_income_statement import normalize_income_statement
from db_postgres.post_insert_utils import insertar_en_postgres

# === CONFIGURACIÓN GENERAL ===
SQLITE_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db")
TABLA_SQLITE = "income_statement"
TABLA_PG = "income_statement_plana"
LIMITE_TICKERS = 100  # Puedes ajustar este valor según el lote que quieras procesar

# === CARGAR CREDENCIALES DESDE .env ===
load_dotenv("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/.env")
PG_CONN_INFO = {
    "host": os.getenv("PGHOST"),
    "port": os.getenv("PGPORT"),
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD")
}

def obtener_tickers_con_datos(db_path, tabla, limite):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f'''
        SELECT DISTINCT ticker
        FROM {tabla}
        WHERE raw_json IS NOT NULL
        ORDER BY ROWID DESC
        LIMIT {limite}
    ''')
    resultados = [row[0] for row in cur.fetchall()]
    conn.close()
    return resultados

def leer_jsons_por_tickers(db_path, tabla, tickers):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    registros = []

    for ticker in tickers:
        cur.execute(f"SELECT raw_json FROM {tabla} WHERE ticker = ?", (ticker,))
        rows = cur.fetchall()
        for row in rows:
            if row[0]:
                registros.append((ticker, row[0]))

    conn.close()
    return registros

def main():
    print(f"🔄 Cargando los últimos {LIMITE_TICKERS} tickers con datos desde '{TABLA_SQLITE}'")
    tickers = obtener_tickers_con_datos(SQLITE_PATH, TABLA_SQLITE, LIMITE_TICKERS)
    if not tickers:
        print("⚠️ No se encontraron tickers con datos.")
        return

    registros_crudos = leer_jsons_por_tickers(SQLITE_PATH, TABLA_SQLITE, tickers)

    normalizados = []
    for _, raw in registros_crudos:
        try:
            datos = json.loads(raw)
            if not isinstance(datos, dict):
                continue
            filas = normalize_income_statement(datos)
            normalizados.extend(filas)
        except Exception as e:
            print(f"❌ Error normalizando registro: {e}")

    if not normalizados:
        print("⚠️ No hay registros normalizados para insertar.")
        return

    print(f"📥 Insertando {len(normalizados)} registros en PostgreSQL...")
    try:
        conn_pg = psycopg2.connect(**PG_CONN_INFO)
        insertar_en_postgres(conn_pg, TABLA_PG, normalizados)
        conn_pg.close()
        print("✅ Inserción completada con éxito.")
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")

if __name__ == "__main__":
    main()