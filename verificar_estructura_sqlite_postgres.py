import sqlite3
import pandas as pd
from db_postgres.USERPGADMIN import get_pg_connection
from datetime import datetime

SQLITE_PATH = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"
TABLAS = [
    "income_statement_plana",
    "balance_sheet_plana",
    "cash_flow_plana",
    "ratios_plana",
    "key_metrics_simplificada",
    "analyst_estimates_plana",
    "enterprise_values_plana",
    "income_statement",
    "balance_sheet",
    "cash_flow",
    "enterprise_values",
    "ratios",
    "key_metrics",
    "analyst_estimates",
    "tickers_consultados"
]

LOG_FILE = "log_verificacion_estructura.txt"

def log(mensaje):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} {mensaje}")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} {mensaje}\n")

def obtener_columnas_sqlite(tabla, conn):
    try:
        df = pd.read_sql_query(f"SELECT * FROM {tabla} LIMIT 1", conn)
        return list(df.columns)
    except Exception as e:
        log(f"❌ Error al leer tabla SQLite '{tabla}': {e}")
        return []

def obtener_columnas_postgres(tabla, conn):
    try:
        df = pd.read_sql_query(f"SELECT * FROM {tabla} LIMIT 1", conn)
        return list(df.columns)
    except Exception as e:
        log(f"⚠️ PostgreSQL: tabla '{tabla}' no existe o no está accesible: {e}")
        return []

def comparar_columnas(tabla, cols_sqlite, cols_postgres):
    set_sqlite = set(cols_sqlite)
    set_postgres = set(cols_postgres)

    solo_sqlite = set_sqlite - set_postgres
    solo_postgres = set_postgres - set_sqlite

    if not solo_sqlite and not solo_postgres:
        log(f"✅ Estructura coincidente para '{tabla}' ({len(cols_sqlite)} columnas)")
    else:
        log(f"⚠️ Diferencias en estructura de '{tabla}':")
        if solo_sqlite:
            log(f"   - Columnas solo en SQLite: {solo_sqlite}")
        if solo_postgres:
            log(f"   - Columnas solo en PostgreSQL: {solo_postgres}")

def verificar_estructuras():
    log("🔍 Iniciando verificación de estructura entre SQLite y PostgreSQL...\n")
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    pg_conn = get_pg_connection()

    for tabla in TABLAS:
        log(f"📂 Verificando tabla: {tabla}")
        cols_sqlite = obtener_columnas_sqlite(tabla, sqlite_conn)
        cols_postgres = obtener_columnas_postgres(tabla, pg_conn)
        if cols_sqlite:
            comparar_columnas(tabla, cols_sqlite, cols_postgres)

    sqlite_conn.close()
    pg_conn.close()
    log("\n✅ Verificación de estructuras completada.\n")

if __name__ == "__main__":
    verificar_estructuras()
