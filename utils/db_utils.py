import sqlite3
import json
from datetime import datetime

DB_PATH = "fmp_datafree.db"

def conectar_db():
    return sqlite3.connect(DB_PATH)

def crear_tablas():
    conn = conectar_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tickers_consultados (
        ticker TEXT PRIMARY KEY,
        nombre_empresa TEXT,
        sector TEXT,
        industria TEXT,
        fecha_consulta TEXT
    )
    """)

    for tabla in ["income_statement", "balance_sheet", "cash_flow", "ratios_raw"]:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {tabla} (
            ticker TEXT,
            anio TEXT,
            raw_json TEXT,
            PRIMARY KEY (ticker, anio)
        )
        """)

    conn.commit()
    conn.close()

def registrar_ticker(ticker, nombre, sector, industria):
    conn = conectar_db()
    cur = conn.cursor()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
        INSERT OR REPLACE INTO tickers_consultados (ticker, nombre_empresa, sector, industria, fecha_consulta)
        VALUES (?, ?, ?, ?, ?)
    """, (ticker, nombre, sector, industria, fecha))

    conn.commit()
    conn.close()

def insertar_json_generico(tabla, ticker, json_data):
    conn = conectar_db()
    cur = conn.cursor()

    anio = json_data.get("date", "")[:4]
    raw_json = json.dumps(json_data)

    cur.execute(f"""
        INSERT OR REPLACE INTO {tabla} (ticker, anio, raw_json)
        VALUES (?, ?, ?)
    """, (ticker, anio, raw_json))

    conn.commit()
    conn.close()

def crear_tablas():
    conn = conectar_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tickers_consultados (
        ticker TEXT PRIMARY KEY,
        nombre_empresa TEXT,
        sector TEXT,
        industria TEXT,
        fecha_consulta TEXT
    )
    """)
