import sqlite3
import json
from pathlib import Path

def insertar_json_generico(tabla, ticker, json_data, db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        anio = json_data.get("date", "")[:4]
        raw = json.dumps(json_data)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabla} (
                ticker TEXT,
                anio TEXT,
                raw_json TEXT
            );
        """)

        cur.execute(f"""
            SELECT 1 FROM {tabla} WHERE ticker = ? AND anio = ?
        """, (ticker, anio))
        if cur.fetchone():
            return

        cur.execute(f"""
            INSERT INTO {tabla} (ticker, anio, raw_json)
            VALUES (?, ?, ?)
        """, (ticker, anio, raw))
        conn.commit()

    except Exception as e:
        print(f"❌ Error insertando en {tabla}: {e}")
    finally:
        conn.close()

def ya_existe_ticker(ticker, db_path):
    ticker = ticker.strip().upper()  # normaliza entrada
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM tickers_consultados WHERE UPPER(ticker) = ?", (ticker,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def registrar_ticker_consultado(ticker, nombre_empresa, sector, industria, market_cap, enterprise_value, db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tickers_consultados (
                ticker TEXT PRIMARY KEY,
                nombre_empresa TEXT,
                sector TEXT,
                industria TEXT,
                market_cap TEXT,
                enterprise_value TEXT,
                fecha_consulta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            INSERT OR REPLACE INTO tickers_consultados
            (ticker, nombre_empresa, sector, industria, market_cap, enterprise_value)
            VALUES (?, ?, ?, ?, ?, ?);
        """, (ticker, nombre_empresa, sector, industria, market_cap, enterprise_value))
        conn.commit()
    finally:
        conn.close()

def crear_tablas_si_faltan(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickers_consultados (
            ticker TEXT PRIMARY KEY,
            nombre_empresa TEXT,
            sector TEXT,
            industria TEXT,
            market_cap TEXT,
            enterprise_value TEXT,
            fecha_consulta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()