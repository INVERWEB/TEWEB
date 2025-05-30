
import sqlite3
from pathlib import Path
import json
from datetime import datetime

# Ruta fija y definitiva de la base de datos
DB_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db")

def crear_tablas_si_faltan():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickers_consultados (
            ticker TEXT PRIMARY KEY,
            nombre_empresa TEXT,
            sector TEXT,
            industria TEXT,
            market_cap REAL,
            enterprise_value REAL,
            fecha_consulta TEXT
        );
    """)

    for tabla in ["income_statement", "balance_sheet", "cash_flow", "ratios"]:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabla} (
                ticker TEXT,
                anio TEXT,
                raw_json TEXT,
                PRIMARY KEY (ticker, anio)
            );
        """)

    conn.commit()
    conn.close()

def ya_existe_ticker(ticker):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM tickers_consultados WHERE ticker = ?", (ticker,))
        return cur.fetchone() is not None
    except Exception as e:
        print(f"⚠️ Error verificando existencia del ticker {ticker}: {e}")
        return False
    finally:
        conn.close()

def insertar_json_generico(tabla, ticker, entrada):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        anio = entrada.get("date", "N/A")[:4]
        raw_json = json.dumps(entrada)

        cur.execute(f"""
            INSERT OR REPLACE INTO {tabla} (ticker, anio, raw_json)
            VALUES (?, ?, ?)
        """, (ticker, anio, raw_json))
        conn.commit()
    except Exception as e:
        print(f"❌ Error insertando {tabla} para {ticker}: {e}")
    finally:
        conn.close()

def registrar_ticker_consultado(ticker, nombre_empresa, sector, industria, market_cap=None, enterprise_value=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR REPLACE INTO tickers_consultados 
            (ticker, nombre_empresa, sector, industria, market_cap, enterprise_value, fecha_consulta)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker,
            nombre_empresa,
            sector,
            industria,
            market_cap,
            enterprise_value,
            datetime.now().strftime("%Y-%m-%d")
        ))
        conn.commit()
    except Exception as e:
        print(f"⚠️ Error registrando metadatos para {ticker}: {e}")
    finally:
        conn.close()
