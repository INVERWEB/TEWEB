import json
import time
import sqlite3
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import sys
import os

# === CARGA DE RUTAS DEL PROYECTO ===
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB")

from utils.fetch_analyst_keymetrics.download_analyst import fetch_analyst_estimates
from utils.fetch_analyst_keymetrics.download_keymetrics import fetch_key_metrics
from db_utils.registrar_tickers_recientes import registrar_ticker_exitoso

# === CONFIGURACIÓN GENERAL ===
EXCHANGES = ["NASDAQ"]
LOTE_TRABAJO = 2000

# === RUTAS ===
OFFSET_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/control/descarga_offset_stocklist.json")
DB_PATH = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"
LOG_DIR = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/logs/")

# === API KEY ===
load_dotenv("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/.env")
API_KEY = os.getenv("FMP_API_KEY")

# === FUNCIONES ===

def insertar_json_desglosado(nombre_tabla, json_list):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {nombre_tabla} (
            ticker TEXT,
            anio TEXT,
            raw_json TEXT
        )
    """)
    for fila in json_list:
        ticker = fila.get("symbol") or fila.get("ticker")
        anio = fila.get("calendarYear") or fila.get("date", "")[:4]
        raw_json = json.dumps(fila)
        if not ticker or not anio:
            continue
        cur.execute(f"SELECT 1 FROM {nombre_tabla} WHERE ticker = ? AND anio = ?", (ticker, anio))
        if cur.fetchone():
            continue
        cur.execute(f"INSERT INTO {nombre_tabla} (ticker, anio, raw_json) VALUES (?, ?, ?)", (ticker, anio, raw_json))
    conn.commit()
    conn.close()

def cargar_tickers(exchange):
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}"
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        tickers = [
            d["symbol"] for d in data
            if d.get("exchangeShortName") == exchange
            and d.get("symbol")
            and not d.get("isEtf", False)
            and not d.get("isFund", False)
            and d["symbol"][0].isalpha()
        ]
        print(f"✅ {len(tickers)} tickers encontrados en {exchange}")
        return sorted(set(tickers))
    except Exception as e:
        print(f"❌ Error al obtener tickers de {exchange}: {e}")
        return []

def cargar_offset():
    if OFFSET_PATH.exists():
        try:
            contenido = OFFSET_PATH.read_text(encoding="utf-8").strip()
            if contenido:
                return json.loads(contenido)
        except Exception as e:
            print(f"⚠️ Error al leer JSON de offset: {e}")
    return {}

def guardar_offset(offset_data):
    OFFSET_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OFFSET_PATH, "w") as f:
        json.dump(offset_data, f, indent=4)

def log_resultado(exchange, mensaje):
    log_file = LOG_DIR / f"log_analyst_key_{exchange}_{datetime.today().strftime('%Y%m%d')}.txt"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(mensaje + "\n")
    print(mensaje)

def ejecutar_descarga_por_exchange(exchange):
    tickers = cargar_tickers(exchange)
    offset_data = cargar_offset()
    offset = offset_data.get(exchange, 0)
    lote = tickers[offset:offset + LOTE_TRABAJO]
    total = len(lote)

    resumen = {
        "procesados": 0,
        "exitosos": 0,
        "duplicados": 0,
        "json_vacio": 0,
        "errores": 0
    }

    for i, ticker in enumerate(lote, 1):
        resumen["procesados"] += 1
        log_resultado(exchange, f"[{offset + i}/{len(tickers)}] ⏳ Procesando: {ticker}")
        try:
            analyst_data = fetch_analyst_estimates(ticker, API_KEY)
            keymetrics_data = fetch_key_metrics(ticker, API_KEY)

            if not analyst_data or not analyst_data['json']:
                resumen["json_vacio"] += 1
                log_resultado(exchange, f"⚠️ Analyst JSON vacío: {ticker}")
                continue
            if not keymetrics_data or not keymetrics_data['json']:
                resumen["json_vacio"] += 1
                log_resultado(exchange, f"⚠️ Key Metrics JSON vacío: {ticker}")
                continue

            insertar_json_desglosado("analyst_estimates", analyst_data["json"])
            registrar_ticker_exitoso(ticker, "analyst_estimates")

            insertar_json_desglosado("key_metrics", keymetrics_data["json"])
            registrar_ticker_exitoso(ticker, "key_metrics")

            resumen["exitosos"] += 1
        except Exception as e:
            resumen["errores"] += 1
            log_resultado(exchange, f"❌ Error en {ticker}: {e}")
        time.sleep(0.25)

    offset_data[exchange] = offset + total
    guardar_offset(offset_data)

    log_resultado(exchange, f"📊 RESUMEN FINAL {exchange}")
    for k, v in resumen.items():
        log_resultado(exchange, f"{k.upper()}: {v}")

# === MAIN ===
if __name__ == "__main__":
    for exchange in EXCHANGES:
        print(f"\n🔁 Descargando tickers desde {exchange}...")
        ejecutar_descarga_por_exchange(exchange)
