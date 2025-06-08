
import json
import time
import sqlite3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from insert_analyst_keymetrics import insertar_json_desglosado

import sys
sys.path.append(r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB")

from utils.fetch_analyst_keymetrics.download_analyst import fetch_analyst_estimates
from utils.fetch_analyst_keymetrics.download_keymetrics import fetch_key_metrics

SECTOR = "Technology"
LOTE_TRABAJO = 1044
TICKERS_PATH = Path(r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/tickers_validos_analyst_key.txt")
OFFSET_PATH = Path(r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/control/descarga_offset_analyst_key.json")
DB_PATH = r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db"
LOG_PATH = Path(f"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/logs/log_analyst_key_{SECTOR}_{datetime.today().strftime('%Y%m%d')}.txt")

load_dotenv(r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/.env")
API_KEY = os.getenv("FMP_API_KEY")
def insertar_json_desglosado(nombre_tabla, json_list):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ✅ Crear la tabla si no existe
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {nombre_tabla} (
            ticker TEXT,
            anio TEXT,
            raw_json TEXT
        )
    """)

    registros_insertados = 0
    registros_omitidos = 0

    for fila in json_list:
        ticker = fila.get("symbol") or fila.get("ticker")
        anio = fila.get("calendarYear") or fila.get("date", "")[:4]
        raw_json = json.dumps(fila)

        if not ticker or not anio:
            registros_omitidos += 1
            continue

        cur.execute(f"""
            SELECT 1 FROM {nombre_tabla}
            WHERE ticker = ? AND anio = ?
        """, (ticker, anio))

        if cur.fetchone():
            registros_omitidos += 1
            continue

        cur.execute(f"""
            INSERT INTO {nombre_tabla} (ticker, anio, raw_json)
            VALUES (?, ?, ?)
        """, (ticker, anio, raw_json))
        registros_insertados += 1

    conn.commit()
    conn.close()

def cargar_tickers():
    if not TICKERS_PATH.exists():
        raise FileNotFoundError(f"❌ No se encontró el archivo de tickers en: {TICKERS_PATH}")
    with open(TICKERS_PATH, "r") as f:
        return [t.strip() for t in f.readlines() if t.strip()]

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

def log_resultado(mensaje):
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(mensaje + "\n")
    print(mensaje)

def ejecutar_descarga():
    tickers = cargar_tickers()
    tickers = [t for t in tickers if t and t[0].isalpha()]

    offset_data = cargar_offset()
    offset = offset_data.get(SECTOR, 0)

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
        log_resultado(f"[{offset + i}/{len(tickers)}] ⏳ Procesando: {ticker}")

        try:
            analyst_data = fetch_analyst_estimates(ticker, API_KEY)
            keymetrics_data = fetch_key_metrics(ticker, API_KEY)

            if not analyst_data or not analyst_data['json']:
                resumen["json_vacio"] += 1
                log_resultado(f"⚠️ Analyst JSON vacío o inválido: {ticker}")
                continue
            if not keymetrics_data or not keymetrics_data['json']:
                resumen["json_vacio"] += 1
                log_resultado(f"⚠️ Key Metrics JSON vacío o inválido: {ticker}")
                continue

            insertar_json_desglosado("analyst_estimates", analyst_data["json"])
            insertar_json_desglosado("key_metrics", keymetrics_data["json"])

            resumen["exitosos"] += 1
        except Exception as e:
            resumen["errores"] += 1
            log_resultado(f"❌ Error en {ticker}: {e}")

        time.sleep(0.25)

    offset_data[SECTOR] = offset + total
    guardar_offset(offset_data)

    log_resultado("\n📊 RESUMEN FINAL")
    for k, v in resumen.items():
        log_resultado(f"{k.upper()}: {v}")

if __name__ == "__main__":
    print(f"🟢 Iniciando descarga para sector: {SECTOR} | Lote: {LOTE_TRABAJO}")
    ejecutar_descarga()
