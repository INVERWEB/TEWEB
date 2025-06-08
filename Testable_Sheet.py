import sqlite3
import json
from tqdm import tqdm

DB_PATH = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\INVERSORWEB\fmp_datafree.db"
TABLE_ORIGEN = "balance_sheet"
TABLE_DESTINO = "balance_sheet_plana"

def crear_tabla_plana_balance():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_DESTINO} (ticker TEXT, anio TEXT, raw_json TEXT)")
    conn.commit()

    cursor.execute(f"SELECT ticker, anio, raw_json FROM {TABLE_ORIGEN}")
    rows = cursor.fetchall()

    insertados = 0
    duplicados = 0
    fallidos = 0

    for ticker, anio, raw in tqdm(rows, desc="📥 Procesando"):
        try:
            cursor.execute(f"SELECT 1 FROM {TABLE_DESTINO} WHERE ticker = ? AND anio = ?", (ticker, anio))
            if cursor.fetchone():
                duplicados += 1
                continue
            cursor.execute(f"INSERT INTO {TABLE_DESTINO} (ticker, anio, raw_json) VALUES (?, ?, ?)", (ticker, anio, raw))
            insertados += 1
        except Exception:
            fallidos += 1

    conn.commit()
    conn.close()

    print(f"✔️ Insertados: {insertados}")
    print(f"⚠️ Duplicados: {duplicados}")
    print(f"❌ Fallidos: {fallidos}")
    print(f"📊 Total origen: {len(rows)}")

crear_tabla_plana_balance()
