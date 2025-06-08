import sqlite3
import json

DB_PATH = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\INVERSORWEB\fmp_datafree.db"
TABLE_ORIGEN = "balance_sheet"
TABLE_DESTINO = "balance_sheet_plana"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute(f"SELECT ticker, anio, raw_json FROM {TABLE_ORIGEN}")
rows = cursor.fetchall()

fallidos = []

for ticker, anio, raw in rows:
    try:
        cursor.execute(f"SELECT 1 FROM {TABLE_DESTINO} WHERE ticker = ? AND anio = ?", (ticker, anio))
        if cursor.fetchone():
            continue
        cursor.execute(f"INSERT INTO {TABLE_DESTINO} (ticker, anio, raw_json) VALUES (?, ?, ?)", (ticker, anio, raw))
        conn.rollback()  # Revertimos para no escribir nada realmente
    except Exception as e:
        print(f"❌ Error: {ticker}-{anio} → {e}")
        fallidos.append((ticker, anio, str(e)))

conn.close()

print(f"\n🔍 Total fallidos: {len(fallidos)}")
