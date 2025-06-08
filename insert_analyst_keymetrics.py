
import sqlite3
import json

DB_PATH = r"/fmp_datafree.db"

def insertar_json_desglosado(tabla, lista_json):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabla} (
                ticker TEXT,
                anio TEXT,
                raw_json TEXT
            )
        """)
        insertados = 0
        fallidos = 0

        for entry in lista_json:
            try:
                ticker = entry.get("symbol")
                anio = entry.get("calendarYear") or entry.get("date", "")[:4]
                if not ticker or not anio:
                    fallidos += 1
                    continue
                raw_json = json.dumps(entry)
                cursor.execute(
                    f"INSERT INTO {tabla} (ticker, anio, raw_json) VALUES (?, ?, ?)",
                    (ticker, anio, raw_json)
                )
                insertados += 1
            except Exception as e:
                print(f"❌ Error insertando entrada: {e}")
                fallidos += 1

        conn.commit()
        print(f"✅ {insertados} filas insertadas en '{tabla}'. ❌ Fallidos: {fallidos}")
