import sys
import os
import sqlite3
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from normalizadores.normalize_income_statement import normalize_income_statement

DB_PATH = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db"
TABLE_NAME = "income_statement_plana"

def insertar_registros(json_list):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    insertados = 0
    duplicados = 0
    fallidos = 0

    for json_raw in json_list:
        registro = normalize_income_statement(json_raw)
        if not registro:
            fallidos += 1
            continue

        try:
            cursor.execute(f"SELECT 1 FROM {TABLE_NAME} WHERE ticker = ? AND anio = ?", (registro["ticker"], registro["anio"]))
            if cursor.fetchone():
                duplicados += 1
                continue

            columnas = ", ".join(registro.keys())
            valores = tuple(registro.values())
            placeholders = ", ".join(["?"] * len(valores))

            cursor.execute(f"INSERT INTO {TABLE_NAME} ({columnas}) VALUES ({placeholders})", valores)
            insertados += 1

        except Exception:
            fallidos += 1

    conn.commit()
    conn.close()

    print(f"📊 Resultado en '{TABLE_NAME}':")
    print(f"✔️ Insertados: {insertados}")
    print(f"⚠️ Duplicados: {duplicados}")
    print(f"❌ Fallidos: {fallidos}")


if __name__ == "__main__":
    json_list = [
        '{"symbol": "AAPL", "calendarYear": "2024", "revenue": 100000000, "netIncome": 20000000, "eps": 5.0}',
        '{"symbol": "GOOG", "calendarYear": "2023", "revenue": 120000000, "netIncome": 30000000, "eps": 6.1}',
    ]
    print("🧪 Insertando registros de prueba en income_statement...")
    insertar_registros(json_list)
