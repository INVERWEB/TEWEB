
import sys
import os
import sqlite3
import json

# Ruta absoluta a Tablasplanas_New_Registro (ajustado a tu estructura real)
sys.path.append(r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/Tablasplanas_New_Registro")

from normalizadores.normalize_analyst_estimates import normalize_analyst_estimates

DB_PATH = r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db"
TABLE_ORIGEN = "analyst_estimates"
TABLE_DESTINO = "analyst_estimates_plana"

def crear_tabla_plana():
    print(f"🚧 Insertando nuevos registros desde '{TABLE_ORIGEN}' hacia '{TABLE_DESTINO}'...")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"SELECT raw_json FROM {TABLE_ORIGEN}")
    rows = cursor.fetchall()

    insertados = 0
    duplicados = 0
    fallidos = 0
    # Crear tabla si no existe
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DESTINO} (
            ticker TEXT,
            anio TEXT,
            estimatedRevenueLow REAL,
            estimatedRevenueHigh REAL,
            estimatedRevenueAvg REAL,
            estimatedEbitdaLow REAL,
            estimatedEbitdaHigh REAL,
            estimatedEbitdaAvg REAL,
            estimatedEbitLow REAL,
            estimatedEbitHigh REAL,
            estimatedEbitAvg REAL,
            estimatedNetIncomeLow REAL,
            estimatedNetIncomeHigh REAL,
            estimatedNetIncomeAvg REAL,
            estimatedSgaExpenseLow REAL,
            estimatedSgaExpenseHigh REAL,
            estimatedSgaExpenseAvg REAL,
            estimatedEpsAvg REAL,
            estimatedEpsHigh REAL,
            estimatedEpsLow REAL,
            numberAnalystEstimatedRevenue INTEGER,
            numberAnalystsEstimatedEps INTEGER
        )
    """)

    for row in rows:
        try:
            try:
                raw = json.loads(row[0])
            except json.JSONDecodeError:
                fallidos += 1
                continue

            print("🧪 RAW JSON CARGADO:", raw)

            registro = normalize_analyst_estimates(raw)

            print("📋 RESULTADO NORMALIZADO:", registro)

            if not registro:
                fallidos += 1
                continue

            cursor.execute(
                f"SELECT 1 FROM {TABLE_DESTINO} WHERE ticker = ? AND anio = ?",
                (registro["ticker"], registro["anio"])
            )
            if cursor.fetchone():
                duplicados += 1
                continue

            columnas = ", ".join(registro.keys())
            valores = tuple(registro.values())
            placeholders = ", ".join(["?"] * len(valores))

            cursor.execute(
                f"INSERT INTO {TABLE_DESTINO} ({columnas}) VALUES ({placeholders})",
                valores
            )
            insertados += 1

        except Exception:
            fallidos += 1

    conn.commit()
    conn.close()

    print(f"📊 Resultado en '{TABLE_DESTINO}':")
    print(f"✔️ Insertados: {insertados}")
    print(f"⚠️ Duplicados: {duplicados}")
    print(f"❌ Fallidos: {fallidos}")

if __name__ == "__main__":
    crear_tabla_plana()
