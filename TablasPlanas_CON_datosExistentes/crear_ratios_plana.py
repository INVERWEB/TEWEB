import sys
import os

# Ruta absoluta a la carpeta que contiene normalizadores/
sys.path.append(r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/Tablasplanas_New_Registro")

import sqlite3
import json
from normalizadores.normalize_ratios import normalize_ratios

DB_PATH = r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db"
TABLE_ORIGEN = "ratios"
TABLE_DESTINO = "ratios_plana"

def crear_tabla_plana():
    print(f"🚧 Insertando nuevos registros desde '{TABLE_ORIGEN}' hacia '{TABLE_DESTINO}'...")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"SELECT raw_json FROM {TABLE_ORIGEN}")
    rows = cursor.fetchall()

    insertados = 0
    duplicados = 0
    fallidos = 0

    for row in rows:
        try:
            registro = normalize_ratios(row[0])
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
