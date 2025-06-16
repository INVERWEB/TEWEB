import os
import sys
import pandas as pd

# Asegurar ruta base del proyecto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_postgres.post_insert_utils import leer_jsons_recientes_por_ticker
from normalizadores.normalize_key_metrics import normalize_key_metrics
from db_postgres.insert_postgres import insertar_en_postgres

# Rutas y configuración
SQLITE_PATH = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"
TICKERS_FILE = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/tickers_recientes_key_metrics.txt"
TABLA_SQLITE = "key_metrics"
TABLA_PG = "key_metrics_simplificada"

def main():
    if not os.path.exists(TICKERS_FILE):
        print(f"❌ Archivo de tickers recientes no encontrado: {TICKERS_FILE}")
        return

    with open(TICKERS_FILE, "r") as f:
        tickers = [line.strip() for line in f if line.strip()]

    if not tickers:
        print("⚠️ Lista de tickers vacía.")
        return

    print(f"📥 Procesando {len(tickers)} tickers desde SQLite → PostgreSQL...")

    registros = leer_jsons_recientes_por_ticker(SQLITE_PATH, TABLA_SQLITE, tickers)

    if not registros:
        print("⚠️ No se encontraron registros válidos en SQLite.")
        return

    df = normalize_key_metrics(registros)

    if df.empty:
        print("⚠️ El DataFrame normalizado está vacío. Nada que insertar.")
        return

    insertar_en_postgres(df, TABLA_PG, modo="append")

if __name__ == "__main__":
    main()
