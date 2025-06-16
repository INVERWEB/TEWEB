import os
import sys

# Asegurar ruta base del proyecto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_postgres.post_insert_utils import leer_jsons_recientes_por_ticker
from normalizadores.normalize_cash_flow import normalize_cash_flow
from db_postgres.insert_postgres import insertar_en_postgres

# Rutas absolutas y configuraciones
SQLITE_PATH = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"
TICKERS_FILE = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/tickers_recientes_cash_flow.txt"
TABLA_SQLITE = "cash_flow"
TABLA_PG = "cash_flow_plana"

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

    # Leer los registros recientes desde SQLite
    registros = leer_jsons_recientes_por_ticker(SQLITE_PATH, TABLA_SQLITE, tickers)

    if not registros:
        print("⚠️ No se encontraron registros válidos en SQLite.")
        return

    # Normalizar los registros
    df = normalize_cash_flow(registros)

    if df.empty:
        print("⚠️ El DataFrame normalizado está vacío. Nada que insertar.")
        return

    # Insertar en PostgreSQL
    insertar_en_postgres(df, TABLA_PG, modo="append")

if __name__ == "__main__":
    main()
