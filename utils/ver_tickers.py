import sqlite3
import pandas as pd

DB_PATH = "fmp_datafree.db"

def listar_tickers():
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT DISTINCT ticker, nombre_empresa, sector, industria, fecha_consulta
        FROM tickers_consultados
        ORDER BY ticker
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("⚠️ No se encontraron tickers en la base de datos.")
    else:
        print("📋 Tickers disponibles:")
        print(df.to_string(index=False))

if __name__ == "__main__":
    listar_tickers()
