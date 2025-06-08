import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db")

def buscar_metadata(ticker):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT nombre_empresa, sector, industria, market_cap, enterprise_value, fecha_consulta
            FROM tickers_consultados
            WHERE ticker = ?
        """, (ticker,))
        row = cursor.fetchone()

    if row:
        print(f"""
📌 Nombre empresa: {row[0]}
🏢 Sector: {row[1]}
🏭 Industria: {row[2]}
💰 Market Cap: {row[3]}
🏷️ Enterprise Value: {row[4]}
📅 Fecha de consulta: {row[5]}
""")
    else:
        print("⚠️ No se encontró el ticker en la base de datos.")

def mostrar_menu():
    print("""¿🔎 Qué exploramos hoy?
📁 Income Statement = p&l
📁 Balance Sheet = balance
📁 Cash Flow = cashflow
📋 Ratios = ratios
📊 Comparables = list_10_2022:2024
✖ salir para terminar""")

def mostrar_tabla(query, params=()):
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn, params=params)
        print(df.to_string(index=False))

def procesar_comando(ticker, comando):
    if comando.startswith("p&l"):
        tabla = "income_statement"
    elif comando.startswith("balance"):
        tabla = "balance_sheet"
    elif comando.startswith("cashflow"):
        tabla = "cash_flow"
    elif comando.startswith("ratios"):
        tabla = "ratios"
    elif comando.startswith("list_"):
        try:
            partes = comando.split("_")
            cantidad = int(partes[1])
            if ":" in partes[2]:
                inicio, fin = partes[2].split(":")
                query = """
                    SELECT * FROM ratios
                    WHERE anio BETWEEN ? AND ?
                    ORDER BY anio DESC
                    LIMIT ?
                """
                mostrar_tabla(query, (inicio, fin, cantidad))
            else:
                anio = partes[2]
                query = """
                    SELECT * FROM ratios
                    WHERE anio = ?
                    ORDER BY anio DESC
                    LIMIT ?
                """
                mostrar_tabla(query, (anio, cantidad))
        except Exception as e:
            print(f"⚠️ Comando de comparables inválido: {e}")
        return
    else:
        print("❌ Comando inválido.")
        return

    if "_" in comando:
        try:
            año_rango = comando.split("_")[1]
            if ":" in año_rango:
                inicio, fin = año_rango.split(":")
                query = f"""
                    SELECT * FROM {tabla}
                    WHERE ticker = ? AND anio BETWEEN ? AND ?
                    ORDER BY anio DESC
                """
                mostrar_tabla(query, (ticker, inicio, fin))
            else:
                query = f"""
                    SELECT * FROM {tabla}
                    WHERE ticker = ? AND anio = ?
                """
                mostrar_tabla(query, (ticker, año_rango))
        except:
            print("⚠️ Comando inválido para rango de años.")
    else:
        query = f"""
            SELECT * FROM {tabla}
            WHERE ticker = ?
            ORDER BY anio DESC
        """
        mostrar_tabla(query, (ticker,))
if __name__ == "__main__":
    ticker = input("✅ Consulta ticker: ").strip().upper()
    buscar_metadata(ticker)
    while True:
        mostrar_menu()
        comando = input("👉 Ingresar comando: ").strip().lower()
        if comando == "salir":
            break
        procesar_comando(ticker, comando)
