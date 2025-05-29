import sqlite3
import pandas as pd

DB_PATH = "fmp_datafree.db"

def mostrar_tabla(query, params=()):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    print(df.to_string(index=False))

def buscar_metadata(cursor, ticker):
    cursor.execute("""
        SELECT nombre_empresa, sector, industria, fecha_consulta
        FROM tickers_consultados
        WHERE ticker = ?
    """, (ticker,))
    row = cursor.fetchone()
    if row:
        print(f"""
📌 Nombre empresa: {row[0]}
🏢 Sector: {row[1]}
🏭 Industria: {row[2]}
📅 Fecha de consulta: {row[3]}
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

def procesar_comando(ticker, comando):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

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
                query = f"""
                    SELECT * FROM ratios
                    WHERE anio BETWEEN ? AND ?
                    ORDER BY anio DESC
                    LIMIT ?
                """
                mostrar_tabla(query, (inicio, fin, cantidad))
            else:
                anio = partes[2]
                query = f"""
                    SELECT * FROM ratios
                    WHERE anio = ?
                    ORDER BY anio DESC
                    LIMIT ?
                """
                mostrar_tabla(query, (anio, cantidad))
        except:
            print("⚠️ Comando de comparables inválido.")
        return

    else:
        print("❌ Comando inválido.")
        return

    if "_" in comando:
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
    else:
        query = f"""
            SELECT * FROM {tabla}
            WHERE ticker = ?
            ORDER BY anio DESC
        """
        mostrar_tabla(query, (ticker,))

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("✅ Consulta ticker:", end=" ")
    ticker = input().strip().upper()
    buscar_metadata(cursor, ticker)

    while True:
        mostrar_menu()
        comando = input("👉 Ingresar comando: ").strip().lower()
        if comando == "salir":
            break
        procesar_comando(ticker, comando)

    conn.close()

if __name__ == "__main__":
    main()

