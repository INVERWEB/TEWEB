import sqlite3
import csv

DB_PATH = "fmp_datafree.db"
OUTPUT_CSV = "tickers_por_estado_faltante.csv"

def exportar_listas_faltantes_csv():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT ticker, nombre_empresa, sector, industria, fecha_consulta FROM tickers_consultados")
    tickers_info = cur.fetchall()

    # Cargar todos los tickers con estado financiero en memoria
    def obtener_tickers_con_data(tabla):
        cur.execute(f"SELECT DISTINCT ticker FROM {tabla}")
        return set(t[0] for t in cur.fetchall())

    tickers_ratios = obtener_tickers_con_data("ratios_plana")
    tickers_income = obtener_tickers_con_data("income_statement_plana")
    tickers_balance = obtener_tickers_con_data("balance_sheet_plana")
    tickers_cash = obtener_tickers_con_data("cash_flow_plana")

    faltantes = []

    for ticker, nombre, sector, industria, fecha in tickers_info:
        if ticker not in tickers_ratios:
            faltantes.append([ticker, nombre, sector, industria, "ratios", fecha])
        if ticker not in tickers_income:
            faltantes.append([ticker, nombre, sector, industria, "income_statement", fecha])
        if ticker not in tickers_balance:
            faltantes.append([ticker, nombre, sector, industria, "balance_sheet", fecha])
        if ticker not in tickers_cash:
            faltantes.append([ticker, nombre, sector, industria, "cash_flow", fecha])

    conn.close()

    with open(OUTPUT_CSV, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Ticker", "Nombre Empresa", "Sector", "Industria", "Estado Faltante", "Fecha de Extracción"])
        writer.writerows(faltantes)

    print(f"✅ Archivo generado: {OUTPUT_CSV} ({len(faltantes)} filas)")

if __name__ == "__main__":
    exportar_listas_faltantes_csv()
