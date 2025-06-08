import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db")

def obtener_tickers_con_data(tabla):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT DISTINCT ticker FROM {tabla};")
        return sorted([r[0] for r in cur.fetchall()])
    except:
        return []
    finally:
        conn.close()


def obtener_todos_los_tickers():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT ticker FROM tickers_consultados;")
    resultado = [r[0] for r in cur.fetchall()]
    conn.close()
    return sorted(resultado)


def mostrar_calidad_descarga():
    hoy = datetime.today().strftime("%Y-%m-%d")
    print(f"\n📅 Fecha de hoy: {hoy}")

    tickers = obtener_todos_los_tickers()
    print(f"📦 Total tickers registrados: {len(tickers)}")

    tickers_ratios = obtener_tickers_con_data("ratios_plana")
    tickers_income = obtener_tickers_con_data("income_statement_plana")
    tickers_balance = obtener_tickers_con_data("balance_sheet_plana")
    tickers_cash = obtener_tickers_con_data("cash_flow_plana")
    tickers_enterprise = obtener_tickers_con_data("enterprise_values_plana")

    completos = [t for t in tickers if t in tickers_ratios and t in tickers_income and t in tickers_balance and t in tickers_cash]
    print(f"\n✅ Tickers completos (10 años × 4 estados): {len(completos)}")

    falta_ratios = [t for t in tickers if t not in tickers_ratios]
    falta_income = [t for t in tickers if t not in tickers_income]
    falta_balance = [t for t in tickers if t not in tickers_balance]
    falta_cash = [t for t in tickers if t not in tickers_cash]
    falta_enterprise = [t for t in tickers if t not in tickers_enterprise]

    print("\n📌 Tickers con faltantes:")
    print(f"   💡 {len(falta_ratios)} tickers faltan_ratios")
    print(f"   💡 {len(falta_income)} tickers falta_income_statement")
    print(f"   💡 {len(falta_balance)} tickers falta_balance_sheet")
    print(f"   💡 {len(falta_cash)} tickers falta_cash_flow")
    print(f"   💡 {len(falta_enterprise)} tickers faltan_enterprise_values")

    # Tickers con menos de 5 años de data en al menos un estado
    incompletos = 0
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for t in tickers:
        for tabla in ["income_statement_plana", "balance_sheet_plana", "cash_flow_plana", "ratios_plana"]:
            cur.execute(f"SELECT COUNT(DISTINCT anio) FROM {tabla} WHERE ticker=?", (t,))
            anios = cur.fetchone()[0]
            if anios < 5:
                incompletos += 1
                break
    conn.close()
    print(f"\n🔁 {incompletos} tickers tienen < 5 años en algún estado financiero")

    return


def resumen_sector():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT sector, COUNT(*) FROM tickers_consultados GROUP BY sector ORDER BY COUNT(*) DESC;")
    resultados = cur.fetchall()
    print("\n📊 Resumen de sectores:")
    for sector, cantidad in resultados:
        print(f"   • {sector}: {cantidad} tickers")
    conn.close()


def exportar_csv_faltantes():
    import csv
    tickers = obtener_todos_los_tickers()
    tickers_income = obtener_tickers_con_data("income_statement_plana")
    tickers_balance = obtener_tickers_con_data("balance_sheet_plana")
    tickers_cash = obtener_tickers_con_data("cash_flow_plana")
    tickers_ratios = obtener_tickers_con_data("ratios_plana")
    tickers_enterprise = obtener_tickers_con_data("enterprise_values_plana")

    incompletos = []
    for t in tickers:
        for tabla in ["income_statement_plana", "balance_sheet_plana", "cash_flow_plana", "ratios_plana", "enterprise_values_plana"]:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(DISTINCT anio) FROM {tabla} WHERE ticker=?", (t,))
            anios = cur.fetchone()[0]
            conn.close()
            if anios < 5:
                incompletos.append(t)
                break

    with open("tickers_incompletos.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker"])
        for t in sorted(set(incompletos)):
            writer.writerow([t])
    print("✅ Exportado: tickers_incompletos.csv")


if __name__ == "__main__":
    print("\n🧭 Consulta disponible:")
    print("1. Analizar calidad de tickers descargados")
    print("2. Ver resumen por sector")
    print("3. Exportar CSV de tickers incompletos (<5 años)")

    opcion = input("\nSeleccione una opción (1, 2 o 3): ").strip()

    if opcion == "1":
        mostrar_calidad_descarga()
    elif opcion == "2":
        resumen_sector()
    elif opcion == "3":
        exportar_csv_faltantes()
    else:
        print("❌ Opción inválida.")
