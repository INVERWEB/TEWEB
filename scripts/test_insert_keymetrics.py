import sqlite3
import json

# Ruta a la base de datos
db_path = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\fmp_datafree.db"

# Conectar a la base de datos
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Obtener datos de la tabla key_metrics
cursor.execute("SELECT raw_json FROM key_metrics")
rows = cursor.fetchall()

# Obtener el número total de filas para el seguimiento del progreso
total_rows = len(rows)

# Iterar sobre cada fila en la tabla key_metrics
for index, row in enumerate(rows, start=1):
    raw_json = row[0]
    try:
        # Parsear el JSON
        data = json.loads(raw_json)

        # Extraer ticker y anio
        ticker = data.get("symbol", "")
        calendar_year = data.get("calendarYear", "")
        date_str = data.get("date", "")

        # Determinar el año
        if calendar_year:
            anio = calendar_year
        elif date_str:
            anio = date_str[:4]
        else:
            print(f"No se pudo determinar el año para el ticker: {ticker}")
            continue

        # Verificar si el registro ya existe en la tabla plana
        cursor.execute("SELECT 1 FROM key_metrics_simplificada WHERE ticker = ? AND anio = ?", (ticker, anio))
        exists = cursor.fetchone()

        if exists:
            print(f"{index}/{total_rows} Registro ya existe para ticker: {ticker}, año: {anio}")
            continue

        # Crear un diccionario con los datos a insertar
        insert_data = {
            "ticker": ticker,
            "anio": anio,
            "revenuePerShare": data.get("revenuePerShare"),
            "netIncomePerShare": data.get("netIncomePerShare"),
            "operatingCashFlowPerShare": data.get("operatingCashFlowPerShare"),
            "freeCashFlowPerShare": data.get("freeCashFlowPerShare"),
            "cashPerShare": data.get("cashPerShare"),
            "bookValuePerShare": data.get("bookValuePerShare"),
            "tangibleBookValuePerShare": data.get("tangibleBookValuePerShare"),
            "shareholdersEquityPerShare": data.get("shareholdersEquityPerShare"),
            "interestDebtPerShare": data.get("interestDebtPerShare"),
            "marketCap": data.get("marketCap"),
            "enterpriseValue": data.get("enterpriseValue"),
            "peRatio": data.get("peRatio"),
            "priceToSalesRatio": data.get("priceToSalesRatio"),
            "pocfratio": data.get("pocfratio"),
            "pfcfRatio": data.get("pfcfRatio"),
            "pbRatio": data.get("pbRatio"),
            "ptbRatio": data.get("ptbRatio"),
            "evToSales": data.get("evToSales"),
            "enterpriseValueOverEBITDA": data.get("enterpriseValueOverEBITDA"),
            "evToOperatingCashFlow": data.get("evToOperatingCashFlow"),
            "evToFreeCashFlow": data.get("evToFreeCashFlow"),
            "earningsYield": data.get("earningsYield"),
            "freeCashFlowYield": data.get("freeCashFlowYield"),
            "debtToEquity": data.get("debtToEquity"),
            "debtToAssets": data.get("debtToAssets"),
            "netDebtToEBITDA": data.get("netDebtToEBITDA"),
            "currentRatio": data.get("currentRatio"),
            "interestCoverage": data.get("interestCoverage"),
            "incomeQuality": data.get("incomeQuality"),
            "dividendYield": data.get("dividendYield"),
            "payoutRatio": data.get("payoutRatio"),
            "capexPerShare": data.get("capexPerShare"),
        }

        # Construir la consulta SQL para insertar datos
        columns = ", ".join(insert_data.keys())
        placeholders = ", ".join(["?"] * len(insert_data))
        sql = f"INSERT INTO key_metrics_simplificada ({columns}) VALUES ({placeholders})"

        # Ejecutar la consulta SQL
        cursor.execute(sql, list(insert_data.values()))

        # Imprimir el progreso
        print(f"{index}/{total_rows} Procesando ticker: {ticker}, año: {anio}")

    except json.JSONDecodeError as e:
        print(f"Error al parsear JSON: {e}")
    except Exception as e:
        print(f"Error al procesar la fila: {e}")

# Confirmar los cambios
conn.commit()

# Cerrar la conexión
conn.close()

print("Proceso completado.")
