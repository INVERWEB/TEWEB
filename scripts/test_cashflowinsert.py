import sqlite3
import json

# Ruta a la base de datos
db_path = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\fmp_datafree.db"

# Conectar a la base de datos
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Obtener datos de la tabla cash_flow
cursor.execute("SELECT raw_json FROM cash_flow")
rows = cursor.fetchall()

# Obtener el número total de filas para el seguimiento del progreso
total_rows = len(rows)

# Iterar sobre cada fila en la tabla cash_flow
for index, row in enumerate(rows, start=1):
    raw_json = row[0]
    try:
        # Parsear el JSON
        data = json.loads(raw_json)

        # Extraer ticker y anio
        ticker = data.get("symbol", "")
        date_str = data.get("date", "")
        fiscal_year = data.get("fiscalYear", "")

        # Determinar el año
        if fiscal_year:
            anio = int(fiscal_year)
        elif date_str:
            anio = int(date_str[:4])
        else:
            print(f"No se pudo determinar el año para el ticker: {ticker}")
            continue

        # Verificar si el registro ya existe en la tabla plana
        cursor.execute("SELECT 1 FROM cash_flow_plana WHERE ticker = ? AND anio = ?", (ticker, anio))
        exists = cursor.fetchone()

        if exists:
            print(f"{index}/{total_rows} Registro ya existe para ticker: {ticker}, año: {anio}")
            continue

        # Crear un diccionario con los datos a insertar
        insert_data = {
            "ticker": ticker,
            "anio": anio,
            "netIncome": data.get("netIncome"),
            "depreciationAndAmortization": data.get("depreciationAndAmortization"),
            "stockBasedCompensation": data.get("stockBasedCompensation"),
            "changeInWorkingCapital": data.get("changeInWorkingCapital"),
            "accountsReceivables": data.get("accountsReceivables"),
            "accountsPayables": data.get("accountsPayables"),
            "inventory": data.get("inventory"),
            "otherWorkingCapital": data.get("otherWorkingCapital"),
            "otherNonCashItems": data.get("otherNonCashItems"),
            "deferredIncomeTax": data.get("deferredIncomeTax"),
            "netCashProvidedByOperatingActivities": data.get("netCashProvidedByOperatingActivities"),
            "operatingCashFlow": data.get("operatingCashFlow"),
            "investmentsInPropertyPlantAndEquipment": data.get("investmentsInPropertyPlantAndEquipment"),
            "capitalExpenditure": data.get("capitalExpenditure"),
            "acquisitionsNet": data.get("acquisitionsNet"),
            "purchasesOfInvestments": data.get("purchasesOfInvestments"),
            "salesMaturitiesOfInvestments": data.get("salesMaturitiesOfInvestments"),
            "otherInvestingActivites": data.get("otherInvestingActivites"),
            "netCashUsedForInvestingActivites": data.get("netCashUsedForInvestingActivites"),
            "commonStockIssued": data.get("commonStockIssued"),
            "commonStockRepurchased": data.get("commonStockRepurchased"),
            "dividendsPaid": data.get("dividendsPaid"),
            "debtRepayment": data.get("debtRepayment"),
            "otherFinancingActivites": data.get("otherFinancingActivites"),
            "netCashUsedProvidedByFinancingActivities": data.get("netCashUsedProvidedByFinancingActivities"),
            "netChangeInCash": data.get("netChangeInCash"),
            "effectOfForexChangesOnCash": data.get("effectOfForexChangesOnCash"),
            "cashAtBeginningOfPeriod": data.get("cashAtBeginningOfPeriod"),
            "cashAtEndOfPeriod": data.get("cashAtEndOfPeriod"),
            "freeCashFlow": data.get("freeCashFlow"),
        }

        # Construir la consulta SQL para insertar datos
        columns = ", ".join(insert_data.keys())
        placeholders = ", ".join(["?"] * len(insert_data))
        sql = f"INSERT INTO cash_flow_plana ({columns}) VALUES ({placeholders})"

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
