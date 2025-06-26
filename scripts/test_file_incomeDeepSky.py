import sqlite3
import json

# Ruta a la base de datos
db_path = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\fmp_datafree.db"

# Conectar a la base de datos
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Obtener datos de la tabla income_statement
cursor.execute("SELECT raw_json FROM income_statement")
rows = cursor.fetchall()

# Obtener el número total de filas para el seguimiento del progreso
total_rows = len(rows)

# Iterar sobre cada fila en la tabla income_statement
for index, row in enumerate(rows, start=1):
    raw_json = row[0]
    try:
        # Parsear el JSON
        data = json.loads(raw_json)

        # Extraer ticker y anio
        ticker = data.get("symbol", "")
        date_str = data.get("date", "")
        fiscal_year = data.get("fiscalYear", "")
        calendar_year = data.get("calendarYear", "")

        # Determinar el año
        if fiscal_year:
            anio = int(fiscal_year)
        elif calendar_year:
            anio = int(calendar_year)
        elif date_str:
            anio = int(date_str[:4])
        else:
            print(f"No se pudo determinar el año para el ticker: {ticker}")
            continue

        # Crear un diccionario con los datos a insertar
        insert_data = {
            "ticker": ticker,
            "anio": anio,
            "revenue": data.get("revenue"),
            "costOfRevenue": data.get("costOfRevenue"),
            "grossProfit": data.get("grossProfit"),
            "operatingExpenses": data.get("operatingExpenses"),
            "sellingGeneralAndAdministrativeExpenses": data.get("sellingGeneralAndAdministrativeExpenses"),
            "depreciationAndAmortization": data.get("depreciationAndAmortization"),
            "otherExpenses": data.get("otherExpenses"),
            "researchAndDevelopmentExpenses": data.get("researchAndDevelopmentExpenses"),
            "operatingIncome": data.get("operatingIncome"),
            "totalOtherIncomeExpensesNet": data.get("totalOtherIncomeExpensesNet"),
            "interestIncome": data.get("interestIncome"),
            "interestExpense": data.get("interestExpense"),
            "incomeBeforeTax": data.get("incomeBeforeTax"),
            "netIncome": data.get("netIncome"),
            "ebitda": data.get("ebitda"),
            "weightedAverageShsOutDil": data.get("weightedAverageShsOutDil"),
            "eps": data.get("eps"),
            "date": data.get("date"),
            "reportedCurrency": data.get("reportedCurrency"),
            "grossProfitRatio": data.get("grossProfitRatio"),
            "generalAndAdministrativeExpenses": data.get("generalAndAdministrativeExpenses"),
            "sellingAndMarketingExpenses": data.get("sellingAndMarketingExpenses"),
            "costAndExpenses": data.get("costAndExpenses"),
            "ebitdaratio": data.get("ebitdaratio"),
            "operatingIncomeRatio": data.get("operatingIncomeRatio"),
            "incomeBeforeTaxRatio": data.get("incomeBeforeTaxRatio"),
            "incomeTaxExpense": data.get("incomeTaxExpense"),
            "netIncomeRatio": data.get("netIncomeRatio"),
            "epsdiluted": data.get("epsdiluted"),
            "weightedAverageShsOut": data.get("weightedAverageShsOut"),
            "link": data.get("link"),
            "finalLink": data.get("finalLink"),
        }

        # Construir la consulta SQL para insertar datos
        columns = ", ".join(insert_data.keys())
        placeholders = ", ".join(["?"] * len(insert_data))
        sql = f"INSERT OR IGNORE INTO income_statement_final ({columns}) VALUES ({placeholders})"

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
