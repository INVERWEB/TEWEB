import sqlite3
import json

# Ruta a la base de datos
db_path = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\fmp_datafree.db"

# Conectar a la base de datos
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Obtener datos de la tabla ratios
cursor.execute("SELECT raw_json FROM ratios")
rows = cursor.fetchall()

# Obtener el número total de filas para el seguimiento del progreso
total_rows = len(rows)

# Iterar sobre cada fila en la tabla ratios
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
        if calendar_year:
            anio = int(calendar_year)
        elif fiscal_year:
            anio = int(fiscal_year)
        elif date_str:
            anio = int(date_str[:4])
        else:
            print(f"No se pudo determinar el año para el ticker: {ticker}")
            continue

        # Verificar si el registro ya existe en la tabla plana
        cursor.execute("SELECT 1 FROM ratios_plana WHERE ticker = ? AND anio = ?", (ticker, anio))
        exists = cursor.fetchone()

        if exists:
            print(f"{index}/{total_rows} Registro ya existe para ticker: {ticker}, año: {anio}")
            continue

        # Crear un diccionario con los datos a insertar
        insert_data = {
            "ticker": ticker,
            "anio": anio,
            "currentRatio": data.get("currentRatio"),
            "quickRatio": data.get("quickRatio"),
            "cashRatio": data.get("cashRatio"),
            "daysOfSalesOutstanding": data.get("daysOfSalesOutstanding"),
            "daysOfInventoryOutstanding": data.get("daysOfInventoryOutstanding"),
            "operatingCycle": data.get("operatingCycle"),
            "daysOfPayablesOutstanding": data.get("daysOfPayablesOutstanding"),
            "cashConversionCycle": data.get("cashConversionCycle"),
            "grossProfitMargin": data.get("grossProfitMargin"),
            "operatingProfitMargin": data.get("operatingProfitMargin"),
            "pretaxProfitMargin": data.get("pretaxProfitMargin"),
            "netProfitMargin": data.get("netProfitMargin"),
            "effectiveTaxRate": data.get("effectiveTaxRate"),
            "returnOnAssets": data.get("returnOnAssets"),
            "returnOnEquity": data.get("returnOnEquity"),
            "returnOnCapitalEmployed": data.get("returnOnCapitalEmployed"),
            "netIncomePerEBT": data.get("netIncomePerEBT"),
            "ebtPerEbit": data.get("ebtPerEbit"),
            "ebitPerRevenue": data.get("ebitPerRevenue"),
            "debtRatio": data.get("debtRatio"),
            "debtEquityRatio": data.get("debtEquityRatio"),
            "longTermDebtToCapitalization": data.get("longTermDebtToCapitalization"),
            "totalDebtToCapitalization": data.get("totalDebtToCapitalization"),
            "interestCoverage": data.get("interestCoverage"),
            "cashFlowToDebtRatio": data.get("cashFlowToDebtRatio"),
            "companyEquityMultiplier": data.get("companyEquityMultiplier"),
            "receivablesTurnover": data.get("receivablesTurnover"),
            "payablesTurnover": data.get("payablesTurnover"),
            "inventoryTurnover": data.get("inventoryTurnover"),
            "fixedAssetTurnover": data.get("fixedAssetTurnover"),
            "assetTurnover": data.get("assetTurnover"),
            "operatingCashFlowPerShare": data.get("operatingCashFlowPerShare"),
            "freeCashFlowPerShare": data.get("freeCashFlowPerShare"),
            "cashPerShare": data.get("cashPerShare"),
            "payoutRatio": data.get("payoutRatio"),
            "operatingCashFlowSalesRatio": data.get("operatingCashFlowSalesRatio"),
            "freeCashFlowOperatingCashFlowRatio": data.get("freeCashFlowOperatingCashFlowRatio"),
            "cashFlowCoverageRatios": data.get("cashFlowCoverageRatios"),
            "shortTermCoverageRatios": data.get("shortTermCoverageRatios"),
            "capitalExpenditureCoverageRatio": data.get("capitalExpenditureCoverageRatio"),
            "dividendPaidAndCapexCoverageRatio": data.get("dividendPaidAndCapexCoverageRatio"),
            "dividendPayoutRatio": data.get("dividendPayoutRatio"),
            "priceBookValueRatio": data.get("priceBookValueRatio"),
            "priceToBookRatio": data.get("priceToBookRatio"),
            "priceToSalesRatio": data.get("priceToSalesRatio"),
            "priceEarningsRatio": data.get("priceEarningsRatio"),
            "priceToFreeCashFlowsRatio": data.get("priceToFreeCashFlowsRatio"),
            "priceToOperatingCashFlowsRatio": data.get("priceToOperatingCashFlowsRatio"),
            "priceCashFlowRatio": data.get("priceCashFlowRatio"),
            "priceEarningsToGrowthRatio": data.get("priceEarningsToGrowthRatio"),
            "dividendYield": data.get("dividendYield"),
            "enterpriseValueMultiple": data.get("enterpriseValueMultiple"),
            "priceFairValue": data.get("priceFairValue"),
        }

        # Construir la consulta SQL para insertar datos
        columns = ", ".join(insert_data.keys())
        placeholders = ", ".join(["?"] * len(insert_data))
        sql = f"INSERT INTO ratios_plana ({columns}) VALUES ({placeholders})"

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
