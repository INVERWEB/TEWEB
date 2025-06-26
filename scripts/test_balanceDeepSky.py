import sqlite3
import json

# Ruta a la base de datos
db_path = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\fmp_datafree.db"

# Conectar a la base de datos
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Obtener datos de la tabla balance_sheet
cursor.execute("SELECT raw_json FROM balance_sheet")
rows = cursor.fetchall()

# Obtener el número total de filas para el seguimiento del progreso
total_rows = len(rows)

# Iterar sobre cada fila en la tabla balance_sheet
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
        cursor.execute("SELECT 1 FROM balance_sheet_plana WHERE ticker = ? AND anio = ?", (ticker, anio))
        exists = cursor.fetchone()

        if exists:
            print(f"{index}/{total_rows} Registro ya existe para ticker: {ticker}, año: {anio}")
            continue

        # Crear un diccionario con los datos a insertar
        insert_data = {
            "ticker": ticker,
            "anio": anio,
            "cashAndCashEquivalents": data.get("cashAndCashEquivalents"),
            "cashAndShortTermInvestments": data.get("cashAndShortTermInvestments"),
            "shortTermInvestments": data.get("shortTermInvestments"),
            "netReceivables": data.get("netReceivables"),
            "inventory": data.get("inventory"),
            "otherCurrentAssets": data.get("otherCurrentAssets"),
            "totalCurrentAssets": data.get("totalCurrentAssets"),
            "propertyPlantEquipmentNet": data.get("propertyPlantEquipmentNet"),
            "goodwillAndIntangibleAssets": data.get("goodwillAndIntangibleAssets"),
            "goodwill": data.get("goodwill"),
            "intangibleAssets": data.get("intangibleAssets"),
            "otherAssets": data.get("otherAssets"),
            "totalNonCurrentAssets": data.get("totalNonCurrentAssets"),
            "totalAssets": data.get("totalAssets"),
            "accountPayables": data.get("accountPayables"),
            "shortTermDebt": data.get("shortTermDebt"),
            "deferredRevenue": data.get("deferredRevenue"),
            "otherCurrentLiabilities": data.get("otherCurrentLiabilities"),
            "totalCurrentLiabilities": data.get("totalCurrentLiabilities"),
            "longTermDebt": data.get("longTermDebt"),
            "capitalLeaseObligations": data.get("capitalLeaseObligations"),
            "deferredRevenueNonCurrent": data.get("deferredRevenueNonCurrent"),
            "deferredTaxLiabilitiesNonCurrent": data.get("deferredTaxLiabilitiesNonCurrent"),
            "otherLiabilities": data.get("otherLiabilities"),
            "totalNonCurrentLiabilities": data.get("totalNonCurrentLiabilities"),
            "totalLiabilities": data.get("totalLiabilities"),
            "commonStock": data.get("commonStock"),
            "retainedEarnings": data.get("retainedEarnings"),
            "minorityInterest": data.get("minorityInterest"),
            "totalEquity": data.get("totalEquity"),
            "totalLiabilitiesAndTotalEquity": data.get("totalLiabilitiesAndTotalEquity"),
            "totalInvestments": data.get("totalInvestments"),
            "totalDebt": data.get("totalDebt"),
            "netDebt": data.get("netDebt"),
            "date": data.get("date"),
            "symbol": data.get("symbol"),
            "reportedCurrency": data.get("reportedCurrency"),
            "cik": data.get("cik"),
            "filingDate": data.get("filingDate"),
            "acceptedDate": data.get("acceptedDate"),
            "fiscalYear": data.get("fiscalYear"),
            "period": data.get("period"),
            "accountsReceivables": data.get("accountsReceivables"),
            "otherReceivables": data.get("otherReceivables"),
            "prepaids": data.get("prepaids"),
            "longTermInvestments": data.get("longTermInvestments"),
            "otherNonCurrentAssets": data.get("otherNonCurrentAssets"),
            "totalPayables": data.get("totalPayables"),
            "otherPayables": data.get("otherPayables"),
            "accruedExpenses": data.get("accruedExpenses"),
            "capitalLeaseObligationsCurrent": data.get("capitalLeaseObligationsCurrent"),
            "taxPayables": data.get("taxPayables"),
            "capitalLeaseObligationsNonCurrent": data.get("capitalLeaseObligationsNonCurrent"),
            "otherNonCurrentLiabilities": data.get("otherNonCurrentLiabilities"),
            "treasuryStock": data.get("treasuryStock"),
            "additionalPaidInCapital": data.get("additionalPaidInCapital"),
            "accumulatedOtherComprehensiveIncomeLoss": data.get("accumulatedOtherComprehensiveIncomeLoss"),
            "otherTotalStockholdersEquity": data.get("otherTotalStockholdersEquity"),
            "link": data.get("link"),
            "finalLink": data.get("finalLink"),
        }

        # Construir la consulta SQL para insertar datos
        columns = ", ".join(insert_data.keys())
        placeholders = ", ".join(["?"] * len(insert_data))
        sql = f"INSERT INTO balance_sheet_plana ({columns}) VALUES ({placeholders})"

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
