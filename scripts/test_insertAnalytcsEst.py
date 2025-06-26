import sqlite3
import json

# Ruta a la base de datos
db_path = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\fmp_datafree.db"

# Conectar a la base de datos
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Obtener datos de la tabla analyst_estimates
cursor.execute("SELECT raw_json FROM analyst_estimates")
rows = cursor.fetchall()

# Obtener el número total de filas para el seguimiento del progreso
total_rows = len(rows)

# Iterar sobre cada fila en la tabla analyst_estimates
for index, row in enumerate(rows, start=1):
    raw_json = row[0]
    try:
        # Parsear el JSON
        data = json.loads(raw_json)

        # Extraer ticker y anio
        ticker = data.get("symbol", "")
        date_str = data.get("date", "")

        # Determinar el año
        if date_str:
            anio = date_str[:4]  # Tomar los primeros cuatro caracteres para el año
        else:
            print(f"No se pudo determinar el año para el ticker: {ticker}")
            continue

        # Verificar si el registro ya existe en la tabla plana
        cursor.execute("SELECT 1 FROM analyst_estimates_plana WHERE ticker = ? AND anio = ?", (ticker, anio))
        exists = cursor.fetchone()

        if exists:
            print(f"{index}/{total_rows} Registro ya existe para ticker: {ticker}, año: {anio}")
            continue

        # Crear un diccionario con los datos a insertar
        insert_data = {
            "ticker": ticker,
            "anio": anio,
            "estimatedRevenueLow": data.get("estimatedRevenueLow"),
            "estimatedRevenueHigh": data.get("estimatedRevenueHigh"),
            "estimatedRevenueAvg": data.get("estimatedRevenueAvg"),
            "estimatedEbitdaLow": data.get("estimatedEbitdaLow"),
            "estimatedEbitdaHigh": data.get("estimatedEbitdaHigh"),
            "estimatedEbitdaAvg": data.get("estimatedEbitdaAvg"),
            "estimatedEbitLow": data.get("estimatedEbitLow"),
            "estimatedEbitHigh": data.get("estimatedEbitHigh"),
            "estimatedEbitAvg": data.get("estimatedEbitAvg"),
            "estimatedNetIncomeLow": data.get("estimatedNetIncomeLow"),
            "estimatedNetIncomeHigh": data.get("estimatedNetIncomeHigh"),
            "estimatedNetIncomeAvg": data.get("estimatedNetIncomeAvg"),
            "estimatedSgaExpenseLow": data.get("estimatedSgaExpenseLow"),
            "estimatedSgaExpenseHigh": data.get("estimatedSgaExpenseHigh"),
            "estimatedSgaExpenseAvg": data.get("estimatedSgaExpenseAvg"),
            "estimatedEpsAvg": data.get("estimatedEpsAvg"),
            "estimatedEpsHigh": data.get("estimatedEpsHigh"),
            "estimatedEpsLow": data.get("estimatedEpsLow"),
            "numberAnalystEstimatedRevenue": data.get("numberAnalystEstimatedRevenue"),
            "numberAnalystsEstimatedEps": data.get("numberAnalystsEstimatedEps"),
        }

        # Construir la consulta SQL para insertar datos
        columns = ", ".join(insert_data.keys())
        placeholders = ", ".join(["?"] * len(insert_data))
        sql = f"INSERT INTO analyst_estimates_plana ({columns}) VALUES ({placeholders})"

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
