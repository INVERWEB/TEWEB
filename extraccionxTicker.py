import sqlite3
import json
import requests
import time
from pathlib import Path
from dotenv import load_dotenv
import os

# Cargar la API key desde el archivo .env
load_dotenv(Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/.env"))
API_KEY = os.getenv("FMP_API_KEY")

# Ruta a la base de datos
DB_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db")

# Definir los tickers para los cuales queremos extraer datos
tickers = ["CSIQ"]  # Agregar más tickers según sea necesario

# Definir los endpoints y sus tablas correspondientes
endpoints = {
    "balance_sheet": {
        "table": "balance_sheet",
        "url": "https://financialmodelingprep.com/api/v3/balance-sheet-statement/"
    },
    "income_statement": {
        "table": "income_statement",
        "url": "https://financialmodelingprep.com/api/v3/income-statement/"
    },
    "cash_flow": {
        "table": "cash_flow",
        "url": "https://financialmodelingprep.com/api/v3/cash-flow-statement/"
    },
    "key_metrics": {
        "table": "key_metrics",
        "url": "https://financialmodelingprep.com/api/v3/key-metrics/"
    },
    "ratios": {
        "table": "ratios",
        "url": "https://financialmodelingprep.com/api/v3/ratios/"
    },
    "enterprise_values": {
        "table": "enterprise_values",
        "url": "https://financialmodelingprep.com/api/v3/enterprise-values/"
    },
    "analyst_estimates": {
        "table": "analyst_estimates",
        "url": "https://financialmodelingprep.com/api/v3/analyst-estimates/"
    }
}

# Conectar a la base de datos
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

def obtener_datos_endpoint(ticker, endpoint_url):
    """Función para obtener datos de un endpoint específico."""
    url = f"{endpoint_url}{ticker}?apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error al obtener datos para {ticker} desde {endpoint_url}: {e}")
        return None

def obtener_anio_desde_datos(data):
    """Función para obtener el año desde los datos."""
    date_str = data.get("date", "")
    fiscal_year = data.get("fiscalYear", "")
    calendar_year = data.get("calendarYear", "")

    # Determinar el año
    if calendar_year:
        return calendar_year
    elif fiscal_year:
        return fiscal_year
    elif date_str:
        return date_str[:4]
    else:
        return None

def insertar_datos_en_tabla_cruda(ticker, endpoint_data, endpoint_table):
    """Función para insertar datos en la tabla cruda correspondiente."""
    if not endpoint_data:
        return 0

    for data in endpoint_data:
        try:
            # Obtener el año estandarizado
            anio = obtener_anio_desde_datos(data)
            if not anio:
                print(f"No se pudo determinar el año para el ticker: {ticker}")
                continue

            # Convertir los datos a JSON
            raw_json = json.dumps(data)

            # Insertar los datos en la tabla cruda
            cursor.execute(
                f"INSERT INTO {endpoint_table} (ticker, anio, raw_json) VALUES (?, ?, ?)",
                (ticker, anio, raw_json)
            )

            print(f"Datos crudos insertados para {ticker} en {endpoint_table} para el año {anio}")

        except Exception as e:
            print(f"Error al insertar datos crudos para {ticker} en {endpoint_table}: {e}")

def obtener_metadatos_ticker(ticker):
    """Función para obtener metadatos de un ticker específico."""
    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data:
            # Asumimos que el primer elemento contiene los datos del perfil
            profile_data = data[0]
            return {
                "nombre_empresa": profile_data.get("companyName"),
                "sector": profile_data.get("sector"),
                "industria": profile_data.get("industry"),
                "market_cap": profile_data.get("mktCap"),
                "enterprise_value": profile_data.get("enterpriseValue")
            }
    except requests.RequestException as e:
        print(f"Error al obtener metadatos para {ticker}: {e}")
    return None

def registrar_ticker_consultado(ticker):
    """Función para registrar un nuevo ticker consultado en la tabla de metadatos."""
    metadatos = obtener_metadatos_ticker(ticker)
    if metadatos:
        try:
            cursor.execute(
                """INSERT INTO tickers_consultados (ticker, nombre_empresa, sector, industria, fecha_consulta, market_cap, enterprise_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (ticker,
                 metadatos["nombre_empresa"],
                 metadatos["sector"],
                 metadatos["industria"],
                 time.strftime("%Y-%m-%d %H:%M:%S"),
                 metadatos["market_cap"],
                 metadatos["enterprise_value"])
            )
            print(f"Ticker registrado en metadatos: {ticker}")
        except Exception as e:
            print(f"Error al registrar ticker en metadatos: {e}")

def main():
    for ticker in tickers:
        print(f"\nProcesando ticker: {ticker}")

        # Procesar cada endpoint para el ticker
        total_endpoints_procesados = 0
        for endpoint, endpoint_info in endpoints.items():
            print(f"Obteniendo datos de {endpoint} para {ticker}")
            data = obtener_datos_endpoint(ticker, endpoint_info["url"])
            if data:
                insertar_datos_en_tabla_cruda(ticker, data, endpoint_info["table"])
                total_endpoints_procesados += 1
            time.sleep(0.20)  # Esperar para evitar sobrecargar la API

        # Registrar el ticker en metadatos
        registrar_ticker_consultado(ticker)

        # Mensaje de log para el ticker procesado
        print(f"✅ {ticker}: {total_endpoints_procesados} de los 7 endpoints procesados.")

    # Confirmar los cambios en la base de datos
    conn.commit()

if __name__ == "__main__":
    main()
    conn.close()
    print("Proceso de extracción completado.")
