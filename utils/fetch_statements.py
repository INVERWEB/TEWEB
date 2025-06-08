import requests
import time
import json
from utils.db_utils import insertar_json_generico
from utils.fmp_config import API_KEY

BASE_URL = "https://financialmodelingprep.com/api/v3"
ENDPOINTS = {
    "income_statement": "/income-statement/{}?limit=120&apikey={}",
    "balance_sheet": "/balance-sheet-statement/{}?limit=120&apikey={}",
    "cash_flow": "/cash-flow-statement/{}?limit=120&apikey={}",
    "ratios": "/ratios/{}?limit=120&apikey={}",
    "enterprise_values": "/enterprise-values/{}?limit=120&apikey={}"
}

def get_all_statements(ticker):
    resultados = {}
    for clave, url_template in ENDPOINTS.items():
        url = BASE_URL + url_template.format(ticker, API_KEY)
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Validar estructura y formato
            if isinstance(data, dict) and "error" in data:
                print(f"⚠️ Error en endpoint {clave} para {ticker}: {data['error']}")
                continue

            if isinstance(data, list) and len(data) > 0:
                resultados[clave] = data
            else:
                print(f"⚠️ {clave} vacío para {ticker}")

        except Exception as e:
            print(f"❌ Error solicitando {clave} para {ticker}: {e}")

        time.sleep(0.20)  # Evitar rate limit
    return resultados



