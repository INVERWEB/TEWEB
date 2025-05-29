# fmp_utils/fetch_statements.py
#Encargado de descargar
#los 4 endpoints (income, balance, cash flow, ratios)
import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

def get_all_statements(ticker):
    endpoints = {
        "income_statement": f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?apikey={API_KEY}",
        "balance_sheet": f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}?apikey={API_KEY}",
        "cash_flow": f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?apikey={API_KEY}",
        "ratios": f"https://financialmodelingprep.com/api/v3/ratios/{ticker}?apikey={API_KEY}"
    }

    resultados = {}

    for tipo, url in endpoints.items():
        try:
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                resultados[tipo] = data
            else:
                print(f"⚠️ Error {r.status_code} al consultar {tipo} para {ticker}")
        except Exception as e:
            print(f"❌ Excepción al consultar {tipo}: {e}")

    return resultados

