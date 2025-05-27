import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("CTKkvKS7dg9MTxwrpbx5WjJv8uVeHfnb")
BASE_URL = "https://financialmodelingprep.com/api/v3"
print("API_KEY cargada:", API_KEY)
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parents[1] / '.env'
load_dotenv(dotenv_path=env_path)

API_KEY = os.getenv("FMP_API_KEY")
print("API_KEY cargada:", API_KEY)

def get_income_statement(ticker, limit=5):
    url = f"{BASE_URL}/income-statement/{ticker}?limit={limit}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        resultado = []
        for item in data:
            resultado.append({
                "anio": item.get("calendarYear"),
                "revenue": item.get("revenue"),
                "costOfRevenue": item.get("costOfRevenue"),
                "grossProfit": item.get("grossProfit"),
                "netIncome": item.get("netIncome")
            })
        return resultado
    else:
        return {"error": f"Error al consultar FMP: {response.status_code}"}

