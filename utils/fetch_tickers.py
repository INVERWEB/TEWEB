
import os
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

def get_tickers_by_sector(sector, limit=50):
    """
    Devuelve una lista de tickers únicos del sector solicitado.
    """
    url = f"https://financialmodelingprep.com/api/v3/stock-screener?sector={sector}&limit={limit}&apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return [item["symbol"] for item in data if "symbol" in item]
    except Exception as e:
        print(f"❌ Error al obtener tickers para sector {sector}: {e}")
        return []
