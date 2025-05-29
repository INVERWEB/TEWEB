# fmp_utils/fetch_tickers.py
# Encargado de pedir tickers por industria sectordesde FMP

import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

def get_tickers_by_sector_and_industry(sector="Technology", industry="Semiconductors", limit=100):
    url = f"https://financialmodelingprep.com/api/v3/stock-screener?sector={sector}&industry={industry}&limit={limit}&apikey={API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        return [item['symbol'] for item in response.json()]
    else:
        print(f"❌ Error al obtener tickers: {response.status_code}")
        return []

def listar_industrias_por_sector(sector="Technology", limite=1000):
    url = f"https://financialmodelingprep.com/api/v3/stock-screener?sector={sector}&limit={limite}&apikey={API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        industrias = sorted(set([item.get("industry", "N/A") for item in data]))
        print(f"📊 Industrias únicas en el sector '{sector}':")
        for i, industria in enumerate(industrias, 1):
            print(f"{i}. {industria}")
        return industrias
    else:
        print(f"❌ Error: {response.status_code}")
        return []

