import os
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

def obtener_datos_perfil(ticker):
    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if not data:
            return None
        perfil = data[0]
        return {
            "ticker": perfil.get("symbol"),
            "nombre_empresa": perfil.get("companyName"),
            "sector": perfil.get("sector"),
            "industria": perfil.get("industry"),
            "market_cap": perfil.get("marketCap") or perfil.get("mktCap"),
            "enterprise_value": perfil.get("enterpriseValue") or None
        }
    except Exception as e:
        print(f"❌ Error obteniendo perfil para {ticker}: {e}")
        return None


