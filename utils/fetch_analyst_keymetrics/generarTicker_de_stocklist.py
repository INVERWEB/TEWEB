import requests
import os
from dotenv import load_dotenv
from pathlib import Path

# === Cargar API Key desde .env ===
load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

# === CONFIGURACIÓN ===
LIMIT = 25000
EXCHANGES_VALIDOS = [
   'NYSE','LIS', 'LSE'
]
TICKERS_OUT = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/tickers_validos_analyst_key.txt")

def get_tickers_by_exchange_list(limit=LIMIT, exchanges=EXCHANGES_VALIDOS):
    tickers = []
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}"

    try:
        print("🌐 Descargando lista completa de tickers desde /stock/list...")
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        print(f"📦 Respuesta recibida con {len(data)} registros")
    except Exception as e:
        print(f"❌ Error al obtener la lista: {e}")
        return []

    for exchange in exchanges:
        encontrados = [
            x["symbol"]
            for x in data
            if x.get("exchangeShortName") == exchange
            and not x.get("isEtf", False)
            and not x.get("isFund", False)
            and x.get("symbol")
            and x["symbol"][0].isalpha()
        ]
        tickers.extend(encontrados)
        print(f"✅ {len(encontrados)} tickers encontrados en {exchange}")

    tickers_filtrados = sorted(set(tickers))[:limit]
    print(f"📦 Total único después de filtro y límite: {len(tickers_filtrados)}")
    print(f"🔍 Tickers totales antes de filtro: {len(tickers)}")
    print(f"🎯 Tickers únicos después de filtro y límite: {len(tickers_filtrados)}")
    return tickers_filtrados

def guardar_tickers(tickers):
    TICKERS_OUT.write_text("\n".join(tickers), encoding="utf-8")
    print(f"📄 {len(tickers)} tickers guardados en {TICKERS_OUT}")

# === PRUEBA MANUAL ===
if __name__ == "__main__":
    resultado = get_tickers_by_exchange_list()
    guardar_tickers(resultado)
