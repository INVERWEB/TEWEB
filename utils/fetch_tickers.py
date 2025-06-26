import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

# === CONFIGURACIÓN ===
LIMIT = 20000  # o más si querés más tickers
EXCHANGES_VALIDOS = [ 'NYSE', 'NASDAQ']

def get_tickers_by_exchange_list(limit=LIMIT, exchanges=EXCHANGES_VALIDOS):
    tickers = []
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}"

    try:
        print("🌐 Descargando lista completa de tickers desde /stock/list...")
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
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
        ]
        tickers.extend(encontrados)
        print(f"✅ {len(encontrados)} tickers encontrados en {exchange}")
    try:
        print("🌐 Solicitando /stock/list...")
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        print(f"📦 Respuesta recibida con {len(data)} registros")
    except Exception as e:
        print(f"❌ Error al obtener la lista: {e}")
        return []

    tickers_filtrados = sorted(set(tickers))[:limit]
    print(f"📦 Total único después de filtro y límite: {len(tickers_filtrados)}")
    print(f"🔍 Tickers totales antes de filtro: {len(tickers)}")
    print(f"🎯 Tickers únicos después de filtro y límite: {len(tickers_filtrados)}")

    return tickers_filtrados

# === PRUEBA MANUAL ===
if __name__ == "__main__":
    resultado = get_tickers_by_exchange_list()
    print(f"\n🎯 Total tickers únicos encontrados: {len(resultado)}")
