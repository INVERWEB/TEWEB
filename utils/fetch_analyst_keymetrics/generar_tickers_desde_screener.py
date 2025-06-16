import requests
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

# === CONFIGURACIÓN ===
SECTOR = "Energy"
LIMIT = 5000
EXCHANGES_VALIDOS = ["NASDAQ", "NYSE", "AMEX"]
TICKERS_OUT = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/tickers_validos_analyst_key.txt")

def obtener_tickers_desde_screener():
    tickers = []
    for exchange in EXCHANGES_VALIDOS:
        url = f"https://financialmodelingprep.com/api/v3/stock-screener?exchange={exchange}&limit={LIMIT}&apikey={API_KEY}"
        if SECTOR != "ALL":
            url += f"&sector={SECTOR}"
        try:
            print(f"🌐 {exchange}: Descargando tickers del sector '{SECTOR}'...")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            nuevos = [x["symbol"] for x in data if x.get("symbol") and not x.get("isEtf", False) and not x.get("isFund", False) and x["symbol"][0].isalpha()]
            tickers.extend(nuevos)
            print(f"✅ {len(nuevos)} tickers encontrados en {exchange}")
        except Exception as e:
            print(f"❌ Error en {exchange}: {e}")
    return sorted(set(tickers))

def guardar_tickers(tickers):
    TICKERS_OUT.write_text("\n".join(tickers), encoding="utf-8")
    print(f"📄 {len(tickers)} tickers guardados en {TICKERS_OUT}")

if __name__ == "__main__":
    tickers = obtener_tickers_desde_screener()
    guardar_tickers(tickers)
