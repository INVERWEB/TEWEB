import requests, time
from datetime import datetime

def fetch_key_metrics(ticker, api_key):
    url = f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?apikey={api_key}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return {
                "ticker": ticker,
                "json": response.json(),
                "fecha_extraccion": datetime.today().strftime('%Y-%m-%d')
            }
        else:
            print(f"❌ Error {response.status_code} en {ticker}")
    except Exception as e:
        print(f"⚠️ Fallo en {ticker}: {e}")
    time.sleep(0.5)
    return None
