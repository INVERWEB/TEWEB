import requests
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")
url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}"

resp = requests.get(url)
data = resp.json()

exchange_shortnames = set()
for d in data:
    val = d.get("exchangeShortName")
    if val:
        exchange_shortnames.add(val)

print(sorted(exchange_shortnames))
