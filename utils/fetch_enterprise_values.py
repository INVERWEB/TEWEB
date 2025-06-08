import requests
import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")
DB_PATH = "fmp_datafree.db"

def crear_tabla_enterprise():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS enterprise_values (
            ticker TEXT,
            date TEXT,
            marketCapitalization REAL,
            enterpriseValue REAL,
            numberOfShares REAL,
            stockPrice REAL,
            cashAndCashEquivalents REAL,
            totalDebt REAL,
            ebitda REAL
        );
    """)
    conn.commit()
    conn.close()

def fetch_and_store_enterprise_values(ticker):
    url = f"https://financialmodelingprep.com/api/v3/enterprise-values/{ticker}?limit=15&apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("enterpriseValues", [])

        if not data:
            print(f"⚠️ No hay datos EV para {ticker}")
            return False

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        for entry in data:
            cur.execute("""
                INSERT INTO enterprise_values (
                    ticker, date, marketCapitalization, enterpriseValue,
                    numberOfShares, stockPrice, cashAndCashEquivalents,
                    totalDebt, ebitda
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker,
                entry.get("date"),
                entry.get("marketCapitalization"),
                entry.get("enterpriseValue"),
                entry.get("numberOfShares"),
                entry.get("stockPrice"),
                entry.get("cashAndCashEquivalents"),
                entry.get("totalDebt"),
                entry.get("ebitda")
            ))

        conn.commit()
        conn.close()
        print(f"✅ Enterprise Values guardados para {ticker}")
        return True

    except Exception as e:
        print(f"❌ Error al procesar EV para {ticker}: {e}")
        return False

if __name__ == "__main__":
    crear_tabla_enterprise()
    fetch_and_store_enterprise_values("AAPL")

