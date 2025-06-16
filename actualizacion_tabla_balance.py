import sqlite3
import json

DB_PATH = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\INVERSORWEB\fmp_datafree.db"
TABLE = "balance_sheet_plana"

CAMPOS = [
    "cashAndCashEquivalents", "cashAndShortTermInvestments", "shortTermInvestments",
    "netReceivables", "inventory", "otherCurrentAssets", "totalCurrentAssets",
    "propertyPlantEquipmentNet", "goodwillAndIntangibleAssets", "goodwill",
    "intangibleAssets", "otherAssets", "totalNonCurrentAssets", "totalAssets",
    "accountPayables", "shortTermDebt", "deferredRevenue", "otherCurrentLiabilities",
    "totalCurrentLiabilities", "longTermDebt", "capitalLeaseObligations",
    "deferredRevenueNonCurrent", "deferredTaxLiabilitiesNonCurrent", "otherLiabilities",
    "totalNonCurrentLiabilities", "totalLiabilities", "commonStock", "preferredStock",
    "retainedEarnings", "minorityInterest", "totalEquity", "totalStockholdersEquity",
    "totalLiabilitiesAndTotalEquity", "netDebt", "totalDebt", "totalInvestments", "taxAssets"
]

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

actualizados = 0
fallidos = 0

cur.execute(f"SELECT ticker, anio, raw_json FROM {TABLE} WHERE raw_json IS NOT NULL")
registros = cur.fetchall()

for ticker, anio, raw in registros:
    try:
        data = json.loads(raw)
        valores = [data.get(c) for c in CAMPOS]

        set_clause = ", ".join([f"{campo} = %s" for campo in CAMPOS])
        sql = f"UPDATE {TABLE} SET {set_clause} WHERE ticker = %s AND anio = %s"

        cur.execute(sql, (*valores, ticker, anio))
        actualizados += 1
    except Exception as e:
        print(f"❌ Error en {ticker}-{anio}: {e}")
        fallidos += 1

conn.commit()
conn.close()

print(f"\n📊 ACTUALIZACIÓN COMPLETA:")
print(f"✔️ Registros actualizados: {actualizados}")
print(f"❌ Fallidos: {fallidos}")
print(f"🧾 Total procesados: {len(registros)}")
