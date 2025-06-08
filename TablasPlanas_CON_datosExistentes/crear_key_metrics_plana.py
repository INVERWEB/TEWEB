import sqlite3
import json

from normalizadores.normalize_key_metrics import normalize_key_metrics

DB_PATH = r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db"
TABLE_ORIGEN = "key_metrics"
TABLE_DESTINO = "key_metrics_plana"

def crear_tabla_plana():
    print(f"🚧 Insertando nuevos registros desde '{TABLE_ORIGEN}' hacia '{TABLE_DESTINO}'...")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ✅ Crear tabla destino si no existe
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DESTINO} (
            ticker TEXT,
            anio TEXT,
            "revenuePerShare" ,
            "netIncomePerShare" ,
            "operatingCashFlowPerShare" ,
            "freeCashFlowPerShare" ,
            "cashPerShare" ,
            "bookValuePerShare" ,
            "tangibleBookValuePerShare",
            "shareholdersEquityPerShare" ,
            "interestDebtPerShare",
            "marketCap"
            "enterpriseValue" 
            "peRatio" 
            "priceToSalesRatio" 
            "pocfratio"
            "pfcfRatio" 
            "pbRatio" 
            "ptbRatio" 
            "evToSales" 
            "enterpriseValueOverEBITDA" 
            "evToOperatingCashFlow" 
            "evToFreeCashFlow" 
            "earningsYield" 
            "freeCashFlowYield" 
            "debtToEquity" 
            "debtToAssets" 
            "netDebtToEBITDA"
            "currentRatio"
            "interestCoverage"
            "incomeQuality" 
            "dividendYield" 
            "payoutRatio" 
            "capexPerShare" 
        )
    """)

    cursor.execute(f"SELECT ticker, anio, raw_json FROM {TABLE_ORIGEN}")
    rows = cursor.fetchall()

    insertados = 0
    duplicados = 0
    fallidos = 0

    for ticker, anio, raw_json in rows:
        try:
            raw = json.loads(raw_json)
            registro = normalize_key_metrics(raw)

            if not registro:
                fallidos += 1
                continue

            cursor.execute(
                f"SELECT 1 FROM {TABLE_DESTINO} WHERE ticker = ? AND anio = ?",
                (registro["ticker"], registro["anio"])
            )
            if cursor.fetchone():
                duplicados += 1
                continue

            columnas = ", ".join(registro.keys())
            valores = tuple(registro.values())
            placeholders = ", ".join(["?"] * len(valores))

            cursor.execute(
                f"INSERT INTO {TABLE_DESTINO} ({columnas}) VALUES ({placeholders})",
                valores
            )
            insertados += 1
        except Exception as e:
            fallidos += 1

    conn.commit()
    conn.close()

    print(f"\n📊 Resultado en '{TABLE_DESTINO}':")
    print(f"✔️ Insertados: {insertados}")
    print(f"⚠️ Duplicados: {duplicados}")
    print(f"❌ Fallidos: {fallidos}")

if __name__ == "__main__":
    crear_tabla_plana()
