import sqlite3
import json

DB_PATH = r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"
TABLE_ORIGEN = "key_metrics"
TABLE_DESTINO = "key_metrics_simplificada"

# Lista de campos clave que extraeremos
CAMPOS = [
    "revenuePerShare" ,
            "netIncomePerShare" ,
            "operatingCashFlowPerShare" ,
            "freeCashFlowPerShare" ,
            "cashPerShare" ,
            "bookValuePerShare" ,
            "tangibleBookValuePerShare",
            "shareholdersEquityPerShare" ,
            "interestDebtPerShare",
            "marketCap",
            "enterpriseValue",
            "peRatio",
            "priceToSalesRatio" ,
            "pocfratio",
            "pfcfRatio",
            "pbRatio",
            "ptbRatio" ,
            "evToSales" ,
            "enterpriseValueOverEBITDA",
            "evToOperatingCashFlow" ,
            "evToFreeCashFlow" ,
            "earningsYield" ,
            "freeCashFlowYield",
            "debtToEquity",
            "debtToAssets" ,
            "netDebtToEBITDA",
            "currentRatio",
            "interestCoverage",
            "incomeQuality" ,
            "dividendYield",
            "payoutRatio" ,
            "capexPerShare",
]

def crear_tabla_simplificada():
    print(f"🚧 Creando tabla '{TABLE_DESTINO}' desde '{TABLE_ORIGEN}'...")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Crear tabla si no existe
    columnas_sql = ",\n".join([f"{campo} REAL" for campo in CAMPOS])
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DESTINO} (
            ticker TEXT,
            anio TEXT,
            {columnas_sql}
        )
    """)

    cur.execute(f"SELECT ticker, anio, raw_json FROM {TABLE_ORIGEN}")
    rows = cur.fetchall()

    insertados = 0
    duplicados = 0
    fallidos = 0

    for ticker, anio, raw_json in rows:
        try:
            data = json.loads(raw_json)
            # Verificar si ya existe
            cur.execute(f"SELECT 1 FROM {TABLE_DESTINO} WHERE ticker = %s AND anio = %s", (ticker, anio))
            if cur.fetchone():
                duplicados += 1
                continue

            valores = [data.get(campo) for campo in CAMPOS]
            cur.execute(
                f"""INSERT INTO {TABLE_DESTINO} 
                (ticker, anio, {', '.join(CAMPOS)}) 
                VALUES (%s, %s, {', '.join(['%s'] * len(CAMPOS))})""",
                (ticker, anio, *valores)
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
    crear_tabla_simplificada()
