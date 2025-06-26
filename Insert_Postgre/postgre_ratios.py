import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from pathlib import Path
from urllib.parse import quote_plus

# Cargar variables de entorno
env_path = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/.env")
load_dotenv(dotenv_path=env_path)

# Datos de conexión
user = os.getenv("PGUSER")
password = quote_plus(os.getenv("PGPASSWORD"))
host = os.getenv("PGHOST")
port = os.getenv("PGPORT")
db = os.getenv("PGDATABASE")

def get_pg_engine():
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

def leer_ratios_sqlite(db_path):
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM ratios_plana", con)
    con.close()
    return df

def filtrar_nuevos_registros(df_sqlite, tabla_pg, engine):
    with engine.connect() as conn:
        resultado = conn.execute(text(f"SELECT ticker, anio FROM {tabla_pg}"))
        existentes = set(resultado.fetchall())
    df_filtrado = df_sqlite[~df_sqlite.apply(lambda x: (x['ticker'], x['anio']) in existentes, axis=1)]
    return df_filtrado

def convertir_numericos(df):
    columnas_numericas = [
        'currentRatio', 'quickRatio', 'cashRatio', 'daysOfSalesOutstanding',
        'daysOfInventoryOutstanding', 'operatingCycle', 'daysOfPayablesOutstanding',
        'cashConversionCycle', 'grossProfitMargin', 'operatingProfitMargin',
        'pretaxProfitMargin', 'netProfitMargin', 'effectiveTaxRate', 'returnOnAssets',
        'returnOnEquity', 'returnOnCapitalEmployed', 'netIncomePerEBT', 'ebtPerEbit',
        'ebitPerRevenue', 'debtRatio', 'debtEquityRatio', 'longTermDebtToCapitalization',
        'totalDebtToCapitalization', 'interestCoverage', 'cashFlowToDebtRatio',
        'companyEquityMultiplier', 'receivablesTurnover', 'payablesTurnover',
        'inventoryTurnover', 'fixedAssetTurnover', 'assetTurnover',
        'operatingCashFlowPerShare', 'freeCashFlowPerShare', 'cashPerShare',
        'payoutRatio', 'operatingCashFlowSalesRatio', 'freeCashFlowOperatingCashFlowRatio',
        'cashFlowCoverageRatios', 'shortTermCoverageRatios', 'capitalExpenditureCoverageRatio',
        'dividendPaidAndCapexCoverageRatio', 'dividendPayoutRatio', 'priceBookValueRatio',
        'priceToBookRatio', 'priceEarningsRatio', 'priceToFreeCashFlowsRatio',
        'priceToOperatingCashFlowsRatio', 'priceCashFlowRatio', 'priceEarningsToGrowthRatio',
        'priceToSalesRatio', 'dividendYield', 'enterpriseValueMultiple', 'priceFairValue'
    ]
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def insertar_a_postgres(df, tabla_destino, engine):
    if df.empty:
        print(f"⚠️ No hay nuevos registros para insertar en '{tabla_destino}'.")
        return

    columnas_permitidas = ['ticker', 'anio'] + [
        'currentRatio', 'quickRatio', 'cashRatio', 'daysOfSalesOutstanding',
        'daysOfInventoryOutstanding', 'operatingCycle', 'daysOfPayablesOutstanding',
        'cashConversionCycle', 'grossProfitMargin', 'operatingProfitMargin',
        'pretaxProfitMargin', 'netProfitMargin', 'effectiveTaxRate', 'returnOnAssets',
        'returnOnEquity', 'returnOnCapitalEmployed', 'netIncomePerEBT', 'ebtPerEbit',
        'ebitPerRevenue', 'debtRatio', 'debtEquityRatio', 'longTermDebtToCapitalization',
        'totalDebtToCapitalization', 'interestCoverage', 'cashFlowToDebtRatio',
        'companyEquityMultiplier', 'receivablesTurnover', 'payablesTurnover',
        'inventoryTurnover', 'fixedAssetTurnover', 'assetTurnover',
        'operatingCashFlowPerShare', 'freeCashFlowPerShare', 'cashPerShare',
        'payoutRatio', 'operatingCashFlowSalesRatio', 'freeCashFlowOperatingCashFlowRatio',
        'cashFlowCoverageRatios', 'shortTermCoverageRatios', 'capitalExpenditureCoverageRatio',
        'dividendPaidAndCapexCoverageRatio', 'dividendPayoutRatio', 'priceBookValueRatio',
        'priceToBookRatio', 'priceEarningsRatio', 'priceToFreeCashFlowsRatio',
        'priceToOperatingCashFlowsRatio', 'priceCashFlowRatio', 'priceEarningsToGrowthRatio',
        'priceToSalesRatio', 'dividendYield', 'enterpriseValueMultiple', 'priceFairValue'
    ]
    df = df[[col for col in columnas_permitidas if col in df.columns]]
    df.to_sql(tabla_destino, engine, if_exists="append", index=False)
    print(f"✅ Insertados {len(df)} registros nuevos en '{tabla_destino}'.")

def main():
    ruta_db = r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"
    tabla_sqlite = "ratios_plana"
    tabla_postgres = "ratios_plana"

    print("📥 Leyendo registros desde SQLite...")
    df_raw = leer_ratios_sqlite(ruta_db)

    print("🧹 Filtrando registros ya existentes en PostgreSQL...")
    engine = get_pg_engine()
    df_nuevos = filtrar_nuevos_registros(df_raw, tabla_postgres, engine)

    print("🔢 Convirtiendo columnas numéricas...")
    df_nuevos = convertir_numericos(df_nuevos)

    print("📤 Insertando en PostgreSQL...")
    insertar_a_postgres(df_nuevos, tabla_postgres, engine)

if __name__ == "__main__":
    main()
