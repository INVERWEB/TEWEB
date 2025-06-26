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

def leer_balance_sheet_sqlite(db_path):
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM balance_sheet_plana", con)
    con.close()
    return df

def filtrar_nuevos_registros(df_sqlite, tabla_pg, engine):
    with engine.connect() as conn:
        resultado = conn.execute(text(f"SELECT ticker, anio FROM {tabla_pg}"))
        existentes = set(resultado.fetchall())
    df_filtrado = df_sqlite[~df_sqlite.apply(lambda x: (x['ticker'], x['anio']) in existentes, axis=1)]
    return df_filtrado

def insertar_a_postgres(df, tabla_destino, engine):
    if df.empty:
        print(f"⚠️ No hay nuevos registros para insertar en '{tabla_destino}'.")
        return

    columnas_permitidas = [
        'ticker', 'anio', 'cashAndCashEquivalents', 'cashAndShortTermInvestments', 'shortTermInvestments',
        'netReceivables', 'inventory', 'otherCurrentAssets', 'totalCurrentAssets', 'propertyPlantEquipmentNet',
        'goodwillAndIntangibleAssets', 'goodwill', 'intangibleAssets', 'otherAssets', 'totalNonCurrentAssets',
        'totalAssets', 'accountPayables', 'shortTermDebt', 'deferredRevenue', 'otherCurrentLiabilities',
        'totalCurrentLiabilities', 'longTermDebt', 'capitalLeaseObligations', 'deferredRevenueNonCurrent',
        'deferredTaxLiabilitiesNonCurrent', 'otherLiabilities', 'totalNonCurrentLiabilities', 'totalLiabilities',
        'commonStock', 'preferredStock', 'retainedEarnings', 'minorityInterest', 'totalEquity',
        'totalStockholdersEquity', 'totalLiabilitiesAndTotalEquity', 'netDebt', 'totalDebt', 'totalInvestments',
        'taxAssets', 'raw_json', 'date', 'symbol', 'reportedCurrency', 'cik', 'filingDate', 'acceptedDate',
        'fiscalYear', 'period', 'accountsReceivables', 'otherReceivables', 'prepaids', 'longTermInvestments',
        'otherNonCurrentAssets', 'totalPayables', 'otherPayables', 'accruedExpenses',
        'capitalLeaseObligationsCurrent', 'taxPayables', 'capitalLeaseObligationsNonCurrent',
        'otherNonCurrentLiabilities', 'treasuryStock', 'additionalPaidInCapital',
        'accumulatedOtherComprehensiveIncomeLoss', 'otherTotalStockholdersEquity', 'link', 'finalLink'
    ]

    columnas_numericas = [
        'cashAndCashEquivalents', 'cashAndShortTermInvestments', 'shortTermInvestments', 'netReceivables',
        'inventory', 'otherCurrentAssets', 'totalCurrentAssets', 'propertyPlantEquipmentNet',
        'goodwillAndIntangibleAssets', 'goodwill', 'intangibleAssets', 'otherAssets', 'totalNonCurrentAssets',
        'totalAssets', 'accountPayables', 'shortTermDebt', 'deferredRevenue', 'otherCurrentLiabilities',
        'totalCurrentLiabilities', 'longTermDebt', 'capitalLeaseObligations', 'deferredRevenueNonCurrent',
        'deferredTaxLiabilitiesNonCurrent', 'otherLiabilities', 'totalNonCurrentLiabilities', 'totalLiabilities',
        'commonStock', 'preferredStock', 'retainedEarnings', 'minorityInterest', 'totalEquity',
        'totalStockholdersEquity', 'totalLiabilitiesAndTotalEquity', 'netDebt', 'totalDebt', 'totalInvestments',
        'taxAssets', 'accountsReceivables', 'otherReceivables', 'prepaids', 'longTermInvestments',
        'otherNonCurrentAssets', 'totalPayables', 'otherPayables', 'accruedExpenses',
        'capitalLeaseObligationsCurrent', 'taxPayables', 'capitalLeaseObligationsNonCurrent',
        'otherNonCurrentLiabilities', 'treasuryStock', 'additionalPaidInCapital',
        'accumulatedOtherComprehensiveIncomeLoss', 'otherTotalStockholdersEquity'
    ]

    # Asegurar columnas válidas
    df = df[[col for col in columnas_permitidas if col in df.columns]]

    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df.to_sql(tabla_destino, engine, if_exists="append", index=False)
    print(f"✅ Insertados {len(df)} registros nuevos en '{tabla_destino}'.")

def main():
    ruta_db = r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"
    tabla_sqlite = "balance_sheet_plana"
    tabla_postgres = "balance_sheet_plana"

    print("📥 Leyendo registros desde SQLite...")
    df_raw = leer_balance_sheet_sqlite(ruta_db)

    print("🧹 Filtrando registros ya existentes en PostgreSQL...")
    engine = get_pg_engine()
    df_nuevos = filtrar_nuevos_registros(df_raw, tabla_postgres, engine)

    print("📤 Insertando en PostgreSQL...")
    insertar_a_postgres(df_nuevos, tabla_postgres, engine)

if __name__ == "__main__":
    main()
