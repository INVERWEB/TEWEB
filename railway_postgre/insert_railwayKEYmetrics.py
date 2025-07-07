import psycopg2

# --- CONFIGURACIÓN ---
TABLA = 'key_metrics_simplificada'
LOTE = 1000

COLUMNAS = [
    "capexPerShare", "dividendYield", "payoutRatio", "revenuePerShare", "netIncomePerShare",
    "operatingCashFlowPerShare", "freeCashFlowPerShare", "cashPerShare", "bookValuePerShare",
    "tangibleBookValuePerShare", "shareholdersEquityPerShare", "interestDebtPerShare", "marketCap",
    "enterpriseValue", "peRatio", "priceToSalesRatio", "pocfratio", "pfcfRatio", "pbRatio",
    "ptbRatio", "evToSales", "enterpriseValueOverEBITDA", "evToOperatingCashFlow", "evToFreeCashFlow",
    "earningsYield", "freeCashFlowYield", "debtToEquity", "debtToAssets", "netDebtToEBITDA",
    "currentRatio", "interestCoverage", "incomeQuality", "anio", "ticker"
]

# --- CONEXIÓN LOCAL ---
conn_local = psycopg2.connect(
    host="localhost",
    database="inversorweb",
    user="postgres",
    password="Boveda08@reit",
    port="5432"
)

# --- CONEXIÓN RAILWAY ---
conn_remote = psycopg2.connect(
    host="nozomi.proxy.rlwy.net",
    database="railway",
    user="postgres",
    password="MYnNWOGEdrSrLhwescMRbjMKQhXCjDrM",
    port="36965"
)

def leer_datos_local():
    with conn_local.cursor() as cur:
        columnas_sql = ', '.join([f'"{c}"' if any(x.isupper() for x in c) else c for c in COLUMNAS])
        cur.execute(f"SELECT {columnas_sql} FROM {TABLA}")
        return cur.fetchall()

def leer_tuplas_existentes():
    with conn_remote.cursor() as cur:
        cur.execute(f'SELECT ticker, anio FROM {TABLA}')
        return set(cur.fetchall())

def insertar_datos_en_railway(nuevos):
    if not nuevos:
        print("🎉 Todos los registros ya están insertados.")
        return

    total = len(nuevos)
    lotes = (total // LOTE) + (1 if total % LOTE > 0 else 0)
    print(f"🚀 Insertando {total} registros en Railway ({lotes} lotes)...")

    columnas_sql = ', '.join([f'"{c}"' if any(x.isupper() for x in c) else c for c in COLUMNAS])
    placeholders = ', '.join(['%s'] * len(COLUMNAS))
    insert_sql = f'INSERT INTO {TABLA} ({columnas_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'

    with conn_remote.cursor() as cur:
        for i in range(lotes):
            inicio = i * LOTE
            fin = min(inicio + LOTE, total)
            print(f"📦 Insertando lote {i+1}/{lotes} ({inicio+1}-{fin})...")
            cur.executemany(insert_sql, nuevos[inicio:fin])
            conn_remote.commit()
            print(f"   ✅ Lote {i+1} insertado.")
    print("✅ Inserción finalizada.")

def main():
    print("📥 Leyendo registros desde PostgreSQL local...")
    registros = leer_datos_local()
    print(f"📄 Total leídos: {len(registros)}")

    print("🔎 Consultando registros ya existentes en Railway...")
    existentes = leer_tuplas_existentes()
    nuevos = [r for r in registros if (r[-1], r[-2]) not in existentes]  # ticker, anio

    insertar_datos_en_railway(nuevos)

if __name__ == "__main__":
    main()
