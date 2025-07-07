import psycopg2

# --- CONFIGURACIÓN ---
TABLA = 'cash_flow_plana'
LOTE = 1000  # Tamaño de lote para inserción

COLUMNAS = [
    'ticker', 'anio', 'netIncome', 'depreciationAndAmortization', 'stockBasedCompensation',
    'changeInWorkingCapital', 'accountsReceivables', 'accountsPayables', 'inventory',
    'otherWorkingCapital', 'otherNonCashItems', 'deferredIncomeTax',
    'netCashProvidedByOperatingActivities', 'operatingCashFlow',
    'investmentsInPropertyPlantAndEquipment', 'capitalExpenditure', 'acquisitionsNet',
    'purchasesOfInvestments', 'salesMaturitiesOfInvestments', 'otherInvestingActivites',
    'netCashUsedForInvestingActivites', 'commonStockIssued', 'commonStockRepurchased',
    'dividendsPaid', 'debtRepayment', 'otherFinancingActivites',
    'netCashUsedProvidedByFinancingActivities', 'netChangeInCash', 'effectOfForexChangesOnCash',
    'cashAtBeginningOfPeriod', 'cashAtEndOfPeriod', 'freeCashFlow'
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
        cur.execute(f"SELECT ticker, anio FROM {TABLA}")
        return set(cur.fetchall())

def insertar_datos_en_railway(nuevos):
    if not nuevos:
        print("🎉 Todos los registros ya están insertados.")
        return

    total = len(nuevos)
    lotes = (total // LOTE) + (1 if total % LOTE > 0 else 0)
    print(f"🚀 Insertando {total} registros en Railway ({lotes} lotes)...")

    columnas_sql = ', '.join([f'"{c}"' if any(x.isupper() for x in c) else c for c in COLUMNAS])
    valores = ', '.join(['%s'] * len(COLUMNAS))
    insert_sql = f'INSERT INTO {TABLA} ({columnas_sql}) VALUES ({valores}) ON CONFLICT DO NOTHING'

    with conn_remote.cursor() as cur:
        for i in range(lotes):
            inicio = i * LOTE
            fin = inicio + LOTE
            lote = nuevos[inicio:fin]
            print(f"📦 Insertando lote {i+1}/{lotes} ({inicio+1}-{min(fin, total)})...")
            cur.executemany(insert_sql, lote)
            conn_remote.commit()
            print(f"   ✅ Lote {i+1} insertado con éxito.")
        print(f"✅ Insertados {total} nuevos registros.")

def main():
    print("📥 Leyendo datos desde PostgreSQL local...")
    rows = leer_datos_local()
    print(f"📄 {len(rows)} registros leídos desde PostgreSQL local.")

    print("🔍 Verificando registros existentes en Railway...")
    existentes = leer_tuplas_existentes()
    nuevos = [row for row in rows if (row[0], row[1]) not in existentes]

    insertar_datos_en_railway(nuevos)

if __name__ == "__main__":
    main()
