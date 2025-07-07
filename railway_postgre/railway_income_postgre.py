import psycopg2

# Conexión directa a PostgreSQL en Railway (⚠️ reemplaza si tus credenciales cambian)
def get_conn_railway():
    return psycopg2.connect(
        host="nozomi.proxy.rlwy.net",
        port=36965,
        database="railway",
        user="postgres",
        password="MYnNWOGEdrSrLhwescMRbjMKQhXCjDrM"
    )

# Crear tabla si no existe
def crear_tabla_income_statement(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS income_statement_final (
                "ticker" TEXT,
                "anio" INTEGER,
                "revenue" DOUBLE PRECISION,
                "costOfRevenue" DOUBLE PRECISION,
                "grossProfit" DOUBLE PRECISION,
                "operatingExpenses" DOUBLE PRECISION,
                "sellingGeneralAndAdministrativeExpenses" DOUBLE PRECISION,
                "depreciationAndAmortization" DOUBLE PRECISION,
                "otherExpenses" DOUBLE PRECISION,
                "researchAndDevelopmentExpenses" DOUBLE PRECISION,
                "operatingIncome" DOUBLE PRECISION,
                "totalOtherIncomeExpensesNet" DOUBLE PRECISION,
                "interestIncome" DOUBLE PRECISION,
                "interestExpense" DOUBLE PRECISION,
                "incomeBeforeTax" DOUBLE PRECISION,
                "netIncome" DOUBLE PRECISION,
                "ebitda" DOUBLE PRECISION,
                "weightedAverageShsOutDil" DOUBLE PRECISION,
                "eps" DOUBLE PRECISION,
                "date" DATE,
                "reportedCurrency" TEXT,
                "grossProfitRatio" DOUBLE PRECISION,
                "generalAndAdministrativeExpenses" DOUBLE PRECISION,
                "sellingAndMarketingExpenses" DOUBLE PRECISION,
                "costAndExpenses" DOUBLE PRECISION,
                "ebitdaratio" DOUBLE PRECISION,
                "operatingIncomeRatio" DOUBLE PRECISION,
                "incomeBeforeTaxRatio" DOUBLE PRECISION,
                "incomeTaxExpense" DOUBLE PRECISION,
                "netIncomeRatio" DOUBLE PRECISION,
                "epsdiluted" DOUBLE PRECISION,
                "weightedAverageShsOut" DOUBLE PRECISION,
                "link" TEXT,
                "finalLink" TEXT
            );
        """)
        conn.commit()
        print("✅ Tabla 'income_statement_final' creada o ya existía.")

# Inserta los datos recibidos (formato: lista de tuplas)
def insertar_datos(rows):
    conn = get_conn_railway()
    crear_tabla_income_statement(conn)

    with conn.cursor() as cur:
        insert_sql = """
            INSERT INTO income_statement_final VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            );
        """
        cur.executemany(insert_sql, rows)
        conn.commit()
        print(f"✅ Insertados {len(rows)} registros en Railway.")

    conn.close()

# Simulación de datos: para pruebas reales, importa desde otro módulo
def main():
    print("⚠️ Este script no extrae datos. Espera una lista de registros desde otro módulo.")
    print("✅ Usa la función insertar_datos(rows) pasando una lista de tuplas.")

if __name__ == "__main__":
    main()
