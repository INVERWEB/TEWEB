import csv
import psycopg2

# --- CONEXIÓN A RAILWAY ---
conn = psycopg2.connect(
    dbname="railway",
    user="postgres",
    password="MYnNWOGEdrSrLhwescMRbjMKQhXCjDrM",
    host="nozomi.proxy.rlwy.net",
    port="36965"
)
cursor = conn.cursor()

# Ruta absoluta al CSV exportado
csv_path = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\tickers_consultados.csv"

# Seguimiento
total = 0
insertados = 0
omitidos = 0

print("🚀 Iniciando inserción desde CSV...")
with open(csv_path, newline='', encoding='utf-8') as archivo:
    reader = csv.DictReader(archivo)
    for row in reader:
        total += 1
        try:
            cursor.execute("""
                INSERT INTO tickers_consultados (
                    ticker, nombre_empresa, sector, industria,
                    fecha_consulta, market_cap, enterprise_value
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker) DO NOTHING;
            """, (
                row["ticker"],
                row["nombre_empresa"],
                row["sector"],
                row["industria"],
                row["fecha_consulta"] or None,
                float(row["market_cap"]) if row["market_cap"] not in ("", "NULL") else None,
                float(row["enterprise_value"]) if row["enterprise_value"] not in ("", "NULL") else None
            ))
            insertados += cursor.rowcount
        except Exception as e:
            print(f"❌ Error en fila {total} ({row['ticker']}): {e}")
            omitidos += 1

        # Log intermedio cada 100 filas
        if total % 100 == 0:
            print(f"➡️  Procesadas: {total} | Insertados: {insertados} | Omitidos: {omitidos}")

# Guardar y cerrar
conn.commit()
cursor.close()
conn.close()

# Log final
print("✅ Inserción terminada.")
print(f"📊 Total filas procesadas: {total}")
print(f"📥 Insertados exitosos: {insertados}")
print(f"⏭ Omitidos por error o duplicado: {omitidos}")
