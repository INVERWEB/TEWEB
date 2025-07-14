import psycopg2
import csv
import re
import logging
from pathlib import Path
from dotenv import load_dotenv
import os

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Cargar variables desde .env si existe
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path)

# Conexión a PostgreSQL (Railway)
conn = psycopg2.connect(
    host="nozomi.proxy.rlwy.net",
    database="railway",
    user="postgres",
    password="MYnNWOGEdrSrLhwescMRbjMKQhXCjDrM",
    port="36965"
)
cursor = conn.cursor()
logging.info("✅ Conectado a PostgreSQL (production)")

# CSV de origen ya corregido
archivo_csv = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\ETF3_industria_corregido.csv"

def normalizar(texto):
    if not texto:
        return ""
    return re.sub(r'[^a-z0-9]', '', texto.lower())

# Leer CSV
with open(archivo_csv, encoding="utf-8") as f:
    lector_csv = csv.DictReader(f)
    total_actualizadas = 0

    for fila in lector_csv:
        industria_csv = fila["industria_google"].strip()
        industria_norm = normalizar(industria_csv)

        try:
            pe = fila.get("pe_promedio") or None
            netdebt = fila.get("netdebt/ebitda_promedio")
            netdebt = float(netdebt) if netdebt else None
            growth = fila.get("growth_sales_industria")
            growth = float(growth) if growth else None
            etf_name = fila.get("etf_name") or None
            etf_sigla = fila.get("etf_seguimiento") or None
            notion = fila.get("notion") or None

            cursor.execute("""
                UPDATE mapa_industrias
                SET
                    pe_promedio = COALESCE(pe_promedio, %s),
                    "netdebt/ebitda_promedio" = COALESCE("netdebt/ebitda_promedio", %s),
                    growth_sales_industria = COALESCE(growth_sales_industria, %s),
                    etf_name = COALESCE(etf_name, %s),
                    etf_seguimiento = COALESCE(etf_seguimiento, %s),
                    notion = COALESCE(notion, %s)
                WHERE LOWER(REGEXP_REPLACE(industria_google, '[^a-z0-9]', '', 'g')) = %s
                  AND (
                        pe_promedio IS NULL OR
                        "netdebt/ebitda_promedio" IS NULL OR
                        growth_sales_industria IS NULL OR
                        etf_name IS NULL OR
                        etf_seguimiento IS NULL OR
                        notion IS NULL
                  )
            """, (
                pe, netdebt, growth, etf_name, etf_sigla, notion, industria_norm
            ))

            if cursor.rowcount > 0:
                logging.info(f"🟢 Actualizado: {industria_csv}")
                total_actualizadas += cursor.rowcount
            else:
                logging.warning(f"⚠️ No actualizado (ya poblado o sin coincidencia): {industria_csv}")

        except Exception as e:
            logging.error(f"❌ Error en fila [{industria_csv}]: {e}")

conn.commit()
cursor.close()
conn.close()
logging.info(f"🎯 Total filas actualizadas: {total_actualizadas}")
logging.info("🔒 Conexión cerrada.")
