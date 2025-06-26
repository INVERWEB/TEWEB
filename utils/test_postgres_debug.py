import os
from dotenv import load_dotenv
import psycopg2

# Cargar variables de entorno desde .env
load_dotenv("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/.env")  # Usa tu ruta real

try:
    # Conexión usando variables del .env
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        database=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        port=os.getenv("PGPORT")
    )
    if __name__ == "__main__":print("✅ Conexión a PostgreSQL exitosa.")
    conn.close()

except Exception as e:
    print("❌ Error al conectar:", e)


