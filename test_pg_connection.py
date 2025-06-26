import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_postgres.userAdmin import get_pg_connection


def probar_conexion():
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print("✅ Conexión exitosa a PostgreSQL:")
        print(version)
        conn.close()
    except Exception as e:
        print("❌ Error de conexión:", e)

if __name__ == "__main__":
    probar_conexion()
