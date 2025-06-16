import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# 🔐 PostgreSQL: configura tu conexión
password = quote_plus("Boveda08@reit")
engine = create_engine(f"postgresql+psycopg2://postgres:{password}@localhost:5432/inversorweb")

# 📦 Ruta a tu archivo SQLite
SQLITE_PATH = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"
NOMBRE_TABLA = "income_statement_plana"

def migrar_tabla(nombre_tabla):
    try:
        print(f"📦 Iniciando migración de tabla: {nombre_tabla}")

        sqlite_conn = sqlite3.connect(SQLITE_PATH)
        cursor = sqlite_conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{nombre_tabla}'")
        if cursor.fetchone()[0] == 0:
            print(f"⚠️ Tabla '{nombre_tabla}' no existe en SQLite. Abortando...\n")
            return

        df = pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", sqlite_conn)
        sqlite_conn.close()

        if df.empty:
            print(f"⚠️ La tabla '{nombre_tabla}' está vacía. Abortando...\n")
            return

        # ✅ Limpieza de texto con codificación robusta
        df = df.applymap(lambda x: str(x).encode("utf-8", "ignore").decode("utf-8") if isinstance(x, str) else x)

        df.to_sql(nombre_tabla, engine, if_exists="replace", index=False)
        print(f"✅ Migración exitosa: {nombre_tabla} ({len(df)} filas)\n")

    except Exception as e:
        print(f"❌ Error al migrar '{nombre_tabla}': {e}\n")

if __name__ == "__main__":
    migrar_tabla(NOMBRE_TABLA)
