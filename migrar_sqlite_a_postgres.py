import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime

# Configura conexión PostgreSQL
password = quote_plus("Boveda08@reit")  # Reemplaza con tu contraseña real
engine = create_engine(f"postgresql+psycopg2://postgres:{password}@localhost:5432/inversorweb")

# Ruta a SQLite
SQLITE_PATH = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/fmp_datafree.db"

# Tablas a migrar
TABLAS = [
    "cash_flow_plana",
    "enterprise_values_plana",
    ]

# Log y barra de progreso
def log(mensaje):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} {mensaje}")

def mostrar_progreso(actual, total):
    porcentaje = int((actual / total) * 100)
    barra = "█" * (porcentaje // 5) + "-" * (20 - porcentaje // 5)
    print(f"Progreso: [{barra}] {porcentaje}%")

# Conectar SQLite
sqlite_conn = sqlite3.connect(SQLITE_PATH)
total = len(TABLAS)

log("🚀 Iniciando migración completa...\n")

for i, tabla in enumerate(TABLAS, 1):
    try:
        log(f"📦 Migrando tabla: {tabla}")
        cursor = sqlite_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tabla}'")
        if cursor.fetchone()[0] == 0:
            log(f"⚠️ Tabla '{tabla}' no existe. Saltando...\n")
            mostrar_progreso(i, total)
            continue

        df = pd.read_sql_query(f"SELECT * FROM {tabla}", sqlite_conn)
        if df.empty:
            log(f"⚠️ Tabla '{tabla}' vacía. Saltando...\n")
            mostrar_progreso(i, total)
            continue

        df = df.applymap(lambda x: str(x).encode("utf-8", "ignore").decode("utf-8") if isinstance(x, str) else x)
        df.to_sql(tabla, engine, if_exists="replace", index=False)

        log(f"✅ Migración exitosa: {tabla} ({len(df)} filas)\n")
    except Exception as e:
        log(f"❌ Error al migrar '{tabla}': {e}\n")

    mostrar_progreso(i, total)

sqlite_conn.close()
log("🏁 Migración completada para todas las tablas.")
