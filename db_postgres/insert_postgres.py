#🔧 Inserta registros normalizados (desde Pandas DataFrames)
# en tu base PostgreSQL usando SQLAlchemy.

from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
import os

def get_engine_from_env():
    user = os.getenv("PGUSER", "postgres")
    password = quote_plus(os.getenv("PGPASSWORD", "Boveda08@reit"))  # Codifica caracteres especiales
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "inversorweb")

    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

def limpiar_utf8(df):
    return df.applymap(lambda x: str(x).encode("utf-8", "ignore").decode("utf-8") if isinstance(x, str) else x)

def insertar_en_postgres(df, nombre_tabla, modo='replace', limpiar_texto=True):
    try:
        engine = get_engine_from_env()

        if limpiar_texto:
            df = limpiar_utf8(df)

        df.to_sql(nombre_tabla, con=engine, if_exists=modo, index=False)
        print(f"✅ Insertado correctamente en '{nombre_tabla}' ({len(df)} filas)")
    except Exception as e:
        print(f"❌ Error al insertar en '{nombre_tabla}': {e}")
