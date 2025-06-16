import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Credenciales (ajusta según tu configuración)
password = quote_plus("Boveda08@reit")  # Usa tu contraseña real aquí

# Conexión a PostgreSQL
engine = create_engine(f"postgresql+psycopg2://postgres:{password}@localhost:5432/inversorweb")

# Leer tabla
df = pd.read_sql("SELECT * FROM test_keymetrics_json", engine)

# Mostrar algunas filas
print(df.head(10))  # Cambia a df.to_string() para ver todo
