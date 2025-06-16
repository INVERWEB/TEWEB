import json
import pandas as pd
import os
from dotenv import load_dotenv

# 1. Carga las variables del entorno (.env)
load_dotenv(dotenv_path="E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/.env")

# 2. Importa la función de inserción
from db_postgres.insert_postgres import insertar_en_postgres

# 3. Ruta absoluta al archivo JSON de prueba
json_path = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/FTNT_testprueba.json"

# 4. Cargar el JSON como lista de diccionarios
with open(json_path, "r", encoding="utf-8") as file:
    data = json.load(file)

# 5. Convertir a DataFrame directamente (ya viene plano)
df = pd.DataFrame(data)
print(df.head())       # primeras filas
print(df.columns)      # todas las columnas
print(df.dtypes)       # tipos de dato por columna

# 6. Insertar en PostgreSQL usando tu función (modo append para no sobrescribir)
insertar_en_postgres(df, nombre_tabla="balance_sheet_ftnt_prueba", modo="replace")
