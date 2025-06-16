# test_entorno.py
import requests
from dotenv import load_dotenv
import os

# Carga variables del archivo .env
load_dotenv()

# Prueba de entorno
print("✅ Entorno virtual funcionando correctamente.")

# Prueba de variable de entorno
api_key = os.getenv("FMP_API_KEY")
print("🔐 FMP_API_KEY cargada desde .env:", api_key)

# Prueba de conexión HTTP
response = requests.get("https://httpbin.org/get")
if response.status_code == 200:
    print("🌐 Conexión a internet OK, respuesta JSON:")
    print(response.json())
else:
    print("❌ Fallo conexión:", response.status_code)
