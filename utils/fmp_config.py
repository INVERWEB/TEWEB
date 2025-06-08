import os
from dotenv import load_dotenv

# Cargar variables desde archivo .env
load_dotenv()

API_KEY = os.getenv("FMP_API_KEY")
