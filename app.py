from flask import Flask, jsonify
from utils.fmp_api import get_income_statement
from dotenv import load_dotenv
import os

load_dotenv()  # Carga variables de .env
API_KEY = os.getenv("API_KEY_FMP")
print("API_KEY cargada:", API_KEY)

app = Flask(__name__)

@app.route("/api/datos/<string:ticker>", methods=["GET"])
def obtener_datos(ticker):
    datos = get_income_statement(ticker)
    return jsonify(datos)

if __name__ == "__main__":
    app.run(debug=True)
