from flask import Flask, jsonify
from pathlib import Path
import sqlite3
import json
from config import DB_PATH

app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({"message": "API Flask TEWEB lista"})

@app.route("/api/income/<ticker>")
def get_income(ticker):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT anio, raw_json FROM income_statement WHERE UPPER(ticker) = UPPER(?) ORDER BY anio", (ticker,))
    filas = cursor.fetchall()
    conn.close()

    resultados = []
    for fila in filas:
        try:
            data = json.loads(fila["raw_json"])
            resultados.append({
                "anio": fila["anio"],
                "revenue": data.get("revenue"),
                "netIncome": data.get("netIncome"),
                "ebitda": data.get("ebitda"),
                "eps": data.get("eps")
            })
        except:
            resultados.append({"anio": fila["anio"], "error": "json inválido"})

    return jsonify(resultados)

if __name__ == "__main__":
    app.run(debug=True)
