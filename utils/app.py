from flask import Flask, jsonify
import sqlite3
import os
from dotenv import load_dotenv

# Cargar la clave desde .env
load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

# Configuración
DB_PATH = "fmp_datafree.db"

app = Flask(__name__)

@app.route("/api/datos/<ticker>", methods=["GET"])
def obtener_datos_financieros(ticker):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Buscar últimos 5 años disponibles
        cursor.execute("""
            SELECT anio, revenue, costOfRevenue, grossProfit, netIncome
            FROM income_statement
            WHERE ticker = ?
            ORDER BY anio DESC
            LIMIT 5
        """, (ticker.upper(),))
        resultados = cursor.fetchall()

        if not resultados:
            return jsonify({"error": "Ticker no encontrado"}), 404

        # Convertir a lista de dicts
        data = []
        for fila in resultados:
            data.append({
                "anio": fila[0],
                "revenue": fila[1],
                "costOfRevenue": fila[2],
                "grossProfit": fila[3],
                "netIncome": fila[4],
            })

        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

if __name__ == "__main__":
    print("API_KEY cargada:", API_KEY)
    app.run(debug=True)

