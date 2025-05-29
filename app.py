
from flask import Flask, jsonify
import sqlite3
from pathlib import Path

app = Flask(__name__)
DB_PATH = Path("fmp_datafree.db")  # usa ruta relativa en Render

@app.route("/api/income/<ticker>")
def get_income_statement_plano(ticker):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT * FROM income_statement_plana
            WHERE ticker = ?
            ORDER BY anio ASC
        """, (ticker.upper(),))
        columnas = [desc[0] for desc in cursor.description]
        resultados = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
        return jsonify(resultados)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        conn.close()

if __name__ == "__main__":
    app.run(debug=True)
