from flask import Flask, jsonify
import sqlite3
from pathlib import Path


# === CONFIGURACIÓN FLASK ===
app = Flask(__name__)
DB_PATH = Path("fmp_datafree.db")

PARTIDAS_PERMITIDAS = [
    "anio", "revenue", "costOfRevenue", "grossProfit", "grossProfitMargin",
    "operatingExpenses", "sellingGeneralAndAdministrativeExpenses",
    "depreciationAndAmortization", "stockBasedCompensation",
    "researchAndDevelopmentExpenses", "operatingIncome", "operatingIncomeRatio",
    "totalOtherIncomeExpensesNet", "interestIncome", "interest_expense",
    "incomeBeforeTax", "incomeBeforeTaxRatio", "netIncome", "netIncomeRatio",
    "ebitda", "ebitdaRatio", "epsdiluted", "weightedAverageShsOutDil"
]

# === ENDPOINT PRINCIPAL ===
@app.route("/api/income/<ticker>")
def get_income_statement(ticker):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT *
            FROM income_statement_plana
            WHERE UPPER(ticker) = UPPER(?)
            ORDER BY anio ASC
        """, (ticker,))

        filas = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]
        resultados = []

        for fila in filas:
            fila_dict = dict(zip(columnas, fila))
            filtrado = {}
            for k in PARTIDAS_PERMITIDAS:
                val = fila_dict.get(k)
                if val in [None, "None", "null"]:
                    val = ""
                else:
                    try:
                        num = float(val)
                        val = round(num / 1_000_000, 2) if k != "anio" else val
                    except:
                        pass
                filtrado[k] = val
            resultados.append(filtrado)

        return jsonify(resultados)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        conn.close()

# === EJECUCIÓN DEL SERVIDOR ===
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
