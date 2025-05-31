from flask import Flask, jsonify
import sqlite3
from pathlib import Path

app = Flask(__name__)

# === CONFIGURACIÓN DE BASE DE DATOS ===
DB_PATH = Path("/data/fmp_datafree.db")

# === PARTIDAS PERMITIDAS POR ESTADO ===
PARTIDAS_PERMITIDAS_INCOME = [
    "anio", "revenue", "costOfRevenue", "grossProfit", "grossProfitMargin",
    "operatingExpenses", "sellingGeneralAndAdministrativeExpenses",
    "depreciationAndAmortization", "stockBasedCompensation",
    "researchAndDevelopmentExpenses", "operatingIncome", "operatingIncomeRatio",
    "totalOtherIncomeExpensesNet", "interestIncome", "interest_expense",
    "incomeBeforeTax", "incomeBeforeTaxRatio", "netIncome", "netIncomeRatio",
    "ebitda", "ebitdaRatio", "epsdiluted", "weightedAverageShsOutDil"
]

PARTIDAS_PERMITIDAS_BALANCE = [
    "anio", "cashAndCashEquivalents", "shortTermInvestments", "netReceivables",
    "inventory", "totalCurrentAssets", "propertyPlantEquipmentNet", "goodwill",
    "totalAssets", "accountsPayable", "shortTermDebt", "totalCurrentLiabilities",
    "longTermDebt", "totalLiabilities", "commonStock", "retainedEarnings",
    "totalStockholdersEquity"
]

PARTIDAS_PERMITIDAS_CASHFLOW = [
    "anio", "netIncome", "depreciationAndAmortization", "stockBasedCompensation",
    "changesInWorkingCapital", "cashFromOperatingActivities",
    "capitalExpenditure", "acquisitionsNet", "purchasesOfInvestments",
    "cashFromInvestingActivities", "debtRepayment", "commonStockIssued",
    "dividendsPaid", "cashFromFinancingActivities", "netChangeInCash",
    "cashAtEndOfPeriod"
]

# === FUNCIÓN REUTILIZABLE PARA OBTENER DATOS ===
def obtener_partidas(ticker, tabla, partidas_permitidas):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT * FROM {tabla}
            WHERE UPPER(ticker) = UPPER(?)
            ORDER BY anio ASC
        """, (ticker,))

        filas = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]

        resultados = []
        for fila in filas:
            fila_dict = dict(zip(columnas, fila))
            filtrado = {}
            for k in partidas_permitidas:
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

        return resultados if resultados else [{"error": f"No data found for {ticker} in {tabla}"}]

    except Exception as e:
        return [{"error": str(e)}]
    finally:
        conn.close()


# === ENDPOINTS ===
@app.route("/")
def home():
    return "✅ API InversorWeb activa. Usa /api/income/<ticker>, /api/balance/<ticker> o /api/cashflow/<ticker>"

@app.route("/api/income/<ticker>")
def income(ticker):
    return jsonify(obtener_partidas(ticker, "income_statement_plana", PARTIDAS_PERMITIDAS_INCOME))

@app.route("/api/balance/<ticker>")
def balance(ticker):
    return jsonify(obtener_partidas(ticker, "balance_sheet_plana", PARTIDAS_PERMITIDAS_BALANCE))

@app.route("/api/cashflow/<ticker>")
def cashflow(ticker):
    return jsonify(obtener_partidas(ticker, "cashflow_statement_plana", PARTIDAS_PERMITIDAS_CASHFLOW))

@app.route("/api/ratios/<ticker>")
def get_ratios(ticker):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT *
            FROM ratios_plana
            WHERE UPPER(ticker) = UPPER(?)
            ORDER BY anio ASC
        """, (ticker,))

        filas = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]
        resultados = []

        for fila in filas:
            fila_dict = dict(zip(columnas, fila))
            for k, v in fila_dict.items():
                if v in [None, "None", "null"]:
                    fila_dict[k] = ""
                else:
                    try:
                        num = float(v)
                        fila_dict[k] = round(num, 4) if k != "anio" else v
                    except:
                        pass
            resultados.append(fila_dict)

        return jsonify(resultados)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        conn.close()

# === EJECUCIÓN DEL SERVIDOR ===
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

