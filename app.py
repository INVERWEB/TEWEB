import json
from flask import Flask, jsonify, request
import sqlite3
from pathlib import Path
from config import DB_PATH

app = Flask(__name__)


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
            WHERE UPPER(ticker) = UPPER(%s)
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

# === ENDPOINT income ===
@app.route("/api/income/<ticker>")
def get_income(ticker):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT anio, raw_json FROM income_statement WHERE UPPER(ticker) = UPPER(%s) ORDER BY anio ASC", (ticker,))
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
                "eps": data.get("eps"),
                "weightedAverageShsOutDil": data.get("weightedAverageShsOutDil")
            })
        except Exception as e:
            resultados.append({"anio": fila["anio"], "error": str(e)})

    return jsonify(resultados)
@app.route("/api/balance/<ticker>")
def get_balance_sheet(ticker):
    import json
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT anio, raw_json FROM balance_sheet WHERE UPPER(ticker) = UPPER(%s) ORDER BY anio ASC", (ticker,))
    filas = cursor.fetchall()
    conn.close()

    resultados = []
    for fila in filas:
        try:
            data = json.loads(fila["raw_json"])
            resultados.append({
                "anio": fila["anio"],
                "totalAssets": data.get("totalAssets"),
                "totalLiabilities": data.get("totalLiabilities"),
                "totalEquity": data.get("totalStockholdersEquity"),
                "cashAndShortTermInvestments": data.get("cashAndShortTermInvestments"),
                "shortTermDebt": data.get("shortTermDebt"),
                "longTermDebt": data.get("longTermDebt"),
                "inventory": data.get("inventory"),
                "accountsReceivable": data.get("netReceivables"),
                "accountsPayable": data.get("accountPayables")
            })
        except Exception as e:
            resultados.append({"anio": fila["anio"], "error": str(e)})

    return jsonify(resultados)



@app.route("/api/cashflow/<ticker>")
def get_cash_flow(ticker):
    import json
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT anio, raw_json FROM cash_flow WHERE UPPER(ticker) = UPPER(%s) ORDER BY anio ASC", (ticker,))
    filas = cursor.fetchall()
    conn.close()

    resultados = []
    for fila in filas:
        try:
            data = json.loads(fila["raw_json"])
            resultados.append({
                "anio": fila["anio"],
                "netIncome": data.get("netIncome"),
                "depreciationAndAmortization": data.get("depreciationAndAmortization"),
                "stockBasedCompensation": data.get("stockBasedCompensation"),
                "changeInWorkingCapital": data.get("changeInWorkingCapital"),
                "operatingCashFlow": data.get("operatingCashFlow"),
                "capitalExpenditure": data.get("capitalExpenditure"),
                "freeCashFlow": data.get("freeCashFlow"),
                "debtRepayment": data.get("debtRepayment"),
                "commonStockIssued": data.get("commonStockIssued")
            })
        except Exception as e:
            resultados.append({"anio": fila["anio"], "error": str(e)})

    return jsonify(resultados)



@app.route("/api/ratios/<ticker>")
def get_ratios(ticker):
    import json
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT anio, raw_json FROM ratios WHERE UPPER(ticker) = UPPER(%s) ORDER BY anio ASC", (ticker,))
    filas = cursor.fetchall()
    conn.close()

    resultados = []
    for fila in filas:
        try:
            data = json.loads(fila["raw_json"])
            resultados.append({
                "anio": fila["anio"],
                "peRatio": data.get("peRatioTTM"),
                "pegRatio": data.get("pegRatioTTM"),
                "payoutRatio": data.get("payoutRatioTTM"),
                "currentRatio": data.get("currentRatioTTM"),
                "quickRatio": data.get("quickRatioTTM"),
                "returnOnAssets": data.get("returnOnAssetsTTM"),
                "returnOnEquity": data.get("returnOnEquityTTM"),
                "grossProfitMargin": data.get("grossProfitMarginTTM"),
                "netProfitMargin": data.get("netProfitMarginTTM"),
                "operatingProfitMargin": data.get("operatingProfitMarginTTM"),
                "assetTurnover": data.get("assetTurnoverTTM")
            })
        except Exception as e:
            resultados.append({"anio": fila["anio"], "error": str(e)})

    return jsonify(resultados)


PARTIDAS_PERMITIDAS_ENTERPRISE = [
    "marketCapitalization",
    "enterpriseValue",
    "addTotalDebt",
    "minusCashAndCashEquivalents",
    "numberOfShares",
    "stockPrice"
]

# === ENDPOINT enterprise ===
@app.route("/api/enterprise/<ticker>")
def get_enterprise(ticker):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT anio, raw_json FROM enterprise_values WHERE UPPER(ticker) = UPPER(%s) ORDER BY anio ASC", (ticker,))
    filas = cursor.fetchall()
    conn.close()

    resultados = []
    for fila in filas:
        try:
            data = json.loads(fila["raw_json"])
            resultados.append({
                "anio": fila["anio"],
                "enterpriseValue": data.get("enterpriseValue"),
                "marketCapitalization": data.get("marketCapitalization"),
                "stockPrice": data.get("stockPrice"),
                "numberOfShares": data.get("numberOfShares")
            })
        except Exception as e:
            resultados.append({"anio": fila["anio"], "error": str(e)})

    return jsonify(resultados)




# --- Endpoint: Ruta del archivo .db usado ---
@app.route("/api/debug/db")
def debug_db():
    return jsonify({"path": str(DB_PATH.resolve())})

# --- Endpoint: Listado de todas las tablas ---
@app.route("/api/debug/tablas")
def listar_tablas():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tablas = [fila[0] for fila in cursor.fetchall()]
        return jsonify({"total_tablas": len(tablas), "tablas": tablas})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# --- Endpoint: Ver datos de una tabla específica ---
@app.route("/api/debug/tabla/<nombre_tabla>")
def debug_tabla(nombre_tabla):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND LOWER(name) = LOWER(?)", (nombre_tabla,))

        existe = cursor.fetchone()
        if not existe:
            return jsonify({"error": f"La tabla '{nombre_tabla}' no existe"}), 404
        cursor.execute(f"SELECT COUNT(*) FROM {nombre_tabla}")
        total = cursor.fetchone()[0]
        cursor.execute(f"SELECT * FROM {nombre_tabla} LIMIT 3")
        columnas = [desc[0] for desc in cursor.description]
        filas = cursor.fetchall()
        muestra = [dict(zip(columnas, fila)) for fila in filas]
        return jsonify({"tabla": nombre_tabla, "total_filas": total, "ejemplo": muestra})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# --- Resto de endpoints: income, balance, etc. ---
# (opcional según tu modelo)

# --- Endpoint: Subida manual del archivo .db ---
@app.route("/upload", methods=["POST"])
def upload_db():
    archivo = request.files.get("file")
    if not archivo:
        return jsonify({"error": "❌ No se recibió ningún archivo"}), 400

    destino = Path("/home/render/project/src/fmp_datafree.db")
    try:
        archivo.save(destino)
        return jsonify({
            "mensaje": "✅ Archivo recibido y guardado",
            "ruta_guardado": str(destino),
            "tamaño_MB": round(destino.stat().st_size / (1024 * 1024), 2)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)




