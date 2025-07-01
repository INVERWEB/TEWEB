import os
from flask import Flask, request, jsonify
from conexion_pg import SessionLocal
from sqlalchemy import text

app = Flask(__name__)

@app.route('/')
def status():
    entorno = os.getenv("ENVIRONMENT", "local")
    return {
        "status": "✅ API funcionando",
        "entorno": entorno
    }

# Reutilizamos la lógica exacta para todos los endpoints
def ejecutar_consulta_por_ticker(nombre_tabla, ticker):
    session = SessionLocal()
    try:
        ticker_upper = ticker.strip().upper()

        # Verificación en tickers_consultados
        check_query = text("""
            SELECT 1 FROM tickers_consultados
            WHERE ticker = :ticker
            LIMIT 1;
        """)
        check_result = session.execute(check_query, {"ticker": ticker_upper}).fetchone()
        if not check_result:
            print(f"🚫 Ticker no listado: {ticker_upper}")
            return {"error": "Ticker no listado en tickers_consultados"}, 404

        # Consulta a la tabla específica
        data_query = text(f"""
            SELECT * FROM {nombre_tabla}
            WHERE ticker = :ticker
            ORDER BY anio;
        """)
        result = session.execute(data_query, {"ticker": ticker_upper})
        columnas = result.keys()
        filas = result.fetchall()
        data = [dict(zip(columnas, fila)) for fila in filas]

        print(f"✅ Consulta exitosa en '{nombre_tabla}' para {ticker_upper}: {len(data)} registros")
        return data, 200

    except Exception as e:
        print(f"💥 ERROR en '{nombre_tabla}' con '{ticker}': {e}")
        return {"error": str(e)}, 500

    finally:
        session.close()
        print("🔚 Sesión cerrada para:", ticker)

# ENDPOINTS UNO A UNO
@app.route('/income_statement_final', methods=['GET'])
def endpoint_income():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400
    data, status = ejecutar_consulta_por_ticker("income_statement_final", ticker)
    return jsonify(data), status

@app.route('/balance_sheet_plana', methods=['GET'])
def endpoint_balance():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400
    data, status = ejecutar_consulta_por_ticker("balance_sheet_plana", ticker)
    return jsonify(data), status

@app.route('/cash_flow_plana', methods=['GET'])
def endpoint_cashflow():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400
    data, status = ejecutar_consulta_por_ticker("cash_flow_plana", ticker)
    return jsonify(data), status

@app.route('/ratios_plana', methods=['GET'])
def endpoint_ratios():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400
    data, status = ejecutar_consulta_por_ticker("ratios_plana", ticker)
    return jsonify(data), status

@app.route('/enterprise_values_plana', methods=['GET'])
def endpoint_enterprise():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400
    data, status = ejecutar_consulta_por_ticker("enterprise_values_plana", ticker)
    return jsonify(data), status

@app.route('/key_metrics_simplificada', methods=['GET'])
def endpoint_keymetrics():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400
    data, status = ejecutar_consulta_por_ticker("key_metrics_simplificada", ticker)
    return jsonify(data), status

@app.route('/analyst_estimates_plana', methods=['GET'])
def endpoint_estimates():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400
    data, status = ejecutar_consulta_por_ticker("analyst_estimates_plana", ticker)
    return jsonify(data), status

@app.route('/buscar_tickers', methods=['GET'])
def buscar_tickers():
    letra = request.args.get('q', '').upper().strip()

    if not letra:
        return jsonify([])

    session = SessionLocal()
    try:
        query = text("""
            SELECT ticker FROM tickers_consultados
            WHERE ticker ILIKE :letra
            ORDER BY ticker
            LIMIT 10;
        """)
        result = session.execute(query, {"letra": f"{letra}%"})
        tickers = [r[0] for r in result.fetchall()]
        return jsonify(tickers)

    except Exception as e:
        print("💥 Error en /buscar_tickers:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()

# Iniciar servidor
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 Iniciando servidor Flask en puerto {port}...")
    app.run(host='0.0.0.0', port=port)
