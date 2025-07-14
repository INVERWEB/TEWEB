import os
from flask import Flask, request, jsonify
from conexion_pg import SessionLocal
from sqlalchemy import text
import threading
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
@app.route('/tickers_consultados', methods=['GET'])
def obtener_info_ticker():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400

    session = SessionLocal()
    try:
        query = text("""
            SELECT
                market_cap,
                enterprise_value,
                sector,
                ticker,
                fecha_consulta,
                industria,
                nombre_empresa
            FROM tickers_consultados
            WHERE ticker = :ticker
            LIMIT 1;
        """)
        result = session.execute(query, {"ticker": ticker.upper()})
        fila = result.fetchone()

        if not fila:
            return jsonify([]), 200

        columnas = result.keys()
        return jsonify([dict(zip(columnas, fila))])

    except Exception as e:
        print("💥 Error en /tickers_consultados:", e)
        return jsonify({"error": str(e)}), 500
    
# Diccionario de caché en memoria y bloqueo para concurrencia
cache_industrias = {}
lock_cache = threading.Lock()

@app.route('/industria_google', methods=['GET'])
def obtener_industria_google():
    industria_fmp = request.args.get('industria_fmp')

    if not industria_fmp:
        return jsonify({"error": "Se requiere el parámetro 'industria_fmp'"}), 400

    industria_fmp = industria_fmp.strip()

    with lock_cache:
        if industria_fmp in cache_industrias:
            return jsonify({
                "industria_fmp": industria_fmp,
                "industria_google": cache_industrias[industria_fmp],
                "cache": True
            })

    session = SessionLocal()
    try:
        query = text("""
            SELECT industria_google 
            FROM mapa_industrias 
            WHERE LOWER(industria_fmp) = LOWER(:industria_fmp)
            LIMIT 1;
        """)
        result = session.execute(query, {"industria_fmp": industria_fmp})
        fila = result.fetchone()

        if not fila:
            return jsonify({
                "industria_fmp": industria_fmp,
                "industria_google": "Pendiente",
                "error": "Industria no encontrada en base"
            }), 404

        industria_google = fila[0]

        with lock_cache:
            cache_industrias[industria_fmp] = industria_google

        return jsonify({
            "industria_fmp": industria_fmp,
            "industria_google": industria_google,
            "cache": False
        })

    except Exception as e:
        print("💥 Error en /industria_google:", e)
        return jsonify({"error": str(e)}), 500
    
@app.route('/mapa_por_industria', methods=['GET'])
def obtener_por_industria_google():
    industria_google = request.args.get('industria_google')
    if not industria_google:
        return jsonify({"error": "Se requiere el parámetro 'industria_google'"}), 400

    session = SessionLocal()
    try:
        query = text("""
            SELECT * FROM mapa_industrias
            WHERE LOWER(industria_google) = LOWER(:industria)
            ORDER BY industria_fmp;
        """)
        result = session.execute(query, {"industria": industria_google})
        filas = result.fetchall()

        if not filas:
            return jsonify([]), 200

        columnas = result.keys()
        data = [dict(zip(columnas, fila)) for fila in filas]
        return jsonify(data)

    except Exception as e:
        print("💥 Error en /mapa_por_industria:", e)
        return jsonify({"error": str(e)}), 500

    

    finally:
        session.close()


# Iniciar servidor
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 Iniciando servidor Flask en puerto {port}...")
    app.run(host='0.0.0.0', port=port)
