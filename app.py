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

@app.route('/income_statement_final', methods=['GET'])
def get_todo_por_ticker():
    ticker = request.args.get('ticker')

    if not ticker:
        return jsonify({"error": "Se requiere el parámetro 'ticker'"}), 400

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
            return jsonify({"error": "Ticker no listado en tickers_consultados"}), 404

        # Consulta a income_statement_final
        data_query = text("""
            SELECT * FROM income_statement_final
            WHERE ticker = :ticker
            ORDER BY anio;
        """)
        result = session.execute(data_query, {"ticker": ticker_upper})
        columnas = result.keys()
        filas = result.fetchall()

        data = [dict(zip(columnas, fila)) for fila in filas]
        print(f"✅ Consulta exitosa para {ticker_upper}: {len(data)} registros")
        return jsonify(data)

    except Exception as e:
        print(f"💥 ERROR al procesar '{ticker}': {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()
        print("🔚 Sesión cerrada para:", ticker)


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))  # Railway asigna un puerto automáticamente
    print(f"🚀 Iniciando servidor Flask en puerto {port}...")
    app.run(host='0.0.0.0', port=port)
