from flask import jsonify
from services.chatgpt_service import obtener_respuesta_gpt


def procesar_ticker_gpt(data):
    ticker = data.get('ticker', 'AAPL')
    prompt = f"Resume el negocio de la empresa con ticker {ticker} en 3 líneas."

    try:
        respuesta = obtener_respuesta_gpt(prompt)
        return jsonify({"respuesta": respuesta})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
