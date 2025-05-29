import openai
import os

# Cargar clave de entorno
openai.api_key = os.getenv("OPENAI_API_KEY")
print("🔐 API KEY GPT:", "CARGADA" if openai.api_key else "NO ENCONTRADA")

def obtener_respuesta_gpt(prompt):
    try:
        print("🧠 Enviando prompt a GPT:", prompt)

        response = openai.ChatCompletion.create(
            model="gpt-4",  # Cambia a "gpt-3.5-turbo" si tienes problemas de acceso
            messages=[
                {"role": "system", "content": "Eres un analista financiero experto."},
                {"role": "user", "content": prompt}
            ]
        )

        resultado = response['choices'][0]['message']['content']
        print("✅ GPT respondió:", resultado)
        return resultado

    except Exception as e:
        print("❌ Error al conectar con GPT:", repr(e))
        return "Error al procesar con GPT"


