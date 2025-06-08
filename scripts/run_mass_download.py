
import json
from pathlib import Path
from time import sleep
from utils.fetch_statements import get_all_statements
from utils.fetch_profile import obtener_datos_perfil
from utils.fetch_tickers import get_tickers_by_exchange_list
from utils.offset_utils import obtener_offset_actual, actualizar_offset
from utils.db_utils import (
    insertar_json_generico,
    registrar_ticker_consultado,
    crear_tablas_si_faltan,
    ya_existe_ticker
)

# === CONFIGURACIÓN ===
MAX_TICKERS = 1000
DB_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db")
LOG_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/log_resultados.txt")
TICKERS_OUT_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/tickers_validos.txt")

# === FUNCIONES DE UTILIDAD ===
def log_resultado(texto):
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(texto + "\n")
    print(texto)

# === FLUJO PRINCIPAL ===
def main():
    crear_tablas_si_faltan(DB_PATH)

    offset_actual = obtener_offset_actual()

    tickers_api = get_tickers_by_exchange_list()
    tickers_api = [t for t in tickers_api if t and t[0].isalpha()]  # Solo tickers que empiezan con letra
    tickers_pendientes = tickers_api[offset_actual:]
    tickers_a_procesar = [t for t in tickers_pendientes if not ya_existe_ticker(t, DB_PATH)][:MAX_TICKERS]

    try:
        TICKERS_OUT_PATH.write_text("\n".join(tickers_a_procesar), encoding="utf-8")
        log_resultado(f"📄 tickers_validos.txt guardado con {len(tickers_a_procesar)} tickers válidos.")
    except Exception as e:
        log_resultado(f"⚠️ No se pudo guardar tickers_validos.txt: {e}")

    if not tickers_a_procesar:
        log_resultado("⚠️ No hay nuevos tickers para procesar.")
        return

    total_procesados = 0
    total_omitidos = 0

    for i, ticker in enumerate(tickers_a_procesar, start=1):
        log_resultado(f"\n▶️ ({offset_actual + i}) Procesando: {ticker}")

        perfil = obtener_datos_perfil(ticker)
        if not perfil or not perfil.get("market_cap"):
            log_resultado(f"❌ No se pudo obtener perfil válido para {ticker}, omitido.")
            total_omitidos += 1
            continue

        resultados = get_all_statements(ticker)
        if not resultados:
            log_resultado(f"❌ No se obtuvo data financiera para {ticker}")
            total_omitidos += 1
            continue

        registrar_ticker_consultado(
            ticker=perfil["ticker"],
            nombre_empresa=perfil["nombre_empresa"],
            sector=perfil["sector"],
            industria=perfil["industria"],
            market_cap=perfil["market_cap"],
            enterprise_value=perfil["enterprise_value"],
            db_path=DB_PATH
        )

        for tipo, contenido in resultados.items():
            if isinstance(content := contenido, list):
                for entry in content:
                    insertar_json_generico(tipo, ticker, entry, DB_PATH)
            else:
                insertar_json_generico(tipo, ticker, content, DB_PATH)

        total_procesados += 1
        sleep(0.25)

    nuevo_offset = offset_actual + len(tickers_a_procesar)
    actualizar_offset(nuevo_offset)
    total_intentos = len(tickers_a_procesar)
    # === RESUMEN FINAL ===

    resumen = [
        "\n==============================",
        f"✅ {total_procesados} tickers procesados correctamente",
        f"❌ {total_omitidos} tickers omitidos por errores o falta de datos",
        f"🎯 Total intentos: {total_intentos}",


        "=============================="
    ]
    for linea in resumen:
        print(linea)
        log_resultado(linea)

if __name__ == "__main__":
    main()
