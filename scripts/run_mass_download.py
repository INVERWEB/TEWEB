#Ejecutatodo elflujo, verifica si el ticker ya fue consultado
# , guarda archivos JSON

from pathlib import Path
from utils.fetch_statements import get_all_statements
from utils.fetch_tickers import get_tickers_by_sector
from utils.fetch_profile import obtener_datos_perfil
from utils.db_utils import (
    insertar_json_generico,
    registrar_ticker_consultado,
    crear_tablas_si_faltan,
    ya_existe_ticker
)

# === CONFIGURACIÓN ===
SECTOR = "Technology"
MAX_TICKERS = 200
LOG_PATH = Path("E:/@VALUECONOMICS/datos analisis/extraccion_testFMPremium/log_descarga.txt")

# === FUNCIONES DE LOG ===
def log_resultado(texto):
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(texto + "\n")
    print(texto)

# === FLUJO PRINCIPAL ===
def main():
    crear_tablas_si_faltan()

    tickers_api = get_tickers_by_sector(SECTOR, limit=10000)
    tickers_a_procesar = [t for t in tickers_api if not ya_existe_ticker(t)][:MAX_TICKERS]

    if not tickers_a_procesar:
        log_resultado("⚠️ No hay nuevos tickers para procesar.")
        return

    for i, ticker in enumerate(tickers_a_procesar, start=1):
        log_resultado(f"\n▶️ ({i}) Procesando: {ticker}")

        perfil = obtener_datos_perfil(ticker)
        if not perfil:
            log_resultado(f"❌ No se pudo obtener perfil para {ticker}")
            continue

        registrar_ticker_consultado(
            ticker=perfil["ticker"],
            nombre_empresa=perfil["nombre_empresa"],
            sector=perfil["sector"],
            industria=perfil["industria"],
            market_cap=perfil["market_cap"],
            enterprise_value=perfil["enterprise_value"]
        )

        resultados = get_all_statements(ticker)
        if not resultados:
            log_resultado(f"❌ No se obtuvo data financiera para {ticker}")
            continue

        for tipo, contenido in resultados.items():
            if isinstance(content := contenido, list):
                for entry in content:
                    insertar_json_generico(tipo, ticker, entry)
            else:
                insertar_json_generico(tipo, ticker, content)

if __name__ == "__main__":
    main()
