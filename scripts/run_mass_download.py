#Ejecutatodo elflujo, verifica si el ticker ya fue consultado
# , guarda archivos JSON

from utils.fetch_tickers import get_tickers_by_sector_and_industry
from utils.fetch_statements import get_all_statements
from utils.db_utils import (
    crear_tablas,
    registrar_ticker,
    insertar_json_generico
)

# Configuración del flujo
SECTOR = "Technology"
INDUSTRY = "Semiconductors"
LIMITE_TICKERS = 100  # Reducido por API gratuita

def main():
    crear_tablas()
    tickers = get_tickers_by_sector_and_industry(SECTOR, INDUSTRY, LIMITE_TICKERS)

    print(f"🔍 Tickers encontrados: {len(tickers)}")
    for i, ticker in enumerate(tickers):
        print(f"\n▶️ ({i+1}/{len(tickers)}) Procesando: {ticker}")
        datos = get_all_statements(ticker)

        if datos:
            registrar_ticker(
                ticker,
                datos.get("companyName", ticker),
                SECTOR,
                INDUSTRY
            )

            for tipo, json_list in datos.items():
                # ✅ Redirigir "ratios" a la tabla "ratios_raw"
                tabla_destino = "ratios_raw" if tipo == "ratios" else tipo

                if isinstance(json_list, list):
                    for entry in json_list:
                        insertar_json_generico(tabla_destino, ticker, entry)
                elif isinstance(json_list, dict):
                    insertar_json_generico(tabla_destino, ticker, json_list)


# redirigimos la tabla para evitar error


if __name__ == "__main__":
    main()
