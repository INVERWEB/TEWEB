import csv
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ruta_csv = r"E:\@VALUECONOMICS\PROYECT DEL PROGRAMA\TEWEB\ETF3_industria_corregido.csv"

industrias_objetivo = {
    "Multi Utilities",
    "Apparel Accessories & Luxury Goods",
    "Technology Hardware Storage & Peripherals",
    "Textiles Apparel & Luxury Goods",
    "Hotels Resorts & Cruise Lines"
}

try:
    with open(ruta_csv, mode='r', encoding='utf-8') as archivo:
        lector = csv.DictReader(archivo)
        encontradas = 0
        logging.info("🔍 Buscando industrias específicas dentro del CSV...")

        for fila in lector:
            industria_csv = fila.get("industria_google", "").strip()

            if industria_csv in industrias_objetivo:
                encontradas += 1
                logging.info("🟢 INDUSTRIA ENCONTRADA EN CSV:")
                for k, v in fila.items():
                    logging.info(f"   {k.strip()}: {v.strip()}")

        if encontradas == 0:
            logging.warning("⚠️ Ninguna industria de la lista fue encontrada en el CSV.")
        else:
            logging.info(f"✅ Total industrias encontradas en CSV: {encontradas}")

except Exception as e:
    logging.error(f"❌ Error leyendo el CSV: {e}")
