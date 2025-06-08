import json
from pathlib import Path

OFFSET_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/offset_sector.json")

SECTORES = [
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities"
]

def cargar_offset():
    if OFFSET_PATH.exists():
        return json.loads(OFFSET_PATH.read_text(encoding="utf-8"))
    return {}

def mostrar_offset_actual():
    offset_data = cargar_offset()
    print("\n📊 Estado actual de offset por sector:\n")
    for sector in SECTORES:
        actual = offset_data.get(sector, 0)
        print(f"• {sector:<25} → {actual:>5} tickers procesados")

if __name__ == "__main__":
    mostrar_offset_actual()
