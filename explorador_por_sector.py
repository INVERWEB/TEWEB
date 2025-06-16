import sqlite3
from pathlib import Path
from explorar_db import buscar_metadata, procesar_comando, mostrar_menu

DB_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db")

SECTOR_ALIAS = {
    "TECH": "Technology",
    "CYCLICAL": "Consumer Cyclical",
    "DEF": "Consumer Defensive",
    "FIN": "Financial Services",
    "HLTH": "Healthcare",
    "IND": "Industrials",
    "REAL": "Real Estate",
    "UTIL": "Utilities",
    "COMM": "Communication Services",
    "ENER": "Energy",
    "MTRL": "Basic Materials"
}

INDUSTRIA_COMANDO = {}
TICKER_COMANDO = {}

def conectar():
    return sqlite3.connect(DB_PATH)

def listar_sectores():
    print("\n🌐 Sectores disponibles:")
    with conectar() as conn:
        cur = conn.cursor()
        for alias, nombre in SECTOR_ALIAS.items():
            cur.execute("SELECT COUNT(*) FROM tickers_consultados WHERE sector = %s", (nombre,))
            cantidad = cur.fetchone()[0]
            print(f"<{nombre}>  {{{alias}}} = {cantidad} tickers")

def explorar_industrias(alias_sector):
    sector = SECTOR_ALIAS.get(alias_sector.upper())
    if not sector:
        print("⚠️ Sector no reconocido.")
        return None

    print(f"\n🏭 Industrias dentro de <{sector}>:\n")
    INDUSTRIA_COMANDO.clear()
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COALESCE(industria, 'None') AS industria, COUNT(*) 
            FROM tickers_consultados
            WHERE sector = %s
            GROUP BY industria
            ORDER BY COUNT(*) DESC
        """, (sector,))
        industrias = cur.fetchall()

        for i, (nombre, cantidad) in enumerate(industrias, start=1):
            print(f"{i}. {nombre} → {cantidad} tickers")
            INDUSTRIA_COMANDO[str(i)] = nombre

    return sector

def listar_tickers_en_industria(industria_nombre):
    print(f"\n🔎 Tickers en industria '{industria_nombre}':\n")
    TICKER_COMANDO.clear()
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ticker, nombre_empresa
            FROM tickers_consultados
            WHERE industria IS %s OR industria = %s
            ORDER BY nombre_empresa
        """, (None if industria_nombre == "None" else industria_nombre, industria_nombre))
        tickers = cur.fetchall()

        for i, (ticker, nombre) in enumerate(tickers, start=1):
            print(f"{i}. {ticker} - {nombre}")
            TICKER_COMANDO[ticker.upper()] = ticker

def consultar_ticker(ticker):
    buscar_metadata(ticker)
    while True:
        mostrar_menu()
        comando = input("👉 Ingresar comando: ").strip().lower()
        if comando == "salir":
            break
        procesar_comando(ticker, comando)

def menu_explorador():
    while True:
        listar_sectores()
        print("\n✳️ Escribe el alias del sector a explorar (ej. TECH, FIN, HLTH) o SALIR para terminar")
        sector_cmd = input("⤷ Sector: ").strip().upper()
        if sector_cmd == "SALIR":
            break

        sector = explorar_industrias(sector_cmd)
        if not sector:
            continue

        print("\n✳️ Escribe el número de la industria para ver sus tickers")
        industria_num = input("⤷ Industria #: ").strip()
        if industria_num not in INDUSTRIA_COMANDO:
            print("⚠️ Número inválido")
            continue

        industria = INDUSTRIA_COMANDO[industria_num]
        listar_tickers_en_industria(industria)

        print("\n✳️ Escribe el TICKER para explorarlo a fondo (o escribe VOLVER para regresar)")
        ticker = input("⤷ Ticker: ").strip().upper()
        if ticker == "VOLVER":
            continue
        if ticker in TICKER_COMANDO:
            consultar_ticker(ticker)
        else:
            print("⚠️ Ticker no reconocido")

if __name__ == "__main__":
    menu_explorador()
