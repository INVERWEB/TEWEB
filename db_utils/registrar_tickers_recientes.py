import os

def registrar_ticker_exitoso(ticker, nombre_archivo):
    """
    Registra un ticker exitosamente procesado durante la descarga (uno a uno).
    Si ya existe en el archivo, no lo duplica.

    Parámetros:
    - ticker: string (ej: 'AAPL')
    - nombre_archivo: base del archivo (ej: 'income', 'ratios')
    """
    ruta_txt = f"tickers_recientes_{nombre_archivo}.txt"

    ticker = ticker.strip().upper()
    if not ticker:
        return

    # Crear el archivo si no existe
    if not os.path.exists(ruta_txt):
        with open(ruta_txt, "w") as f:
            f.write(ticker + "\n")
        return

    # Verificar si ya está en el archivo
    with open(ruta_txt, "r") as f:
        existentes = set(line.strip().upper() for line in f if line.strip())

    if ticker not in existentes:
        with open(ruta_txt, "a") as f:
            f.write(ticker + "\n")
