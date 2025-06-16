# normalizadores/utils_estandarizacion.py

import pandas as pd

def estandarizar_ticker_anio(df, tipo='calendar'):
    """
    Añade columnas 'ticker' y 'anio' estandarizadas según el tipo.
    """
    df['ticker'] = df['symbol'].astype(str)
    if tipo == 'calendar':
        df['anio'] = df['calendarYear'].astype(str)
    elif tipo == 'date':
        df['anio'] = df['date'].str[:4].astype(str)

    df.drop(columns=['symbol', 'calendarYear', 'date', 'period'], errors='ignore', inplace=True)
    return df

def limpiar_valores_numericos(df):
    """
    Convierte todas las columnas numéricas a formato legible (máx 2 decimales), sin afectar ticker ni anio.
    """
    for col in df.columns:
        if col not in ['ticker', 'anio']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    return df
