from .normalize_json import normalizar_comun
from .utils_estandarizacion import estandarizar_ticker_anio, limpiar_valores_numericos

def normalize_enterprise_values(json_raw):
    CAMPOS = [
        'marketCapitalization', 'addTotalDebt',
        'minusCashAndCashEquivalents', 'numberOfShares',
        'enterpriseValue'
    ]

    # 1. Normalizar campos clave
    df = normalizar_comun(json_raw, CAMPOS)

    # 2. Estandarizar columnas ticker/anio → usa 'date' como año
    df = estandarizar_ticker_anio(df, tipo='date')

    # 3. Limpieza de valores numéricos (legibilidad y formato correcto)
    df = limpiar_valores_numericos(df)

    return df

