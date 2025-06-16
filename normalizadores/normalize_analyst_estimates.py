from .normalize_json import normalizar_comun
from .utils_estandarizacion import estandarizar_ticker_anio, limpiar_valores_numericos

def normalize_analyst_estimates(json_raw):
    CAMPOS = [
        'estimatedRevenueLow', 'estimatedRevenueHigh', 'estimatedRevenueAvg',
        'estimatedEbitdaLow', 'estimatedEbitdaHigh', 'estimatedEbitdaAvg',
        'estimatedEbitLow', 'estimatedEbitHigh', 'estimatedEbitAvg',
        'estimatedNetIncomeLow', 'estimatedNetIncomeHigh', 'estimatedNetIncomeAvg',
        'estimatedSgaExpenseLow', 'estimatedSgaExpenseHigh', 'estimatedSgaExpenseAvg',
        'estimatedEpsAvg', 'estimatedEpsHigh', 'estimatedEpsLow',
        'numberAnalystEstimatedRevenue', 'numberAnalystsEstimatedEps'
    ]

    # 1. Normalizar campos deseados
    df = normalizar_comun(json_raw, CAMPOS)

    # 2. Estandarizar columnas ticker/anio (usa 'date' para anio en este endpoint)
    df = estandarizar_ticker_anio(df, tipo='date')

    # 3. Limpiar valores numéricos
    df = limpiar_valores_numericos(df)

    return df

