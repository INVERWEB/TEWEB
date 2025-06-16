from .normalize_json import normalizar_comun
from .utils_estandarizacion import estandarizar_ticker_anio, limpiar_valores_numericos

def normalize_balance_sheet(json_raw):
    CAMPOS = [
        'cashAndCashEquivalents', 'cashAndShortTermInvestments', 'shortTermInvestments',
        'netReceivables', 'inventory', 'otherCurrentAssets', 'totalCurrentAssets',
        'propertyPlantEquipmentNet', 'goodwillAndIntangibleAssets', 'goodwill', 'intangibleAssets',
        'otherAssets', 'totalNonCurrentAssets', 'totalAssets', 'accountPayables',
        'shortTermDebt', 'deferredRevenue', 'otherCurrentLiabilities', 'totalCurrentLiabilities',
        'longTermDebt', 'capitalLeaseObligations', 'deferredRevenueNonCurrent',
        'deferredTaxLiabilitiesNonCurrent', 'otherLiabilities', 'totalNonCurrentLiabilities',
        'totalLiabilities', 'commonStock', 'preferredStock', 'retainedEarnings',
        'minorityInterest', 'totalEquity', 'totalStockholdersEquity', 'totalLiabilitiesAndTotalEquity',
        'netDebt', 'totalDebt', 'totalInvestments', 'taxAssets'
    ]

    # 1. Normalizar campos relevantes
    df = normalizar_comun(json_raw, CAMPOS)

    # 2. Estandarizar ticker/anio (usa calendarYear como base)
    df = estandarizar_ticker_anio(df, tipo='calendar')

    # 3. Limpiar valores numéricos con máximo 2 decimales
    df = limpiar_valores_numericos(df)

    return df

