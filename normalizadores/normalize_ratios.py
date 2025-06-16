from .normalize_json import normalizar_comun
from .utils_estandarizacion import estandarizar_ticker_anio, limpiar_valores_numericos

def normalize_ratios(json_raw):
    CAMPOS = [
        'currentRatio', 'quickRatio', 'cashRatio', 'daysOfSalesOutstanding',
        'daysOfInventoryOutstanding', 'operatingCycle', 'daysOfPayablesOutstanding',
        'cashConversionCycle', 'grossProfitMargin', 'operatingProfitMargin',
        'pretaxProfitMargin', 'netProfitMargin', 'effectiveTaxRate', 'returnOnAssets',
        'returnOnEquity', 'returnOnCapitalEmployed', 'netIncomePerEBT', 'ebtPerEbit',
        'ebitPerRevenue', 'debtRatio', 'debtEquityRatio', 'longTermDebtToCapitalization',
        'totalDebtToCapitalization', 'interestCoverage', 'cashFlowToDebtRatio',
        'companyEquityMultiplier', 'receivablesTurnover', 'payablesTurnover',
        'inventoryTurnover', 'fixedAssetTurnover', 'assetTurnover',
        'operatingCashFlowPerShare', 'freeCashFlowPerShare', 'cashPerShare',
        'payoutRatio', 'operatingCashFlowSalesRatio', 'freeCashFlowOperatingCashFlowRatio',
        'cashFlowCoverageRatios', 'shortTermCoverageRatios', 'capitalExpenditureCoverageRatio',
        'dividendPaidAndCapexCoverageRatio', 'dividendPayoutRatio', 'priceBookValueRatio',
        'priceToBookRatio', 'priceToSalesRatio', 'priceEarningsRatio',
        'priceToFreeCashFlowsRatio', 'priceToOperatingCashFlowsRatio', 'priceCashFlowRatio',
        'priceEarningsToGrowthRatio', 'priceSalesRatio', 'dividendYield',
        'enterpriseValueMultiple', 'priceFairValue'
    ]

    # 1. Normalizar campos clave
    df = normalizar_comun(json_raw, CAMPOS)

    # 2. Establecer ticker y anio (usa calendarYear)
    df = estandarizar_ticker_anio(df, tipo='calendar')

    # 3. Limpiar valores numéricos
    df = limpiar_valores_numericos(df)

    return df
