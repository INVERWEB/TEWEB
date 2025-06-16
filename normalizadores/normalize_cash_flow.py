from .normalize_json import normalizar_comun
from .utils_estandarizacion import estandarizar_ticker_anio, limpiar_valores_numericos

def normalize_cash_flow(json_raw):
    CAMPOS = [
        'netIncome', 'depreciationAndAmortization', 'stockBasedCompensation',
        'changeInWorkingCapital', 'accountsReceivables', 'accountsPayables',
        'inventory', 'otherWorkingCapital', 'otherNonCashItems', 'deferredIncomeTax',
        'netCashProvidedByOperatingActivities', 'operatingCashFlow',
        'investmentsInPropertyPlantAndEquipment', 'capitalExpenditure',
        'acquisitionsNet', 'purchasesOfInvestments', 'salesMaturitiesOfInvestments',
        'otherInvestingActivites', 'netCashUsedForInvestingActivites',
        'commonStockIssued', 'commonStockRepurchased', 'dividendsPaid',
        'debtRepayment', 'otherFinancingActivites', 'netCashUsedProvidedByFinancingActivities',
        'netChangeInCash', 'effectOfForexChangesOnCash',
        'cashAtBeginningOfPeriod', 'cashAtEndOfPeriod', 'freeCashFlow'
    ]

    # 1. Normalizar los campos relevantes
    df = normalizar_comun(json_raw, CAMPOS)

    # 2. Establecer columnas estándar 'ticker' y 'anio'
    df = estandarizar_ticker_anio(df, tipo='calendar')

    # 3. Redondear valores numéricos para presentación legible
    df = limpiar_valores_numericos(df)

    return df
