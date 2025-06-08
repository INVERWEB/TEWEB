from .normalize_json import normalizar_comun

def normalize_cash_flow(json_raw):
    CAMPOS = ['netIncome', 'depreciationAndAmortization', 'stockBasedCompensation', 'changeInWorkingCapital', 'accountsReceivables', 'accountsPayables', 'inventory', 'otherWorkingCapital', 'otherNonCashItems', 'deferredIncomeTax', 'netCashProvidedByOperatingActivities', 'operatingCashFlow', 'investmentsInPropertyPlantAndEquipment', 'capitalExpenditure', 'acquisitionsNet', 'purchasesOfInvestments', 'salesMaturitiesOfInvestments', 'otherInvestingActivites', 'netCashUsedForInvestingActivites', 'commonStockIssued', 'commonStockRepurchased', 'dividendsPaid', 'debtRepayment', 'otherFinancingActivites', 'netCashUsedProvidedByFinancingActivities', 'netChangeInCash', 'effectOfForexChangesOnCash', 'cashAtBeginningOfPeriod', 'cashAtEndOfPeriod', 'freeCashFlow']
    return normalizar_comun(json_raw, CAMPOS)
