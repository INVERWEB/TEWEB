from .normalize_json import normalizar_comun

def normalize_balance_sheet(json_raw):
    CAMPOS = ['cashAndCashEquivalents', 'cashAndShortTermInvestments', 'shortTermInvestments', 'netReceivables', 'inventory', 'otherCurrentAssets', 'totalCurrentAssets', 'propertyPlantEquipmentNet', 'goodwillAndIntangibleAssets', 'goodwill', 'intangibleAssets', 'otherAssets', 'totalNonCurrentAssets', 'totalAssets', 'accountPayables', 'shortTermDebt', 'deferredRevenue', 'otherCurrentLiabilities', 'totalCurrentLiabilities', 'longTermDebt', 'capitalLeaseObligations', 'deferredRevenueNonCurrent', 'deferredTaxLiabilitiesNonCurrent', 'otherLiabilities', 'totalNonCurrentLiabilities', 'totalLiabilities', 'commonStock', 'preferredStock', 'retainedEarnings', 'minorityInterest', 'totalEquity', 'totalStockholdersEquity', 'totalLiabilitiesAndTotalEquity', 'netDebt', 'totalDebt', 'totalInvestments', 'taxAssets']
    return normalizar_comun(json_raw, CAMPOS)
