from .normalize_json import normalizar_comun

def normalize_analyst_estimates(json_raw):
    CAMPOS = ['estimatedRevenueLow', 'estimatedRevenueHigh', 'estimatedRevenueAvg', 'estimatedEbitdaLow', 'estimatedEbitdaHigh', 'estimatedEbitdaAvg', 'estimatedEbitLow', 'estimatedEbitHigh', 'estimatedEbitAvg', 'estimatedNetIncomeLow', 'estimatedNetIncomeHigh', 'estimatedNetIncomeAvg', 'estimatedSgaExpenseLow', 'estimatedSgaExpenseHigh', 'estimatedSgaExpenseAvg', 'estimatedEpsAvg', 'estimatedEpsHigh', 'estimatedEpsLow', 'numberAnalystEstimatedRevenue', 'numberAnalystsEstimatedEps']
    return normalizar_comun(json_raw, CAMPOS)

