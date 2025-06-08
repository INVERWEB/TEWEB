from .normalize_json import normalizar_comun

def normalize_income_statement(json_raw):
    CAMPOS = ['revenue', 'costOfRevenue', 'grossProfit', 'operatingExpenses', 'sellingGeneralAndAdministrativeExpenses', 'depreciationAndAmortization', 'otherExpenses', 'researchAndDevelopmentExpenses', 'operatingIncome', 'totalOtherIncomeExpensesNet', 'interestIncome', 'interestExpense', 'incomeBeforeTax', 'netIncome', 'ebitda', 'weightedAverageShsOutDil', 'eps']
    return normalizar_comun(json_raw, CAMPOS)
