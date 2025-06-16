import json



def extraer_anio(data):
    if "calendarYear" in data:
        return data["calendarYear"]
    if "date" in data and isinstance(data["date"], str):
        return data["date"][:4]
    return None


def normalizar_comun(json_raw, campos_validos):
    """Convierte un JSON crudo en un dict plano con ticker, anio y los campos seleccionados."""
    try:
        if isinstance(json_raw, str):
            data = json.loads(json_raw)
        else:
            data = json_raw

        ticker = data.get("symbol")
        anio = extraer_anio(data)

        if not ticker or not anio:
            return None

        fila = {
            "ticker": ticker,
            "anio": anio
        }

        for campo in campos_validos:
            fila[campo] = data.get(campo)

        return fila

    except Exception as e:
        return None


def normalize_key_metrics(json_raw):
    CAMPOS_KEY_METRICS = [
        "revenuePerShare", "netIncomePerShare", "operatingCashFlowPerShare", "freeCashFlowPerShare",
        "cashPerShare", "bookValuePerShare", "tangibleBookValuePerShare", "shareholdersEquityPerShare",
        "interestDebtPerShare", "marketCap", "enterpriseValue", "peRatio", "priceToSalesRatio",
        "pocfratio", "pfcfRatio", "pbRatio", "ptbRatio", "evToSales", "enterpriseValueOverEBITDA",
        "evToOperatingCashFlow", "evToFreeCashFlow", "earningsYield", "freeCashFlowYield",
        "debtToEquity", "debtToAssets", "netDebtToEBITDA", "currentRatio", "interestCoverage",
        "incomeQuality", "dividendYield", "payoutRatio", "salesGeneralAndAdministrativeToRevenue",
        "researchAndDdevelopementToRevenue", "intangiblesToTotalAssets", "capexToOperatingCashFlow",
        "capexToRevenue", "capexToDepreciation", "stockBasedCompensationToRevenue", "grahamNumber",
        "roic", "returnOnTangibleAssets", "grahamNetNet", "workingCapital", "tangibleAssetValue",
        "netCurrentAssetValue", "investedCapital", "averageReceivables", "averagePayables",
        "averageInventory", "daysSalesOutstanding", "daysPayablesOutstanding", "daysOfInventoryOnHand",
        "receivablesTurnover", "payablesTurnover", "inventoryTurnover", "roe", "capexPerShare"
    ]
    return normalizar_comun(json_raw, CAMPOS_KEY_METRICS)

def normalize_analyst_estimates(json_raw):
    CAMPOS_ANALYST = [
        "estimatedRevenueLow", "estimatedRevenueHigh", "estimatedRevenueAvg",
        "estimatedEbitdaLow", "estimatedEbitdaHigh", "estimatedEbitdaAvg",
        "estimatedEbitLow", "estimatedEbitHigh", "estimatedEbitAvg",
        "estimatedNetIncomeLow", "estimatedNetIncomeHigh", "estimatedNetIncomeAvg",
        "estimatedSgaExpenseLow", "estimatedSgaExpenseHigh", "estimatedSgaExpenseAvg",
        "estimatedEpsAvg", "estimatedEpsHigh", "estimatedEpsLow",
        "numberAnalystEstimatedRevenue", "numberAnalystsEstimatedEps"
    ]
    return normalizar_comun(json_raw, CAMPOS_ANALYST)
