import pandas as pd
from .utils_estandarizacion import estandarizar_ticker_anio, limpiar_valores_numericos

def normalize_key_metrics(json_list):
    def extraer_registro(data):
        try:
            return {
                "ticker": data.get("symbol"),
                "anio": data.get("calendarYear"),
                "revenuePerShare": data.get("revenuePerShare"),
                "netIncomePerShare": data.get("netIncomePerShare"),
                "operatingCashFlowPerShare": data.get("operatingCashFlowPerShare"),
                "freeCashFlowPerShare": data.get("freeCashFlowPerShare"),
                "cashPerShare": data.get("cashPerShare"),
                "bookValuePerShare": data.get("bookValuePerShare"),
                "tangibleBookValuePerShare": data.get("tangibleBookValuePerShare"),
                "shareholdersEquityPerShare": data.get("shareholdersEquityPerShare"),
                "interestDebtPerShare": data.get("interestDebtPerShare"),
                "marketCap": data.get("marketCap"),
                "enterpriseValue": data.get("enterpriseValue"),
                "peRatio": data.get("peRatio"),
                "priceToSalesRatio": data.get("priceToSalesRatio"),
                "pocfratio": data.get("pocfratio"),
                "pfcfRatio": data.get("pfcfRatio"),
                "pbRatio": data.get("pbRatio"),
                "ptbRatio": data.get("ptbRatio"),
                "evToSales": data.get("evToSales"),
                "enterpriseValueOverEBITDA": data.get("enterpriseValueOverEBITDA"),
                "evToOperatingCashFlow": data.get("evToOperatingCashFlow"),
                "evToFreeCashFlow": data.get("evToFreeCashFlow"),
                "earningsYield": data.get("earningsYield"),
                "freeCashFlowYield": data.get("freeCashFlowYield"),
                "debtToEquity": data.get("debtToEquity"),
                "debtToAssets": data.get("debtToAssets"),
                "netDebtToEBITDA": data.get("netDebtToEBITDA"),
                "currentRatio": data.get("currentRatio"),
                "interestCoverage": data.get("interestCoverage"),
                "incomeQuality": data.get("incomeQuality"),
                "dividendYield": data.get("dividendYield"),
                "payoutRatio": data.get("payoutRatio"),
                "capexPerShare": data.get("capexPerShare")
            }
        except Exception:
            return None

    # 1. Construir DataFrame plano a partir de registros válidos
    registros = [extraer_registro(d) for d in json_list if extraer_registro(d)]
    df = pd.DataFrame(registros)

    # 2. Aplicar estandarización
    df = estandarizar_ticker_anio(df, tipo="calendar")

    # 3. Limpieza numérica para legibilidad
    df = limpiar_valores_numericos(df)

    return df
