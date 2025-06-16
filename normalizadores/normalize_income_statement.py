from .normalize_json import normalizar_comun
from .utils_estandarizacion import estandarizar_ticker_anio, limpiar_valores_numericos

def normalize_income_statement(json_raw):
    CAMPOS = [
        'revenue', 'costOfRevenue', 'grossProfit', 'operatingExpenses',
        'sellingGeneralAndAdministrativeExpenses', 'depreciationAndAmortization',
        'otherExpenses', 'researchAndDevelopmentExpenses', 'operatingIncome',
        'totalOtherIncomeExpensesNet', 'interestIncome', 'interestExpense',
        'incomeBeforeTax', 'netIncome', 'ebitda', 'weightedAverageShsOutDil', 'eps'
    ]

    def estandarizar_ticker_anio(df, tipo='calendar'):
        """
        Añade columnas 'ticker' y 'anio' estandarizadas según el tipo.
        Soporta symbol, ticker, calendarYear, fiscalYear, date
        """
        # === TICKER ===
        if 'symbol' in df.columns:
            df['ticker'] = df['symbol']
        elif 'ticker' in df.columns:
            df['ticker'] = df['ticker']
        else:
            df['ticker'] = ""

        # === AÑO ===
        if tipo == 'calendar':
            if 'calendarYear' in df.columns:
                df['anio'] = df['calendarYear'].astype(str)
            elif 'fiscalYear' in df.columns:
                df['anio'] = df['fiscalYear'].astype(str)
            elif 'date' in df.columns:
                df['anio'] = df['date'].str[:4]
            else:
                df['anio'] = ""
        elif tipo == 'date':
            df['anio'] = df['date'].str[:4] if 'date' in df.columns else ""

        # Limpiar columnas originales para no duplicar
        df.drop(columns=['symbol', 'calendarYear', 'fiscalYear', 'date', 'period'], errors='ignore', inplace=True)

        # Forzar tipo string
        df['ticker'] = df['ticker'].astype(str)
        df['anio'] = df['anio'].astype(str)

        return df
