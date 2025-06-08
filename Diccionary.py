def corregir_tax_expense(diccionario):
    """
    Corrige o inserta la partida 'taxExpense' usando:
    taxExpense = incomeBeforeTax - netIncome
    Solo se aplica si 'taxExpense' está vacío o nulo.
    """
    ebt = diccionario.get("incomeBeforeTax")
    net_income = diccionario.get("netIncome")
    tax = diccionario.get("taxExpense")

    if tax in (None, "", "NA") and ebt is not None and net_income is not None:
        try:
            tax_calculado = ebt - net_income
            diccionario["taxExpense"] = round(tax_calculado, 2)
            print(f"✅ taxExpense corregido para {diccionario.get('date', '')}: {tax_calculado}")
        except Exception as e:
            print(f"⚠️ Error al calcular taxExpense: {e}")
    else:
        print(f"ℹ️ taxExpense ya existe o faltan datos para {diccionario.get('date', '')}.")

    return diccionario


# 🔁 Procesar múltiples años de un mismo ticker (ejemplo con JSON de AAPL)
def procesar_ticker(json_de_empresa):
    Table_Fixed = []

    for fila in json_de_empresa:
        fila_corregida = corregir_tax_expense(fila)
        Table_Fixed.append(fila_corregida)

    return Table_Fixed


# 🧪 Ejemplo de uso con datos simulados
if __name__ == "__main__":
    json_de_empresa = [
        {
            "date": "2023-09-30",
            "symbol": "AAPL",
            "incomeBeforeTax": 111510000000,
            "netIncome": 96995000000,
            "taxExpense": ""
        },
        {
            "date": "2022-09-30",
            "symbol": "AAPL",
            "incomeBeforeTax": 119111000000,
            "netIncome": 99803000000,
            "taxExpense": None
        }
    ]

    resultado = procesar_ticker(json_de_empresa)

    import pprint
    pprint.pprint(resultado)

