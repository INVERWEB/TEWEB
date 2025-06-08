from .normalize_json import normalizar_comun

def normalize_enterprise_values(json_raw):
    CAMPOS = ['marketCapitalization', 'addTotalDebt', 'minusCashAndCashEquivalents', 'numberOfShares', 'enterpriseValue']
    return normalizar_comun(json_raw, CAMPOS)
