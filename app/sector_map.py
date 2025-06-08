# sector_map.py

# Diccionario de mapeo del sector detectado por FMP a etiquetas técnicas
SECTOR_TAG_MAP = {
    "Technology": "[Tech]",
    "Healthcare": "[Healthcare]",
    "Communication Services": "[Tech]",
    "Consumer Cyclical": "[Consumer]",
    "Consumer Defensive": "[Consumer]",
    "Industrials": "[Industrials]",
    "Basic Materials": "[Materials]",
    "Energy": "[Energy]",
    "Utilities": "[Utilities]",
    "Financial Services": "[Banca]",
    "Insurance": "[Seguros]",
    "Real Estate": "[REIT]",
    "Unknown": "[Otros]"
}

def get_sector_tag(sector: str) -> str:
    """
    Recibe el nombre de un sector desde FMP y devuelve su etiqueta técnica
    para activación de subpartidas contables.
    """
    return SECTOR_TAG_MAP.get(sector.strip(), "[Otros]")
