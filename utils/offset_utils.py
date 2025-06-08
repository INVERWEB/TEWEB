
import json
import os

OFFSET_FILE = "offset_sector.json"

def cargar_offset():
    if not os.path.exists(OFFSET_FILE):
        return {}
    with open(OFFSET_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_offset(offset_data):
    with open(OFFSET_FILE, "w", encoding="utf-8") as f:
        json.dump(offset_data, f, indent=2)

def obtener_offset_actual():
    offset = cargar_offset()
    return offset.get("ALL", 0)

def actualizar_offset(nuevo_valor):
    offset = cargar_offset()
    offset["ALL"] = nuevo_valor
    guardar_offset(offset)
    print(f"💾 Offset actualizado: ALL = {nuevo_valor}")
