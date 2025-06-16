
import sqlite3
import json

DB_PATH = r"E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db"

def reformatear_tabla_origen(nombre_tabla, campo_anio):
    print(f"🔁 Reformateando tabla: {nombre_tabla}...")

    tabla_destino = f"{nombre_tabla}_reformada"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Eliminar si ya existía (opcional: para pruebas)
    cur.execute(f"DROP TABLE IF EXISTS {tabla_destino}")

    # Crear nueva tabla
    cur.execute(f"""
        CREATE TABLE {tabla_destino} (
            ticker TEXT,
            anio TEXT,
            raw_json TEXT
        )
    """)

    # Leer datos originales
    cur.execute(f"SELECT ticker, json FROM {nombre_tabla}")
    rows = cur.fetchall()

    insertados = 0
    fallidos = 0

    for ticker, json_lista in rows:
        try:
            registros = json.loads(json_lista)
            for entry in registros:
                anio = entry.get(campo_anio) or entry.get("date", "")[:4]
                if not anio:
                    fallidos += 1
                    continue

                fila = (
                    entry.get("symbol", ticker),
                    anio,
                    json.dumps(entry)
                )
                cur.execute(f"INSERT INTO {tabla_destino} (ticker, anio, raw_json) VALUES (%s, %s, %s)", fila)
                insertados += 1

        except Exception as e:
            print(f"⚠️ Error en ticker {ticker}: {e}")
            fallidos += 1

    conn.commit()
    conn.close()
    print(f"✅ Completado: {insertados} registros insertados en {tabla_destino}. Fallidos: {fallidos}\n")

if __name__ == "__main__":
    reformatear_tabla_origen("key_metrics", "calendarYear")
    reformatear_tabla_origen("analyst_estimates", "calendarYear")  # fallback al campo 'date' si no hay calendarYear
