import sqlite3
from pathlib import Path
from collections import Counter

DB_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db")
TABLA = "enterprise_values_plana"

with sqlite3.connect(DB_PATH) as conn:
    cursor = conn.cursor()

    # 1. Recuento por ticker
    print("\n🔍 Tickers con más de una fila:")
    cursor.execute(f"SELECT ticker, COUNT(*) FROM {TABLA} GROUP BY ticker HAVING COUNT(*) > 1")
    for t, c in cursor.fetchall()[:10]:
        print(f" - {t}: {c} registros")

    # 2. Recuento por (ticker, anio)
    print("\n📆 Duplicados exactos por (ticker, anio):")
    cursor.execute(f"SELECT ticker, anio, COUNT(*) FROM {TABLA} GROUP BY ticker, anio HAVING COUNT(*) > 1")
    duplicados = cursor.fetchall()
    for d in duplicados[:10]:
        print(f" - {d[0]} año {d[1]} → {d[2]} repeticiones")
    if len(duplicados) > 10:
        print(f" ...y {len(duplicados)-10} más")

    # 3. Analizar campos sensibles: symbol, date (sin fillingDate)
    print("\n🧪 Variantes únicas encontradas por ticker:")
    cursor.execute(f"SELECT ticker, symbol, date FROM {TABLA}")
    registros = cursor.fetchall()
    analisis = {}
    for t, s, d in registros:
        t = t.upper().strip()
        if t not in analisis:
            analisis[t] = set()
        analisis[t].add((s, d))

    problemas = {k: v for k, v in analisis.items() if len(v) > 1}
    for k, variantes in list(problemas.items())[:5]:
        print(f" - {k}: {len(variantes)} variantes")
        for v in variantes:
            print(f"    symbol={v[0]}, date={v[1]}")
    if len(problemas) > 5:
        print(f" ...y {len(problemas)-5} tickers más con inconsistencias")

    print("\n✅ Auditoría finalizada")
