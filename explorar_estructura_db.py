import sqlite3
import os

DB_PATH = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db"

def explorar_estructura(db_path):
    print("🔍 Explorando base de datos:", db_path)
    if not os.path.exists(db_path):
        print("❌ ERROR: La base de datos no fue encontrada en la ruta indicada.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Obtenemos todas las tablas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = [row[0] for row in cursor.fetchall()]
    print(f"✅ {len(tablas)} tablas encontradas en la base.")

    # Clasificamos tablas
    tablas_plana = [t for t in tablas if t.endswith("_plana")]
    tablas_crudo = [t for t in tablas if not t.endswith("_plana") and not t.startswith("sqlite_")]

    print("\n📄 Tablas en formato plano (_plana):", len(tablas_plana))
    for t in tablas_plana:
        print(f"   └─ {t}")

    print("\n🗃️ Tablas en crudo (posiblemente JSON):", len(tablas_crudo))
    for t in tablas_crudo:
        print(f"   └─ {t}")

    print("\n🔧 Recomendación:")
    print("   - Aplicar limpieza (opción A) sobre las tablas _plana.")
    print("   - Usar los nombres de las tablas crudas para crear nuevas _plana si faltan.")

    conn.close()

if __name__ == "__main__":
    explorar_estructura(DB_PATH)
