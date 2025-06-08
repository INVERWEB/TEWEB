from pathlib import Path
import os

if "RENDER" in os.environ:
    DB_PATH = Path("/tmp/fmp_datafree.db")
else:
    DB_PATH = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/fmp_datafree.db").resolve()


if not DB_PATH.exists():
    print(f"⚠️ Advertencia: No se encontró la base de datos en {DB_PATH}")

