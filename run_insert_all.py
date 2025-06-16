
import subprocess
import time
from pathlib import Path

# === CONFIGURACIÓN ===
INSERT_SCRIPTS = [
    "insertpostgre_income.py",
    "insertpostgre_balance.py",
    "insertpostgre_cashflow.py",
    "insertpostgre_ratios.py",
    "insertpostgre_enterprise.py",
    "insertpostgre_key_metrics.py",
    "insertpostgre_analyst.py",
]

SCRIPT_DIR = "db_postgres"
LOG_FILE = Path("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/logs/log_insert_all.txt")

BASE_DIR = Path(__file__).resolve().parent

def log(texto):
    timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]")
    linea = f"{timestamp} {texto}"
    print(linea)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(linea + "\n")

def ejecutar_script(nombre_script):
    ruta = BASE_DIR / SCRIPT_DIR / nombre_script
    log(f"🚀 Iniciando: {nombre_script}")
    inicio = time.time()
    try:
        resultado = subprocess.run(
            ["python", str(ruta)],
            capture_output=True,
            text=True,
            encoding="utf-8",   # ✅ Forzar lectura UTF-8
            errors="ignore"     # ✅ Ignorar errores de codificación
        )
        duracion = time.time() - inicio
        log(f"✅ Completado: {nombre_script} en {round(duracion, 2)} segundos")

        if resultado.stdout and "nada que insertar" in resultado.stdout.lower():
            log(f"⚠️ {nombre_script} no insertó registros nuevos.")

    except subprocess.CalledProcessError as e:
        log(f"❌ ERROR en {nombre_script}")
        log(f"STDOUT:\n{e.stdout}")
        log(f"STDERR:\n{e.stderr}")

def main():
    log("🟢 Lanzador de inserciones iniciado")
    total = len(INSERT_SCRIPTS)
    for i, script in enumerate(INSERT_SCRIPTS, 1):
        log(f"\n📍 Ejecutando ({i}/{total}): {script}")
        ejecutar_script(script)

    log(f"\n✅ Inserción global completada en {round(time.time() / 60, 1)} minutos.")

if __name__ == "__main__":
    main()
