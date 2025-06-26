import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Cargar .env local si existe
load_dotenv("E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/TEWEB/.env")

# Detectar entorno
entorno = os.getenv("ENVIRONMENT", "local").lower()

# Conexión en entorno de producción (Railway)
if entorno == "production":
    db_url = os.getenv("DATABASE_URL")
    if db_url is None:
        raise ValueError("❌ DATABASE_URL no está definida en entorno de producción")
    if db_url.startswith("postgres://"):  # fix compatibilidad SQLAlchemy
        db_url = db_url.replace("postgres://", "postgresql://", 1)

# Conexión en entorno local
else:
    user = os.getenv("PGUSER")
    password = quote_plus(os.getenv("PGPASSWORD"))
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    database = os.getenv("PGDATABASE")
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

# Crear engine y sesión
engine = create_engine(db_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
