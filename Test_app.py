from flask import Flask, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

# Configuración de la conexión a la base de datos
DATABASE_URL = "postgresql://usuario:contraseña@localhost:5432/nombre_de_la_base_de_datos?client_encoding=utf8"
engine = create_engine(DATABASE_URL)

# Crear una sesión local
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@app.route('/test_db_connection', methods=['GET'])
def test_db_connection():
    session = SessionLocal()
    try:
        # Ejecuta una consulta simple para verificar la conexión
        result = session.execute(text("SELECT 1;"))
        session.commit()
        return jsonify({"message": "Conexión exitosa a la base de datos"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True)
