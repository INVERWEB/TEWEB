# TEWEB – API Flask Financiera

API mínima para servir estados financieros desde una base SQLite (`fmp_datafree.db`) para uso con Google Sheets o despliegue en Railway.

## Estructura

- `app.py`: API principal con Flask
- `config.py`: Ruta fija al archivo `.db`
- `fmp_datafree.db`: Base de datos (debe colocarse manualmente en la raíz)
- `.gitignore`: Para ignorar archivos innecesarios

## Instrucciones

1. Colocá `fmp_datafree.db` en esta carpeta.
2. Ejecutá local con:

```
python app.py
```

3. Subí a Railway y abrí `https://tu-app.up.railway.app/api/income/TSM`
