
import requests


def get_sector_from_fmp(ticker: str, api_key: str = "CTKkvKS7dg9MTxwrpbx5WjJv8uVeHfnb") -> str:
    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={api_key}"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Error al acceder a FMP")

    data = response.json()

    if isinstance(data, list) and len(data) > 0:
        return data[0].get("sector", "Sector no encontrado")
    else:
        return "Sector no encontrado"

def map_sector_to_tag(sector: str) -> str:
    sector_tag_map = {
        "Technology": "[Tech]",
        "Communication Services": "[Tech]",
        "Healthcare": "[Healthcare]",
        "Industrials": "[Industrials]",
        "Consumer Cyclical": "[Consumer]",
        "Consumer Defensive": "[Consumer]",
        "Financial Services": "[Banca]",
        "Insurance": "[Seguros]",
        "Energy": "[Energy]",
        "Basic Materials": "[Materials]",
        "Utilities": "[Utilities]"
    }
    return sector_tag_map.get(sector, "[Otros]")


def get_revenue_data(ticker: str):
    url = f"https://www.macrotrends.net/stocks/charts/{ticker}/apple/revenue"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Error al acceder a Macrotrends: {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="historical_data_table table")

    if not table:
        raise Exception("No se encontró la tabla de ingresos anuales.")

    years = []
    revenues = []

    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) >= 2:
            try:
                year = int(cols[0].text.strip())
                revenue = float(cols[1].text.strip().replace("$", "").replace(",", ""))
                years.append(year)
                revenues.append(revenue)
            except ValueError:
                continue

    # Tomar máximo los 10 años más recientes
    years = years[:10]
    revenues = revenues[:10]

    # Invertir (para ir del más antiguo al más reciente)
    years = years[::-1]
    revenues = revenues[::-1]

    # Alinear de derecha a izquierda (rellenar a la izquierda con vacío si hay menos de 10)
    padding = 10 - len(years)
    years = [""] * padding + years
    revenues = [""] * padding + revenues

    return {"years": years, "revenues": revenues}



def get_gross_profit_data_resilient(ticker: str) -> dict[str, list]:
    def try_vertical_page(ticker: str):
        url = f"https://www.macrotrends.net/stocks/charts/{ticker.upper()}/gross-profit"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table", class_="historical_data_table table")

        data = []
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    year = cols[0].text.strip()
                    value = cols[1].text.strip().replace(",", "").replace("$", "")
                    if year.isdigit() and re.match(r"^\d+(\.\d+)?$", value):
                        data.append((int(year), float(value)))
        if not data:
            raise ValueError("No data in vertical page")
        return data

    def try_income_statement_fallback(ticker: str):
        url = f"https://www.macrotrends.net/stocks/charts/{ticker.upper()}/income-statement"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("tr")

        for row in rows:
            cols = row.find_all("td")
            if not cols:
                continue
            label = cols[0].text.strip().lower()
            if "gross profit" in label:
                values = []
                for col in cols[1:]:
                    text = col.text.strip().replace(",", "").replace("$", "")
                    try:
                        values.append(float(text))
                    except ValueError:
                        values.append("")
                return list(enumerate(values[::-1], start=2024 - len(values) + 1))[-10:]

        raise ValueError("Gross Profit not found in fallback")

    try:
        data = try_vertical_page(ticker)
        source = "vertical"
    except Exception:
        try:
            data = try_income_statement_fallback(ticker)
            source = "horizontal"
        except Exception:
            return {"years": list(range(2015, 2025)), "gross_profit": [""] * 10, "source": "none"}

    data.sort(key=lambda x: x[0])
    data = data[-10:]

    aligned_years = [""] * (10 - len(data)) + [str(y) for y, _ in data]
    aligned_values = [""] * (10 - len(data)) + [v for _, v in data]

    return {
        "years": aligned_years,
        "gross_profit": aligned_values,
        "source": source
    }



from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options
from bs4 import BeautifulSoup
import os
import re
import time

def get_gross_profit_data(ticker: str) -> dict:
    url = f"https://www.macrotrends.net/stocks/charts/{ticker.lower()}/gross-profit"

    driver_path = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/msedgedriver.exe"
    options = Options()
    options.use_chromium = True
    options.add_argument("--headless=new")

    service = EdgeService(driver_path)
    driver = webdriver.Edge(service=service, options=options)

    driver.get(url)
    time.sleep(7)  # Espera que cargue el JS

    soup = BeautifulSoup(driver.page_source, "html.parser")
    with open("debug_gross_profit.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)

    driver.quit()

    table = soup.find("table", class_="historical_data_table table")
    if not table:
        raise Exception("❌ No se encontró la tabla de Gross Profit en la página.")

    rows = table.find_all("tr")[1:]
    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            year = cols[0].text.strip()
            value = cols[1].text.strip().replace(",", "").replace("$", "")
            if year.isdigit() and re.match(r"^\d+(\.\d+)?$", value):
                data.append((int(year), float(value)))

    if not data:
        raise Exception(f"No se pudo extraer Gross Profit para {ticker}")

    data.sort(key=lambda x: x[0])
    data = data[-10:]

    aligned_years = [""] * (10 - len(data)) + [str(y) for y, _ in data]
    aligned_gp = [""] * (10 - len(data)) + [v for _, v in data]

    return {
        "years": aligned_years,
        "gross_profit": aligned_gp,
        "source": "edge_selenium"
    }

from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService

from bs4 import BeautifulSoup
import time
import re






def get_gross_profit_data(ticker: str) -> dict:
    import time
    import re
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    chrome_driver_path = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/chromedriver.exe"
    chrome_binary_path = "C:/Program Files/Google/Chrome/Application/chrome.exe"

    options = Options()
    options.binary_location = chrome_binary_path
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    service = ChromeService(executable_path=chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    try:
        url = f"https://www.macrotrends.net/stocks/charts/{ticker.upper()}/income-statement"
        driver.get(url)

        # Esperar a que la tabla cargue
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "contenttablejqxgrid"))
        )

        # Buscar todas las filas
        rows = driver.find_elements(By.CSS_SELECTOR, "div[id^='row']")

        gross_profit_row = None
        for row in rows:
            if "Gross Profit" in row.text:
                gross_profit_row = row
                break

        if gross_profit_row is None:
            raise Exception("❌ 'Gross Profit' no encontrado en el DOM.")

        # Extraer las celdas de esa fila
        cells = gross_profit_row.find_elements(By.CSS_SELECTOR, "div.jqx-grid-cell")

        # Tomar solo los últimos 10 valores válidos (de derecha a izquierda)
        values = []
        for cell in cells[1:]:  # [0] es el nombre "Gross Profit"
            text = cell.text.strip().replace(",", "").replace("$", "")
            try:
                values.append(float(text))
            except:
                values.append("")

        values = values[::-1][:10]  # Reverso y últimos 10 años
        values = values[::-1]       # Regresamos al orden cronológico

        # Rellenar si hay menos de 10
        missing = 10 - len(values)
        aligned_values = [""] * missing + values
        aligned_years = [""] * missing + [str(y) for y in range(2025 - len(values), 2025)]

        return {
            "years": aligned_years,
            "gross_profit": aligned_values,
            "source": "selenium_income_statement"
        }

    except Exception as e:
        raise Exception(f"❌ Error en Selenium para {ticker}: {str(e)}")

    finally:
        driver.quit()

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def get_gross_profit_data(ticker: str) -> dict:
    print("🔍 Intentando cargar la página vertical de Gross Profit...")
    chrome_binary_path = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/chrome.exe"
    chrome_driver_path = "E:/@VALUECONOMICS/PROYECT DEL PROGRAMA/INVERSORWEB/chromedriver.exe"

    options = Options()
    options.binary_location = chrome_binary_path
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = ChromeService(executable_path=chrome_driver_path)

    try:
        driver = webdriver.Chrome(service=service, options=options)
        url = f"https://www.macrotrends.net/stocks/charts/{ticker.upper()}/gross-profit"
        driver.get(url)

        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        print("✅ Página cargada. Buscando tabla de Gross Profit...")

        tables = driver.find_elements(By.TAG_NAME, "table")
        for table in tables:
            if "Gross Profit" in table.text:
                rows = table.find_elements(By.TAG_NAME, "tr")
                data = []
                for row in rows[1:]:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) >= 2:
                        year = cols[0].text.strip()
                        value = cols[1].text.strip().replace(",", "").replace("$", "")
                        if year.isdigit() and value.replace(".", "", 1).isdigit():
                            data.append((int(year), float(value)))
                if data:
                    data.sort(key=lambda x: x[0])
                    data = data[-10:]
                    aligned_years = [""] * (10 - len(data)) + [str(y) for y, _ in data]
                    aligned_values = [""] * (10 - len(data)) + [v for _, v in data]
                    driver.quit()
                    return {
                        "years": aligned_years,
                        "gross_profit": aligned_values,
                        "source": "vertical"
                    }
        print("🔍 ❌ No se encontró tabla con Gross Profit visible en vertical.")
        driver.quit()

    except Exception as e:
        print(f"⚠️ Selenium falló en vertical: {e}")

    print("🔁 Intentando fallback en income-statement...")
    try:
        driver = webdriver.Chrome(service=service, options=options)
        url = f"https://www.macrotrends.net/stocks/charts/{ticker.upper()}/income-statement"
        driver.get(url)
        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        tables = driver.find_elements(By.TAG_NAME, "table")
        for table in tables:
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if not cols:
                    continue
                label = cols[0].text.strip().lower()
                if "gross profit" in label:
                    values = []
                    for col in cols[1:]:
                        text = col.text.strip().replace(",", "").replace("$", "")
                        try:
                            values.append(float(text))
                        except ValueError:
                            values.append("")
                    values = values[::-1]
                    start_year = 2024 - len(values) + 1
                    data = list(zip(range(start_year, 2025), values))[-10:]
                    aligned_years = [""] * (10 - len(data)) + [str(y) for y, _ in data]
                    aligned_values = [""] * (10 - len(data)) + [v for _, v in data]
                    driver.quit()
                    return {
                        "years": aligned_years,
                        "gross_profit": aligned_values,
                        "source": "horizontal"
                    }
        driver.quit()
        print("🔍 ❌ Gross Profit no encontrado en fallback horizontal (HTML)")
        return {
            "years": list(range(2015, 2025)),
            "gross_profit": [""] * 10,
            "source": "none"
        }

    except Exception as e:
        print(f"❌ Fallback Selenium income-statement falló: {e}")
        return {
            "years": list(range(2015, 2025)),
            "gross_profit": [""] * 10,
            "source": "none"
        }










