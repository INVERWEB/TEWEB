import psycopg2
import logging

# -----------------------
# CONFIGURACIÓN DE LOGS
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -----------------------
# CONFIGURACIÓN MANUAL DE ENTORNO
# -----------------------
modo = "production"  # Cambiar a "local" si aplica

try:
    if modo == "production":
        conn = psycopg2.connect(
            host="nozomi.proxy.rlwy.net",
            database="railway",
            user="postgres",
            password="MYnNWOGEdrSrLhwescMRbjMKQhXCjDrM",
            port="36965"
        )
        logging.info("✅ Conectado a entorno PRODUCTION")
    else:
        conn = psycopg2.connect(
            host="localhost",
            database="inversorweb",
            user="postgres",
            password="Boveda08@reit",
            port="5432"
        )
        logging.info("✅ Conectado a entorno LOCAL")

    cursor = conn.cursor()

    # -----------------------
    # MAPEO INDUSTRIAS FMP → GOOGLE FINANCE
    
    mapeo_industrias = {
        "Advertising Agencies": "Advertising",
    "Aerospace & Defense": "Aerospace & Defense",
    "Agricultural - Commodities/Milling": "Agricultural Products",
    "Agricultural Farm Products": "Agricultural Products",
    "Agricultural Inputs": "Fertilizers & Agricultural Chemicals",
    "Agricultural - Machinery": "Farm & Heavy Construction Machinery",
    "Airlines, Airports & Air Services": "Airlines",
    "Aluminum": "Aluminum",
    "Apparel - Footwear & Accessories": "Apparel, Accessories & Luxury Goods",
    "Apparel - Manufacturers": "Apparel, Accessories & Luxury Goods",
    "Apparel - Retail": "Apparel Retail",
    "Asset Management": "Asset Management",
    "Asset Management - Bonds": "Asset Management",
    "Asset Management - Cryptocurrency": "Asset Management",
    "Asset Management - Global": "Asset Management",
    "Asset Management - Income": "Asset Management",
    "Asset Management - Leveraged": "Asset Management",
    "Auto - Dealerships": "Automotive Retail",
    "Auto - Manufacturers": "Automobiles",
    "Auto - Parts": "Auto Parts & Equipment",
    "Auto - Recreational Vehicles": "Recreational Vehicles & Boats",
    "Banks": "Banks",
    "Banks - Diversified": "Banks",
    "Banks - Regional": "Regional Banks",
    "Beverages - Alcoholic": "Distillers & Vintners",
    "Beverages - Non-Alcoholic": "Soft Drinks",
    "Beverages - Wineries & Distilleries": "Distillers & Vintners",
    "Biotechnology": "Biotechnology",
    "Broadcasting": "Broadcasting",
    "Business Equipment & Supplies": "Office Services & Supplies",
    "Chemicals": "Chemicals",
    "Chemicals - Specialty": "Specialty Chemicals",
    "Coal": "Coal & Consumable Fuels",
    "Communication Equipment": "Communications Equipment",
    "Computer Hardware": "Technology Hardware, Storage & Peripherals",
    "Conglomerates": "Conglomerates",
    "Construction": "Construction & Engineering",
    "Construction Materials": "Construction Materials",
    "Consulting Services": "IT Consulting & Other Services",
    "Consumer Electronics": "Consumer Electronics",
    "Copper": "Copper",
    "Department Stores": "Broadline Retail",
    "Discount Stores": "Broadline Retail",
    "Diversified Utilities": "Multi‑Utilities",
    "Drug Manufacturers - General": "Pharmaceuticals",
    "Drug Manufacturers - Specialty & Generic": "Pharmaceuticals",
    "Education & Training Services": "Education Services",
    "Electrical Equipment & Parts": "Electrical Components & Equipment",
    "Electric Utilities": "Electric Utilities",
    "Electronic Gaming & Multimedia": "Interactive Home Entertainment",
    "Engineering & Construction": "Construction & Engineering",
    "Entertainment": "Movies & Entertainment",
    "Financial - Capital Markets": "Capital Markets",
    "Financial - Conglomerates": "Capital Markets",
    "Financial - Credit Services": "Consumer Finance",
    "Financial - Data & Stock Exchanges": "Capital Markets",
    "Financial - Diversified": "Capital Markets",
    "Financial - Mortgages": "Consumer Finance",
    "Food Confectioners": "Packaged Foods & Meats",
    "Food Distribution": "Food Distributors",
    "Furnishings, Fixtures & Appliances": "Household Appliances",
    "Gambling, Resorts & Casinos": "Casinos & Gaming",
    "General Utilities": "Utilities",
    "Gold": "Gold",
    "Grocery Stores": "Food Retail",
    "Hardware, Equipment & Parts": "Technology Hardware, Storage & Peripherals",
    "Home Improvement": "Home Improvement Retail",
    "Household & Personal Products": "Household & Personal Products",
    "Independent Power Producers": "Independent Power and Renewable Electricity Producers",
    "Industrial - Capital Goods": "Industrial Machinery",
    "Industrial - Distribution": "Trading Companies & Distributors",
    "Industrial - Infrastructure Operations": "Building Products",
    "Industrial - Machinery": "Industrial Machinery",
    "Industrial Materials": "Industrial Metals & Mining",
    "Industrial - Pollution & Treatment Controls": "Environmental Services",
    "Industrial - Specialties": "Industrial Products",
    "Information Technology Services": "IT Services",
    "Insurance - Brokers": "Insurance Brokers",
    "Insurance - Diversified": "Property & Casualty Insurance",
    "Insurance - Life": "Insurance: Life & Health",
    "Insurance - Property & Casualty": "Insurance: Property & Casualty",
    "Insurance - Reinsurance": "Reinsurance",
    "Insurance - Specialty": "Insurance Brokers",
    "Integrated Freight & Logistics": "Air Freight & Logistics",
    "Internet Content & Information": "Interactive Media & Services",
    "Investment - Banking & Investment Services": "Capital Markets",
    "Leisure": "Leisure Products",
    "Luxury Goods": "Apparel, Accessories & Luxury Goods",
    "Manufacturing - Metal Fabrication": "Industrial Machinery",
    "Manufacturing - Miscellaneous": "Industrial Products",
    "Manufacturing - Textiles": "Textiles, Apparel & Luxury Goods",
    "Manufacturing - Tools & Accessories": "Machinery",
    "Marine Shipping": "Marine Transportation",
    "Media & Entertainment": "Media",
    "Medical - Care Facilities": "Health Care Facilities",
    "Medical - Devices": "Health Care Equipment & Supplies",
    "Medical - Diagnostics & Research": "Health Care Equipment & Supplies",
    "Medical - Distribution": "Health Care Equipment & Supplies",
    "Medical - Equipment & Services": "Health Care Equipment & Supplies",
    "Medical - Healthcare Information Services": "Health Care Technology",
    "Medical - Healthcare Plans": "Health Care Providers & Services",
    "Medical - Instruments & Supplies": "Health Care Equipment & Supplies",
    "Medical - Pharmaceuticals": "Pharmaceuticals",
    "Medical - Specialties": "Health Care Providers & Services",
    "Oil & Gas Drilling": "Oil & Gas Drilling",
    "Oil & Gas Energy": "Oil & Gas Exploration & Production",
    "Oil & Gas Equipment & Services": "Oil & Gas Equipment & Services",
    "Oil & Gas Exploration & Production": "Oil & Gas Exploration & Production",
    "Oil & Gas Integrated": "Integrated Oil & Gas",
    "Oil & Gas Midstream": "Oil & Gas Storage & Transportation",
    "Oil & Gas Refining & Marketing": "Oil Refining & Marketing",
    "Other Precious Metals": "Metals & Mining",
    "Packaged Foods": "Packaged Foods & Meats",
    "Packaging & Containers": "Containers & Packaging",
    "Paper, Lumber & Forest Products": "Paper Products",
    "Personal Products & Services": "Personal Products",
    "Publishing": "Publishing",
    "Railroads": "Railroads",
    "Real Estate - Development": "Real Estate Management & Development",
    "Real Estate - Diversified": "Real Estate Management & Development",
    "Real Estate - General": "Real Estate Management & Development",
    "Real Estate - Services": "Real Estate Services",
    "Regulated Electric": "Electric Utilities",
    "Regulated Gas": "Gas Utilities",
    "Regulated Water": "Water Utilities",
    "REIT - Diversified": "Diversified REITs",
    "REIT - Healthcare Facilities": "Health Care REITs",
    "REIT - Hotel & Motel": "Hotel & Resort REITs",
    "REIT - Industrial": "Industrial REITs",
    "REIT - Mortgage": "Mortgage REITs",
    "REIT - Office": "Office REITs",
    "REIT - Residential": "Residential REITs",
    "REIT - Retail": "Retail REITs",
    "REIT - Specialty": "Specialized REITs",
    "Renewable Utilities": "Independent Power and Renewable Electricity Producers",
    "Rental & Leasing Services": "Consumer Services",
    "Residential Construction": "Home Construction",
    "Restaurants": "Restaurants",
    "Security & Protection Services": "Security Services",
    "Semiconductors": "Semiconductors",
     "Silver": "Silver",
    "Software - Application": "Application Software",
    "Software - Infrastructure": "Systems Software",
    "Software - Services": "Application Software",
    "Solar": "Solar Energy",
    "Specialty Business Services": "Professional Services",
    "Specialty Retail": "Specialty Retail",
    "Staffing & Employment Services": "Professional Services",
    "Steel": "Steel",
    "Technology Distributors": "Technology Distributors",
    "Telecommunications Services": "Integrated Telecommunication Services",
    "Tobacco": "Tobacco",
    "Travel Lodging": "Hotels, Resorts & Cruise Lines",
    "Travel Services": "Travel & Tourism",
    "Trucking": "Truck Transportation",
    "Uranium": "Metals & Mining",
    "Waste Management": "Environmental & Facilities Services" 
    }

    limite = 158  # ajustable

    # -----------------------
    # CONSULTA FILAS VACÍAS
    # -----------------------
    cursor.execute("""
        SELECT industria_fmp
        FROM mapa_industrias
        WHERE industria_google IS NULL
        LIMIT %s
    """, (limite,))

    resultados = cursor.fetchall()

    if not resultados:
        logging.info("✅ No hay industrias pendientes por normalizar.")
    else:
        for fila in resultados:
            industria_fmp = fila[0]
            if not industria_fmp:
                continue  # salta filas completamente vacías

            industria_google = mapeo_industrias.get(industria_fmp)

            if industria_google:
                cursor.execute("""
                    UPDATE mapa_industrias
                    SET industria_google = %s
                    WHERE industria_fmp = %s
                """, (industria_google, industria_fmp))
                conn.commit()
                logging.info(f"🟢 {industria_fmp} → {industria_google}")
            else:
                logging.warning(f"⚠️ No hay mapeo para: {industria_fmp}")

except Exception as e:
    logging.error(f"❌ Error general: {e}")
    if conn:
        conn.rollback()

finally:
    if 'conn' in locals() and conn:
        cursor.close()
        conn.close()
        logging.info("🔒 Conexión cerrada.")
