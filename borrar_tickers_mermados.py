import sqlite3

DB_PATH = "fmp_datafree.db"

def borrar_tickers_sin_estados():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT ticker FROM tickers_consultados")
    tickers = [row[0] for row in cur.fetchall()]

    eliminados = []

    for ticker in tickers:
        cur.execute("SELECT COUNT(*) FROM ratios_plana WHERE ticker=?", (ticker,))
        r = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM income_statement_plana WHERE ticker=?", (ticker,))
        i = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM balance_sheet_plana WHERE ticker=?", (ticker,))
        b = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cash_flow_plana WHERE ticker=?", (ticker,))
        c = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM enterprise_values_plana WHERE ticker=?", (ticker,))
        ev = cur.fetchone()[0]

        if r == 0 and i == 0 and b == 0 and c == 0 and ev == 0:
            eliminados.append(ticker)
            cur.execute("DELETE FROM tickers_consultados WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM income_statement WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM income_statement_plana WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM balance_sheet WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM balance_sheet_plana WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM cash_flow WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM cash_flow_plana WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM ratios WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM ratios_plana WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM enterprise_values WHERE ticker=?", (ticker,))
            cur.execute("DELETE FROM enterprise_values_plana WHERE ticker=?", (ticker,))

    conn.commit()
    conn.close()

    print(f"✅ Tickers eliminados por estar vacíos en los 5 estados: {len(eliminados)}")
    if eliminados:
        print("Ejemplos:", eliminados[:10])

if __name__ == "__main__":
    borrar_tickers_sin_estados()

