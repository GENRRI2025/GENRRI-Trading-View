"""
Downloads the full TSE listed-company roster from the official JPX Excel file
and populates the stocks table in portfolio.db.

Run once (or monthly):  python3 fetch_stocks.py
"""

import sqlite3, os, io, requests, xlrd

DB_PATH = os.path.join(os.path.dirname(__file__), 'portfolio.db')

# JPX publishes the current list at this URL (updated ~3rd business day each month)
JPX_URL = (
    "https://www.jpx.co.jp/markets/statistics-equities/misc/"
    "tvdivq0000001vg2-att/data_j.xls"
)

# Sector code → readable label (33 Tokyo Stock Exchange industry classifications)
SECTOR_MAP = {
    "0050": "Fishery, Agriculture & Forestry",
    "1050": "Mining",
    "2050": "Construction",
    "3050": "Foods",
    "3100": "Textiles & Apparel",
    "3150": "Pulp & Paper",
    "3200": "Chemicals",
    "3250": "Pharmaceutical",
    "3300": "Oil & Coal Products",
    "3350": "Rubber Products",
    "3400": "Glass & Ceramics",
    "3450": "Steel Products",
    "3500": "Nonferrous Metals",
    "3550": "Metal Products",
    "3600": "Machinery",
    "3650": "Electric Machinery",
    "3700": "Transportation Equipment",
    "3750": "Precision Instruments",
    "3800": "Other Products",
    "4050": "Electric Power & Gas",
    "5050": "Land Transportation",
    "5100": "Marine Transportation",
    "5150": "Air Transportation",
    "5200": "Warehousing & Harbor",
    "5250": "Information & Communication",
    "6050": "Wholesale Trade",
    "6100": "Retail Trade",
    "7050": "Banks",
    "7100": "Securities & Commodity Futures",
    "7150": "Insurance",
    "7200": "Other Financial Services",
    "8050": "Real Estate",
    "9050": "Services",
}

def init_stocks_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            symbol      TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            name_jp     TEXT NOT NULL,
            sector      TEXT NOT NULL,
            market      TEXT NOT NULL,
            code        TEXT NOT NULL
        )
    """)
    conn.commit()

def fetch_and_store():
    print(f"Downloading JPX listed-issues file from:\n  {JPX_URL}")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; stock-sim/1.0)"}
    resp = requests.get(JPX_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    print(f"Downloaded {len(resp.content):,} bytes")

    wb = xlrd.open_workbook(file_contents=resp.content)
    ws = wb.sheet_by_index(0)

    rows = [ws.row_values(i) for i in range(ws.nrows)]
    # Find header row (contains "コード" or "Code")
    header_row = 0
    for i, row in enumerate(rows):
        if row and any(str(c) in ("コード", "Code", "銘柄コード") for c in row if c):
            header_row = i
            break

    headers_found = [str(c) if c is not None else "" for c in rows[header_row]]
    print(f"Header row {header_row}: {headers_found[:8]}")

    # Locate columns: code, name (JP), market division, sector code
    def col(names):
        for n in names:
            for i, h in enumerate(headers_found):
                if n in h:
                    return i
        return None

    idx_code   = col(["コード", "Code", "銘柄コード"])
    idx_name   = col(["銘柄名", "会社名", "Name"])
    idx_market = col(["市場・商品区分", "市場区分", "Market"])
    idx_sector = col(["業種コード", "33業種コード"])
    idx_sector_name = col(["業種名", "33業種区分"])

    print(f"Columns → code:{idx_code} name:{idx_name} market:{idx_market} "
          f"sector_code:{idx_sector} sector_name:{idx_sector_name}")

    conn = sqlite3.connect(DB_PATH)
    init_stocks_table(conn)
    conn.execute("DELETE FROM stocks")

    count = 0
    for row in rows[header_row + 1:]:
        if not row or idx_code is None or idx_name is None:
            continue
        code_raw = row[idx_code]
        name_jp  = row[idx_name]
        if not code_raw or not name_jp:
            continue

        code = str(code_raw).strip().replace(".0","").zfill(4)
        if not code.isdigit():
            continue

        symbol = f"{code}.T"
        name_en = str(name_jp).strip()   # Use JP name as fallback for now
        market  = str(row[idx_market]).strip() if idx_market is not None and row[idx_market] else "TSE"

        # Sector: prefer name col, fall back to code map
        sector = ""
        if idx_sector_name is not None and row[idx_sector_name]:
            sector = str(row[idx_sector_name]).strip()
        elif idx_sector is not None and row[idx_sector]:
            sc = str(row[idx_sector]).strip().replace(".0","")
            sector = SECTOR_MAP.get(sc, sc)

        conn.execute(
            "INSERT OR REPLACE INTO stocks (symbol, name, name_jp, sector, market, code) VALUES (?,?,?,?,?,?)",
            (symbol, name_en, name_en, sector or "Other", market, code)
        )
        count += 1

    conn.commit()
    conn.close()
    print(f"\n✅  Stored {count:,} stocks in database.")

if __name__ == "__main__":
    fetch_and_store()
