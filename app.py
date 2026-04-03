from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import yfinance as yf
import json
import os
import sqlite3
import uuid
import hashlib
import secrets
from datetime import datetime, timedelta
import threading
from db import get_db, init_db, get_raw_conn, USE_PG, DB_PATH

import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import ssl
import re

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


def fetch_google_news(query, max_items=10, lang='ja'):
    """Fetch news from Google News RSS for broader coverage."""
    try:
        encoded = urllib.parse.quote(query)
        if lang == 'en':
            url = f'https://news.google.com/rss/search?q={encoded}&hl=en&gl=US&ceid=US:en'
        else:
            url = f'https://news.google.com/rss/search?q={encoded}&hl=ja&gl=JP&ceid=JP:ja'
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        ctx = ssl.create_default_context()
        try:
            with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
                data = resp.read()
        except Exception:
            # Fallback: skip SSL verification if certs are broken (macOS Python)
            ctx = ssl._create_unverified_context()
            with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
                data = resp.read()
        root = ET.fromstring(data)
        items = []
        for item in root.findall('.//item')[:max_items]:
            title = item.findtext('title', '')
            pub_date = item.findtext('pubDate', '')
            source = item.findtext('source', '')
            desc = item.findtext('description', '')
            # Clean HTML from description
            desc = re.sub(r'<[^>]+>', '', desc)[:300]
            link = item.findtext('link', '')
            if title:
                items.append({'title': title, 'date': pub_date, 'source': source, 'summary': desc, 'url': link})
        return items
    except Exception:
        return []

app = Flask(__name__, static_folder='static')
ALLOWED_ORIGINS = os.environ.get('ALLOWED_ORIGINS', '*')
CORS(app, origins=[o.strip() for o in ALLOWED_ORIGINS.split(',')])


@app.route('/health')
def health_check():
    return jsonify({'status': 'ok'}), 200


def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(32)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), 100000)
    return hashed.hex(), salt


def verify_password(password, stored_hash, salt):
    hashed, _ = hash_password(password, salt)
    return secrets.compare_digest(hashed, stored_hash)


init_db()


_JPX_MARKET_MAP = {
    'プライム（内国株式）': 'Prime',
    'スタンダード（内国株式）': 'Standard',
    'グロース（内国株式）': 'Growth',
    'プライム（外国株式）': 'Prime',
    'スタンダード（外国株式）': 'Standard',
    'グロース（外国株式）': 'Growth',
}
_JPX_SECTOR_MAP = {
    '水産・農林業': 'Fishery/Agriculture', '鉱業': 'Mining', '建設業': 'Construction',
    '食料品': 'Foods', '繊維製品': 'Textiles', 'パルプ・紙': 'Pulp & Paper',
    '化学': 'Chemicals', '医薬品': 'Pharmaceuticals', '石油・石炭製品': 'Oil & Coal',
    'ゴム製品': 'Rubber', 'ガラス・土石製品': 'Glass & Ceramics', '鉄鋼': 'Iron & Steel',
    '非鉄金属': 'Nonferrous Metals', '金属製品': 'Metal Products', '機械': 'Machinery',
    '電気機器': 'Electric Appliances', '輸送用機器': 'Transport Equipment',
    '精密機器': 'Precision Instruments', 'その他製品': 'Other Products',
    '電気・ガス業': 'Electric Power & Gas', '陸運業': 'Land Transportation',
    '海運業': 'Marine Transportation', '空運業': 'Air Transportation',
    '倉庫・運輸関連業': 'Warehousing', '情報・通信業': 'Information & Communication',
    '卸売業': 'Wholesale Trade', '小売業': 'Retail Trade', '銀行業': 'Banking',
    '証券、商品先物取引業': 'Securities', '保険業': 'Insurance',
    'その他金融業': 'Other Financial', '不動産業': 'Real Estate', 'サービス業': 'Services',
}
_STOCK_LIST_REFRESH_INTERVAL = 6 * 3600  # 6 hours
_last_stock_refresh = None

# ── Finnhub integration (real-time US quotes) ────────────────────
FINNHUB_API_KEY = os.environ.get('FINNHUB_API_KEY', '')
_FINNHUB_BASE = 'https://finnhub.io/api/v1'
_finnhub_call_times = []  # timestamps of recent calls for rate limiting
_FINNHUB_RATE_LIMIT = 55  # stay under 60/min
_US_STOCK_REFRESH_INTERVAL = 24 * 3600  # 24 hours
_last_us_stock_refresh = None


def _is_us_symbol(symbol):
    """Check if a symbol is a US stock (not JP, not index, not forex)."""
    return bool(symbol) and not symbol.endswith('.T') and '=' not in symbol and not symbol.startswith('^')


def _finnhub_get(path, params=None):
    """Make a rate-limited GET request to Finnhub API. Returns parsed JSON or None."""
    if not FINNHUB_API_KEY:
        return None
    now_ts = datetime.now().timestamp()
    # Prune calls older than 60 seconds
    _finnhub_call_times[:] = [t for t in _finnhub_call_times if now_ts - t < 60]
    if len(_finnhub_call_times) >= _FINNHUB_RATE_LIMIT:
        return None  # rate limited
    _finnhub_call_times.append(now_ts)

    query = urllib.parse.urlencode({**(params or {}), 'token': FINNHUB_API_KEY})
    url = f'{_FINNHUB_BASE}{path}?{query}'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            return json.loads(resp.read())
    except Exception:
        try:
            ctx = ssl._create_unverified_context()
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                return json.loads(resp.read())
        except Exception:
            return None


def _fetch_us_stocks():
    """Fetch US common stock listings from Finnhub. Returns list of stock tuples."""
    data = _finnhub_get('/stock/symbol', {'exchange': 'US'})
    if not data:
        return None
    _MIC_MAP = {
        'XNYS': 'NYSE', 'XNAS': 'NASDAQ', 'XASE': 'AMEX',
        'ARCX': 'NYSE Arca', 'BATS': 'BATS',
    }
    stocks = []
    for item in data:
        if item.get('type') != 'Common Stock':
            continue
        mic = item.get('mic', '')
        market = _MIC_MAP.get(mic)
        if not market:
            continue
        sym = item.get('symbol', '').strip()
        desc = item.get('description', '').strip()
        if not sym or not desc:
            continue
        # Skip symbols with dots/slashes (preferred shares, warrants, etc.)
        if '.' in sym or '/' in sym:
            continue
        stocks.append((sym, desc, desc, 'US', market, sym))
    return stocks


def refresh_us_stock_list():
    """Fetch US stock listings from Finnhub and update the database."""
    global _last_us_stock_refresh
    if not FINNHUB_API_KEY:
        return
    try:
        new_stocks = _fetch_us_stocks()
        if not new_stocks:
            return
        if len(new_stocks) < 2000:
            print(f"[us-stocks] Too few stocks ({len(new_stocks)}), skipping update", flush=True)
            return

        conn = get_db()

        # Get current US symbols in DB
        existing = set()
        try:
            rows = conn.execute("SELECT symbol FROM stocks WHERE symbol NOT LIKE ?", ('%.T',)).fetchall()
            existing = {r[0] for r in rows}
        except Exception:
            pass

        new_symbols = {s[0] for s in new_stocks}
        added = new_symbols - existing
        removed = existing - new_symbols

        if added:
            print(f"[us-stocks] Newly listed ({len(added)}): {', '.join(sorted(added)[:20])}{'...' if len(added) > 20 else ''}", flush=True)
        if removed:
            print(f"[us-stocks] Delisted ({len(removed)}): {', '.join(sorted(removed)[:20])}{'...' if len(removed) > 20 else ''}", flush=True)

        # Delete existing US stocks, keep JP stocks intact
        conn.execute("DELETE FROM stocks WHERE symbol NOT LIKE ?", ('%.T',))
        conn.executemany(
            'INSERT OR REPLACE INTO stocks (symbol, name, name_jp, sector, market, code) VALUES (?,?,?,?,?,?)',
            new_stocks)
        conn.commit()
        conn.close()

        _last_us_stock_refresh = datetime.now()
        change_msg = ''
        if added or removed:
            change_msg = f' (added: {len(added)}, removed: {len(removed)})'
        print(f"[us-stocks] Refreshed: {len(new_stocks)} US stocks from Finnhub{change_msg}", flush=True)
    except Exception as e:
        print(f"[us-stocks] Could not refresh from Finnhub: {e}", flush=True)


def _us_stock_list_auto_refresh():
    """Background thread: periodically refresh US stock listings."""
    import time as _time
    while True:
        _time.sleep(_US_STOCK_REFRESH_INTERVAL)
        try:
            print(f"[us-stocks] Auto-refresh: checking Finnhub for listing changes...", flush=True)
            refresh_us_stock_list()
        except Exception as e:
            print(f"[us-stocks] Auto-refresh error: {e}", flush=True)


def _fetch_jpx_stocks():
    """Download the JPX listed stocks file and return list of stock tuples."""
    try:
        import xlrd
    except ImportError:
        print("[stocks] xlrd not installed, skipping JPX refresh", flush=True)
        return None

    url = 'https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=20, context=ctx) as resp:
            data = resp.read()
    except Exception:
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=20, context=ctx) as resp:
            data = resp.read()

    tmp = os.path.join('/tmp', 'jpx_listed.xls')
    with open(tmp, 'wb') as f:
        f.write(data)

    wb = xlrd.open_workbook(tmp)
    ws = wb.sheet_by_index(0)
    stocks = []
    for r in range(1, ws.nrows):
        row = [str(ws.cell_value(r, c)).strip() for c in range(ws.ncols)]
        code_raw, name_jp, market_jp, sector_jp = row[1], row[2], row[3], row[5]
        market_en = _JPX_MARKET_MAP.get(market_jp)
        if not market_en:
            continue
        code = code_raw[:-2] if code_raw.endswith('.0') else code_raw
        symbol = f'{code}.T'
        sector_en = _JPX_SECTOR_MAP.get(sector_jp, sector_jp)
        stocks.append((symbol, name_jp, name_jp, sector_en, market_en, code))
    return stocks


def refresh_stock_list():
    """Fetch the full TSE stock list from JPX and update the database.
    Detects newly listed and delisted stocks."""
    global _last_stock_refresh
    try:
        new_stocks = _fetch_jpx_stocks()
        if not new_stocks:
            return
        if len(new_stocks) < 1000:
            print(f"[stocks] Too few stocks ({len(new_stocks)}), skipping update", flush=True)
            return

        conn = get_db()

        # Get current symbols in DB for change detection
        existing = set()
        try:
            rows = conn.execute('SELECT symbol FROM stocks').fetchall()
            existing = {r[0] for r in rows}
        except Exception:
            pass

        new_symbols = {s[0] for s in new_stocks}

        # Detect changes
        added = new_symbols - existing
        removed = existing - new_symbols

        if added:
            print(f"[stocks] Newly listed ({len(added)}): {', '.join(sorted(added)[:20])}{'...' if len(added) > 20 else ''}", flush=True)
        if removed:
            print(f"[stocks] Delisted ({len(removed)}): {', '.join(sorted(removed)[:20])}{'...' if len(removed) > 20 else ''}", flush=True)

        # Update database
        conn.execute('DELETE FROM stocks')
        conn.executemany(
            'INSERT OR REPLACE INTO stocks (symbol, name, name_jp, sector, market, code) VALUES (?,?,?,?,?,?)',
            new_stocks)
        conn.commit()
        conn.close()

        _last_stock_refresh = datetime.now()
        change_msg = ''
        if added or removed:
            change_msg = f' (added: {len(added)}, removed: {len(removed)})'
        print(f"[stocks] Refreshed: {len(new_stocks)} stocks from JPX{change_msg}", flush=True)
    except Exception as e:
        print(f"[stocks] Could not refresh from JPX: {e}", flush=True)


def _stock_list_auto_refresh():
    """Background thread: periodically refresh the stock list from JPX."""
    import time as _time
    while True:
        _time.sleep(_STOCK_LIST_REFRESH_INTERVAL)
        try:
            print(f"[stocks] Auto-refresh: checking JPX for listing changes...", flush=True)
            refresh_stock_list()
        except Exception as e:
            print(f"[stocks] Auto-refresh error: {e}", flush=True)


# Refresh stock list on startup
refresh_stock_list()
refresh_us_stock_list()


def get_auth_user():
    """Returns (user_id, error_response) — checks X-Auth-Token header."""
    token = request.headers.get('X-Auth-Token', '')
    if not token:
        return None, (jsonify({'error': 'Not authenticated', 'code': 'AUTH_REQUIRED'}), 401)
    conn = get_db()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = conn.execute(
        'SELECT user_id FROM sessions WHERE token = ? AND expires_at > ?', (token, now)
    ).fetchone()
    conn.close()
    if not row:
        return None, (jsonify({'error': 'Session expired', 'code': 'AUTH_REQUIRED'}), 401)
    return row['user_id'], None


def ensure_user_portfolio(conn, user_id):
    """Create portfolio + default settings for a new user if not yet present."""
    row = conn.execute('SELECT id FROM portfolio WHERE user_id = ?', (user_id,)).fetchone()
    if not row:
        fund_row = conn.execute("SELECT value FROM settings WHERE user_id = 'default' AND key = 'fund_amount'").fetchone()
        fund = float(fund_row['value']) if fund_row else 1000000.0
        conn.execute('INSERT INTO portfolio (user_id, cash) VALUES (?, ?)', (user_id, fund))
        for key, val in [('fund_amount', str(int(fund))), ('theme', 'dark')]:
            conn.execute('INSERT OR IGNORE INTO settings (user_id, key, value) VALUES (?, ?, ?)', (user_id, key, val))
        conn.commit()
    # Ensure a 'main' sub_portfolio exists
    main_id = f'main_{user_id}'
    sp = conn.execute('SELECT id FROM sub_portfolios WHERE id = ?', (main_id,)).fetchone()
    if not sp:
        fund_row = conn.execute("SELECT value FROM settings WHERE user_id = ? AND key = 'fund_amount'", (user_id,)).fetchone()
        if not fund_row:
            fund_row = conn.execute("SELECT value FROM settings WHERE user_id = 'default' AND key = 'fund_amount'").fetchone()
        fund = float(fund_row['value']) if fund_row else 1000000.0
        # Sync cash from legacy portfolio table
        cash_row = conn.execute('SELECT cash FROM portfolio WHERE user_id = ?', (user_id,)).fetchone()
        cash = cash_row['cash'] if cash_row else fund
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('INSERT OR IGNORE INTO sub_portfolios (id, user_id, name, cash, fund_amount, created_at) VALUES (?, ?, ?, ?, ?, ?)',
                     (main_id, user_id, 'My Portfolio', cash, fund, now))
        # Insert initial deposit into fund_transactions
        # Use the earliest transaction date (or portfolio creation date) so it sorts to the bottom
        has_ft = conn.execute('SELECT 1 FROM fund_transactions WHERE user_id = ? AND portfolio_id = ? LIMIT 1', (user_id, main_id)).fetchone()
        if not has_ft:
            earliest_txn = conn.execute('SELECT MIN(timestamp) as ts FROM transactions WHERE user_id = ? AND portfolio_id = ?', (user_id, main_id)).fetchone()
            initial_ts = earliest_txn['ts'] if earliest_txn and earliest_txn['ts'] else now
            # Make it 1 second before the earliest transaction so it's always last
            if initial_ts != now:
                try:
                    from datetime import timedelta
                    dt = datetime.strptime(initial_ts[:19], '%Y-%m-%d %H:%M:%S')
                    initial_ts = (dt - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    pass
            conn.execute('INSERT INTO fund_transactions (user_id, portfolio_id, type, amount, note, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                         (user_id, main_id, 'deposit', fund, 'Initial deposit', initial_ts))
        conn.commit()


def get_portfolio_id(user_id, requested_id=None):
    """Return the effective portfolio_id. Defaults to main_{user_id}."""
    if not requested_id or requested_id == 'main':
        return f'main_{user_id}'
    return requested_id


def get_fund_amount(conn, user_id, portfolio_id):
    """Compute fund amount from fund_transactions (deposits - withdrawals)."""
    row = conn.execute(
        "SELECT COALESCE(SUM(CASE WHEN type='deposit' THEN amount ELSE 0 END), 0) as deposits, "
        "COALESCE(SUM(CASE WHEN type='withdrawal' THEN amount ELSE 0 END), 0) as withdrawals "
        "FROM fund_transactions WHERE user_id = ? AND portfolio_id = ?",
        (user_id, portfolio_id)
    ).fetchone()
    return row['deposits'] - row['withdrawals']


def get_fund_totals(conn, user_id, portfolio_id):
    """Return (total_deposits, total_withdrawals) from fund_transactions."""
    row = conn.execute(
        "SELECT COALESCE(SUM(CASE WHEN type='deposit' THEN amount ELSE 0 END), 0) as deposits, "
        "COALESCE(SUM(CASE WHEN type='withdrawal' THEN amount ELSE 0 END), 0) as withdrawals "
        "FROM fund_transactions WHERE user_id = ? AND portfolio_id = ?",
        (user_id, portfolio_id)
    ).fetchone()
    return row['deposits'], row['withdrawals']


@app.route('/')
def index():
    resp = send_from_directory('static', 'index.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp


@app.route('/manifest.json')
def manifest():
    return send_from_directory('static', 'manifest.json')


@app.route('/sw.js')
def service_worker():
    return send_from_directory('static', 'sw.js', mimetype='application/javascript')


@app.route('/api/register', methods=['POST'])
def register():
    data = request.json or {}
    username = (data.get('username') or '').strip()
    password = (data.get('password') or '').strip()
    invite_code = (data.get('invite_code') or '').strip()
    expected_code = os.environ.get('INVITE_CODE', 'GENRRI')
    if expected_code and invite_code != expected_code:
        return jsonify({'error': 'Invalid invite code'}), 403
    if not username or not password:
        return jsonify({'error': 'Username and password are required'}), 400
    if len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    conn = get_db()
    existing = conn.execute('SELECT id FROM users WHERE name = ?', (username,)).fetchone()
    if existing:
        conn.close()
        return jsonify({'error': 'Username already taken'}), 409
    user_id = str(uuid.uuid4())
    pw_hash, salt = hash_password(password)
    conn.execute('INSERT INTO users (id, name, password_hash, salt, created_at) VALUES (?, ?, ?, ?, ?)',
                 (user_id, username, pw_hash, salt, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    ensure_user_portfolio(conn, user_id)
    token = secrets.token_urlsafe(32)
    expires = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)', (token, user_id, expires))
    conn.commit()
    conn.close()
    return jsonify({'token': token, 'username': username, 'user_id': user_id})


@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = (data.get('username') or '').strip()
    password = (data.get('password') or '').strip()
    if not username or not password:
        return jsonify({'error': 'Username and password are required'}), 400
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE name = ?', (username,)).fetchone()
    if not user:
        conn.close()
        return jsonify({'error': 'Invalid username or password'}), 401
    # Handle legacy users without password (migrated accounts)
    if not user['password_hash']:
        conn.close()
        return jsonify({'error': 'Please set a password — contact admin or re-register'}), 401
    if not verify_password(password, user['password_hash'], user['salt']):
        conn.close()
        return jsonify({'error': 'Invalid username or password'}), 401
    token = secrets.token_urlsafe(32)
    expires = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)', (token, user['id'], expires))
    conn.commit()
    conn.close()
    return jsonify({'token': token, 'username': username, 'user_id': user['id']})


@app.route('/api/logout', methods=['POST'])
def logout():
    token = request.headers.get('X-Auth-Token', '')
    if token:
        conn = get_db()
        conn.execute('DELETE FROM sessions WHERE token = ?', (token,))
        conn.commit()
        conn.close()
    return jsonify({'success': True})


@app.route('/api/stocks')
def get_stocks():
    # Bulk symbol lookup — returns info for specific symbols
    symbols_param = request.args.get('symbols', '').strip()
    if symbols_param:
        syms = [s.strip() for s in symbols_param.split(',') if s.strip()]
        if syms:
            conn = get_db()
            placeholders = ','.join(['?'] * len(syms))
            rows = conn.execute(
                f"SELECT symbol, name_jp, sector, market, code FROM stocks WHERE symbol IN ({placeholders})",
                syms
            ).fetchall()
            conn.close()
            return jsonify({
                "total": len(rows), "offset": 0, "limit": len(rows),
                "stocks": [{"symbol": r[0], "name": r[1], "nameJP": r[1], "sector": r[2], "market": r[3], "code": r[4]} for r in rows]
            })

    q      = request.args.get('q', '').strip()
    sector = request.args.get('sector', '').strip()
    market = request.args.get('market', '').strip()
    region = request.args.get('region', 'jp').strip().lower()
    try:
        limit  = min(int(request.args.get('limit', 50)), 200)
        offset = int(request.args.get('offset', 0))
    except (ValueError, TypeError):
        limit, offset = 50, 0

    conn = get_db()
    where, params = [], []

    # Region filter: jp = symbols ending in .T, us = everything else
    if region == 'us':
        where.append("symbol NOT LIKE ?")
        params.append('%.T')
    else:
        where.append("symbol LIKE ?")
        params.append('%.T')

    if q:
        where.append("(name_jp LIKE ? OR symbol LIKE ? OR code LIKE ?)")
        like = f"%{q}%"
        params += [like, like, like]
    if sector:
        where.append("sector = ?"); params.append(sector)
    if market:
        where.append("market = ?"); params.append(market)

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM stocks {clause}", params).fetchone()[0]

    # Sort algorithm depends on region
    sort_param = request.args.get('sort', 'default')
    if sort_param == 'code':
        order_by = "code"
    elif region == 'us':
        order_by = "symbol"  # Alphabetical for US stocks
    else:
        order_by = ("CASE "
                    "WHEN market='プライム（内国株式）' THEN 1 "
                    "WHEN market='スタンダード（内国株式）' THEN 2 "
                    "WHEN market='グロース（内国株式）' THEN 3 "
                    "ELSE 4 END, code")

    rows = conn.execute(
        f"SELECT symbol, name_jp, sector, market, code FROM stocks {clause} ORDER BY {order_by} LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    conn.close()

    return jsonify({
        "total": total,
        "offset": offset,
        "limit": limit,
        "stocks": [{"symbol": r[0], "name": r[1], "nameJP": r[1], "sector": r[2], "market": r[3], "code": r[4]} for r in rows]
    })

@app.route('/api/sectors')
def get_sectors():
    region = request.args.get('region', 'jp').strip().lower()
    conn = get_db()
    if region == 'us':
        region_filter = "symbol NOT LIKE ?"
    else:
        region_filter = "symbol LIKE ?"
    rows = conn.execute(f"SELECT DISTINCT sector FROM stocks WHERE sector != '' AND {region_filter} ORDER BY sector", ('%.T',)).fetchall()
    markets = conn.execute(f"SELECT DISTINCT market FROM stocks WHERE market != '' AND {region_filter} ORDER BY market", ('%.T',)).fetchall()
    conn.close()
    return jsonify({"sectors": [r[0] for r in rows], "markets": [r[0] for r in markets]})

@app.route('/api/stocks/refresh', methods=['POST'])
def api_refresh_stocks():
    """Manually trigger a JPX stock list refresh."""
    uid, err = get_auth_user()
    if err: return err
    threading.Thread(target=refresh_stock_list, daemon=True).start()
    conn = get_db()
    count = conn.execute('SELECT COUNT(*) FROM stocks').fetchone()[0]
    conn.close()
    return jsonify({
        'status': 'refresh started',
        'current_count': count,
        'last_refresh': _last_stock_refresh.isoformat() if _last_stock_refresh else None,
        'auto_refresh_interval_hours': _STOCK_LIST_REFRESH_INTERVAL / 3600
    })

# ── Price cache ───────────────────────────────────────────────────
PRICE_CACHE = {}       # { symbol: { data, ts } }
PRICE_CACHE_TTL = 300   # 5 minutes — yfinance data is ~15 min delayed anyway

# ── Stock info cache ──────────────────────────────────────────────
STOCK_INFO_CACHE = {}       # { symbol: { data, ts } }
STOCK_INFO_CACHE_TTL = 1800  # 30 minutes
STOCK_INFO_DISK_CACHE = os.path.join(os.path.dirname(DB_PATH), 'stock_info_cache.json')

def _load_stock_info_cache():
    """Load stock info cache from disk on startup."""
    try:
        if os.path.exists(STOCK_INFO_DISK_CACHE):
            import json
            with open(STOCK_INFO_DISK_CACHE, 'r') as f:
                saved = json.load(f)
            count = 0
            for sym, entry in saved.items():
                STOCK_INFO_CACHE[sym] = {'data': entry['data'], 'ts': datetime.fromisoformat(entry['ts'])}
                count += 1
            if count:
                print(f'[stock-info] Loaded {count} cached entries from disk', flush=True)
    except Exception as e:
        print(f'[stock-info] Failed to load disk cache: {e}', flush=True)

def _save_stock_info_cache():
    """Persist stock info cache to disk."""
    try:
        import json
        out = {sym: {'data': e['data'], 'ts': e['ts'].isoformat()} for sym, e in STOCK_INFO_CACHE.items()}
        with open(STOCK_INFO_DISK_CACHE, 'w') as f:
            json.dump(out, f)
    except Exception:
        pass

def _fetch_finnhub_quote(symbol):
    """Fetch a real-time quote from Finnhub for a US stock."""
    data = _finnhub_get('/quote', {'symbol': symbol})
    if not data or data.get('c', 0) == 0:
        return None
    price = round(float(data['c']), 2)
    change = round(float(data.get('d', 0) or 0), 2)
    change_pct = round(float(data.get('dp', 0) or 0), 2)
    prev_close = round(float(data.get('pc', 0) or 0), 2)
    result = {
        'symbol': symbol,
        'price': price,
        'change': change,
        'change_pct': change_pct,
        'prev_close': prev_close,
        'chart': []
    }
    PRICE_CACHE[symbol] = {'data': result, 'ts': datetime.now()}
    return result


def _fetch_single_quote(symbol, with_chart=False):
    """Fetch a single quote, using cache when possible."""
    now = datetime.now()
    cached = PRICE_CACHE.get(symbol)
    if cached and (now - cached['ts']).total_seconds() < PRICE_CACHE_TTL:
        result = cached['data'].copy()
        if not with_chart:
            result.pop('chart', None)
        return result

    # Try Finnhub first for US stocks (real-time)
    if _is_us_symbol(symbol) and FINNHUB_API_KEY:
        fh_result = _fetch_finnhub_quote(symbol)
        if fh_result:
            if with_chart:
                # Fetch intraday chart from yfinance (Finnhub free has no candle data)
                try:
                    hist = yf.Ticker(symbol).history(period='1d', interval='5m')
                    if not hist.empty:
                        chart_data = []
                        for ts, row in hist.iterrows():
                            chart_data.append({'time': ts.strftime('%H:%M'), 'price': round(float(row['Close']), 2)})
                        fh_result['chart'] = chart_data
                except Exception:
                    pass
            return fh_result

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info
        price = float(info.last_price) if hasattr(info, 'last_price') and info.last_price else 0
        prev_close = float(info.previous_close) if hasattr(info, 'previous_close') and info.previous_close else 0

        # For indices or when prev_close is missing/same as price, try history fallback
        if not prev_close or prev_close == price:
            try:
                hist = ticker.history(period='5d', interval='1d')
                if len(hist) >= 2:
                    prev_close = float(hist['Close'].iloc[-2])
                    price = float(hist['Close'].iloc[-1])
            except Exception:
                pass
        if not prev_close:
            prev_close = price

        change = price - prev_close
        change_pct = (change / prev_close * 100) if prev_close else 0

        chart_data = []
        if with_chart:
            hist = ticker.history(period='1d', interval='5m')
            if not hist.empty:
                for ts, row in hist.iterrows():
                    chart_data.append({
                        'time': ts.strftime('%H:%M'),
                        'price': round(float(row['Close']), 2)
                    })

        result = {
            'symbol': symbol,
            'price': round(price, 2),
            'change': round(change, 2),
            'change_pct': round(change_pct, 2),
            'prev_close': round(prev_close, 2),
            'chart': chart_data
        }
        PRICE_CACHE[symbol] = {'data': result, 'ts': now}
        return result
    except Exception as e:
        return {'symbol': symbol, 'error': str(e)}


@app.route('/api/quote/<symbol>')
def get_quote(symbol):
    with_chart = request.args.get('chart', '0') == '1'
    result = _fetch_single_quote(symbol, with_chart=with_chart)
    if 'error' in result:
        return jsonify(result), 500
    if not with_chart:
        result.pop('chart', None)
    return jsonify(result)


@app.route('/api/quotes', methods=['POST'])
def get_quotes_batch():
    """Batch fetch quotes for multiple symbols at once — much faster."""
    data = request.json or {}
    symbols = data.get('symbols', [])
    if not symbols:
        return jsonify({})

    now = datetime.now()
    results = {}
    to_fetch = []

    # Check cache first
    for sym in symbols:
        cached = PRICE_CACHE.get(sym)
        if cached and (now - cached['ts']).total_seconds() < PRICE_CACHE_TTL:
            r = cached['data'].copy()
            r.pop('chart', None)
            results[sym] = r
        else:
            to_fetch.append(sym)

    # Split into US and JP symbols for different fetch strategies
    us_to_fetch = [s for s in to_fetch if _is_us_symbol(s)]
    jp_to_fetch = [s for s in to_fetch if not _is_us_symbol(s)]

    # Fetch US symbols via Finnhub (real-time, individual calls with rate limiting)
    if us_to_fetch and FINNHUB_API_KEY:
        for sym in us_to_fetch[:50]:  # cap at 50 to stay within rate limit
            fh = _fetch_finnhub_quote(sym)
            if fh:
                r = fh.copy()
                r.pop('chart', None)
                results[sym] = r
        # Any US symbols not fetched via Finnhub fall through to yfinance below
        us_remaining = [s for s in us_to_fetch if s not in results]
        jp_to_fetch.extend(us_remaining)

    # Batch fetch JP (and any remaining US) symbols using yf.download
    if jp_to_fetch:
        try:
            import pandas as pd
            tickers_str = ' '.join(jp_to_fetch)
            df = yf.download(tickers_str, period='5d', interval='1d',
                           group_by='ticker', progress=False, threads=True)

            for sym in jp_to_fetch:
                try:
                    sym_df = df[sym] if sym in df.columns.get_level_values(0) else None

                    if sym_df is not None and not sym_df.empty:
                        closes = sym_df['Close'].dropna()
                        if len(closes) >= 2:
                            price = round(float(closes.iloc[-1]), 2)
                            prev = round(float(closes.iloc[-2]), 2)
                        elif len(closes) == 1:
                            price = round(float(closes.iloc[-1]), 2)
                            prev = price
                        else:
                            continue

                        change = round(price - prev, 2)
                        change_pct = round((change / prev * 100) if prev else 0, 2)
                        result = {
                            'symbol': sym,
                            'price': price,
                            'change': change,
                            'change_pct': change_pct,
                            'prev_close': prev,
                        }
                        results[sym] = result
                        PRICE_CACHE[sym] = {'data': {**result, 'chart': []}, 'ts': now}
                except Exception:
                    pass
        except Exception:
            # Fallback: fetch individually with thread pool
            from concurrent.futures import ThreadPoolExecutor
            def _fetch_one(s):
                return s, _fetch_single_quote(s, with_chart=False)
            with ThreadPoolExecutor(max_workers=6) as ex:
                futures = list(ex.map(_fetch_one, jp_to_fetch))
                for sym, result in futures:
                    if 'error' not in result:
                        r = result.copy()
                        r.pop('chart', None)
                        results[sym] = r

    return jsonify(results)





# ── Chart data cache ─────────────────────────────────────────────
CHART_CACHE = {}        # { 'symbol:key': { 'data': [...], 'ts': datetime } }
CHART_CACHE_TTL_INTRADAY = 7200    # 2 hours for intraday charts
CHART_CACHE_TTL_DAILY    = 28800   # 8 hours for daily/weekly/monthly charts

CANDLE_CFG = {
    '1m':  ('5d',  '1m',  True),
    '5m':  ('1mo', '5m',  True),
    '15m': ('1mo', '15m', True),
    '30m': ('3mo', '30m', True),
    '60m': ('6mo', '60m', True),
    '1d':  ('2y',  '1d',  False),
    '1wk': ('5y',  '1wk', False),
    '1mo': ('max', '1mo', False),
}

LINE_MAP = {
    '1d':  ('1d',  '1m',  True),
    '5d':  ('5d',  '5m',  True),
    '1mo': ('1mo', '60m', True),
    '3mo': ('3mo', '1d',  False),
    '6mo': ('6mo', '1d',  False),
    '1y':  ('1y',  '1wk', False),
    '5y':  ('5y',  '1mo', False),
    'max': ('max', '1mo', False),
}


def _fetch_chart_data(symbol, yf_period, yf_interval, is_intraday):
    """Fetch chart data from Yahoo Finance. Returns list of OHLCV dicts."""
    JST_OFFSET = 9 * 3600
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period=yf_period, interval=yf_interval)
    chart_data = []
    seen_times = set()
    for ts, row in hist.iterrows():
        if is_intraday:
            t = int(ts.timestamp()) + JST_OFFSET
        else:
            t = ts.strftime('%Y-%m-%d')
        if t in seen_times:
            continue
        seen_times.add(t)
        chart_data.append({
            'time': t,
            'open':   round(float(row['Open']),  2),
            'high':   round(float(row['High']),  2),
            'low':    round(float(row['Low']),   2),
            'close':  round(float(row['Close']), 2),
            'volume': int(row['Volume'])
        })
    return chart_data


def _get_chart_cached(symbol, cache_key, yf_period, yf_interval, is_intraday):
    """Return chart data from cache or fetch fresh. Falls back to stale cache on error."""
    now = datetime.now()
    ttl = CHART_CACHE_TTL_INTRADAY if is_intraday else CHART_CACHE_TTL_DAILY
    cached = CHART_CACHE.get(cache_key)
    if cached and (now - cached['ts']).total_seconds() < ttl:
        return cached['data']
    try:
        data = _fetch_chart_data(symbol, yf_period, yf_interval, is_intraday)
        if data:
            CHART_CACHE[cache_key] = {'data': data, 'ts': now}
            return data
        # 1D with 1m often returns empty (market closed / no data) — fallback to 5D/5m
        if yf_period == '1d' and yf_interval == '1m':
            data = _fetch_chart_data(symbol, '5d', '5m', True)
            if data:
                CHART_CACHE[cache_key] = {'data': data, 'ts': now}
                return data
    except Exception as e:
        print(f"[chart] Error fetching {symbol} ({cache_key}): {e}")
    # Return stale cache if available
    if cached:
        return cached['data']
    return []


@app.route('/api/history/<symbol>')
def get_history(symbol):
    interval_req = request.args.get('interval', None)
    period = request.args.get('period', '1mo')

    if interval_req:
        yf_period, yf_interval, is_intraday = CANDLE_CFG.get(interval_req, ('1mo', '1d', False))
        cache_key = f"{symbol}:candle:{interval_req}"
    else:
        yf_period, yf_interval, is_intraday = LINE_MAP.get(period, ('1mo', '1d', False))
        cache_key = f"{symbol}:line:{period}"

    data = _get_chart_cached(symbol, cache_key, yf_period, yf_interval, is_intraday)
    return jsonify(data)

def _build_stock_info(symbol):
    """Fetch stock info from yfinance and return as dict. Uses cache."""
    now = datetime.now()
    cached = STOCK_INFO_CACHE.get(symbol)
    if cached and (now - cached['ts']).total_seconds() < STOCK_INFO_CACHE_TTL:
        return cached['data']

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
    except Exception:
        # Rate limited — return stale cache if available, else raise
        if cached:
            return cached['data']
        raise

    def safe(key, default=None):
        v = info.get(key)
        return v if v is not None else default

    result = {
        # Company
        'longName': safe('longName', symbol),
        'sector': safe('sector', ''),
        'industry': safe('industry', ''),
        'employees': safe('fullTimeEmployees'),
        'website': safe('website', ''),
        'summary': safe('longBusinessSummary', ''),
        'country': safe('country', ''),
        'city': safe('city', ''),

        # Valuation
        'marketCap': safe('marketCap'),
        'enterpriseValue': safe('enterpriseValue'),
        'trailingPE': safe('trailingPE'),
        'forwardPE': safe('forwardPE'),
        'priceToBook': safe('priceToBook'),
        'priceToSales': safe('priceToSalesTrailing12Months'),
        'enterpriseToEbitda': safe('enterpriseToEbitda'),
        'enterpriseToRevenue': safe('enterpriseToRevenue'),

        # Financials
        'revenue': safe('totalRevenue'),
        'ebitda': safe('ebitda'),
        'netIncome': safe('netIncomeToCommon'),
        'eps': safe('trailingEps'),
        'epsForward': safe('epsForward'),
        'profitMargin': safe('profitMargins'),
        'operatingMargin': safe('operatingMargins'),
        'grossMargin': safe('grossMargins'),
        'returnOnEquity': safe('returnOnEquity'),
        'returnOnAssets': safe('returnOnAssets'),
        'revenueGrowth': safe('revenueGrowth'),
        'earningsGrowth': safe('earningsGrowth'),

        # Dividends
        'dividendRate': safe('dividendRate'),
        'dividendYield': safe('dividendYield'),
        'payoutRatio': safe('payoutRatio'),
        'exDividendDate': safe('exDividendDate'),

        # Trading
        'volume': safe('volume'),
        'avgVolume': safe('averageVolume'),
        'fiftyTwoWeekHigh': safe('fiftyTwoWeekHigh'),
        'fiftyTwoWeekLow': safe('fiftyTwoWeekLow'),
        'fiftyDayAvg': safe('fiftyDayAverage'),
        'twoHundredDayAvg': safe('twoHundredDayAverage'),
        'beta': safe('beta'),

        # Balance Sheet
        'totalCash': safe('totalCash'),
        'totalDebt': safe('totalDebt'),
        'debtToEquity': safe('debtToEquity'),
        'currentRatio': safe('currentRatio'),
        'bookValue': safe('bookValue'),

        # Analyst
        'analystRating': safe('averageAnalystRating', ''),
        'targetHigh': safe('targetHighPrice'),
        'targetLow': safe('targetLowPrice'),
        'targetMean': safe('targetMeanPrice'),
        'targetMedian': safe('targetMedianPrice'),
        'numAnalysts': safe('numberOfAnalystOpinions'),
        'recommendationKey': safe('recommendationKey', ''),
    }
    STOCK_INFO_CACHE[symbol] = {'data': result, 'ts': now}
    # Persist to disk in background so it survives restarts
    threading.Thread(target=_save_stock_info_cache, daemon=True).start()
    return result


@app.route('/api/stock-info/<symbol>')
def get_stock_info(symbol):
    try:
        result = _build_stock_info(symbol)
        return jsonify(result)
    except Exception as e:
        # On rate limit or error, return stale cache if available rather than failing
        cached = STOCK_INFO_CACHE.get(symbol)
        if cached:
            return jsonify(cached['data'])
        return jsonify({'error': str(e)}), 500


# ── Background prefetch ──────────────────────────────────────────
_prefetch_running = False

_PREFETCH_INTERVAL = 45  # seconds between refreshes

def _prefetch_once():
    """Fetch fresh prices for all portfolio holdings, watchlist, and indices."""
    conn = get_db()

    holding_syms = set()
    watchlist_syms = set()
    try:
        rows = conn.execute('SELECT DISTINCT symbol FROM holdings').fetchall()
        holding_syms = {r[0] for r in rows}
    except Exception:
        pass
    try:
        rows = conn.execute('SELECT DISTINCT symbol FROM watchlist').fetchall()
        watchlist_syms = {r[0] for r in rows}
    except Exception:
        pass
    conn.close()

    all_syms = list(holding_syms | watchlist_syms)
    all_syms += ['^N225', 'USDJPY=X', '^GSPC', '^DJI', '^IXIC']

    if not all_syms:
        return holding_syms

    print(f'[prefetch] Refreshing prices for {len(all_syms)} symbols...', flush=True)

        # Batch-fetch prices using yf.download (fast, single HTTP call per batch)
    now = datetime.now()
    batch_size = 100
    for i in range(0, len(all_syms), batch_size):
        batch = all_syms[i:i + batch_size]
        try:
            tickers_str = ' '.join(batch)
            df = yf.download(tickers_str, period='5d', interval='1d',
                           group_by='ticker', progress=False, threads=True)
            for sym in batch:
                try:
                    if len(batch) == 1:
                        sym_df = df
                    else:
                        if sym not in df.columns.get_level_values(0):
                            continue
                        sym_df = df[sym]
                    if sym_df is None or sym_df.empty:
                        continue
                    closes = sym_df['Close'].dropna()
                    if len(closes) < 1:
                        continue
                    price = round(float(closes.iloc[-1]), 2)
                    prev = round(float(closes.iloc[-2]), 2) if len(closes) >= 2 else price
                    change = round(price - prev, 2)
                    change_pct = round((change / prev * 100) if prev else 0, 2)
                    result = {
                        'symbol': sym,
                        'price': price,
                        'change': change,
                        'change_pct': change_pct,
                        'prev_close': prev,
                        'chart': []
                    }
                    PRICE_CACHE[sym] = {'data': result, 'ts': now}
                except Exception:
                    pass
        except Exception as e:
            print(f'[prefetch] Batch price error: {e}', flush=True)

    print(f'[prefetch] Prices cached for {len(all_syms)} symbols ({len(PRICE_CACHE)} total in cache)', flush=True)
    return holding_syms


def _prefetch_background():
    """Continuously refresh prices for portfolio holdings, watchlist, and indices."""
    global _prefetch_running
    if _prefetch_running:
        return
    _prefetch_running = True
    import time as _time
    try:
        # First run: also prefetch stock info for holdings
        holding_syms = _prefetch_once()

        # Pre-fetch stock-info for holdings on first run only (slow calls)
        if holding_syms:
            from concurrent.futures import ThreadPoolExecutor
            info_syms = list(holding_syms)[:20]
            def _fetch_info(sym):
                try:
                    _build_stock_info(sym)
                except Exception:
                    pass
            with ThreadPoolExecutor(max_workers=4) as ex:
                ex.map(_fetch_info, info_syms)
            print(f'[prefetch] Stock info cached for {len(info_syms)} holdings', flush=True)

        # Pre-fetch chart data (1mo line) for holdings so charts load instantly
        if holding_syms:
            chart_syms = list(holding_syms)[:20]
            for sym in chart_syms:
                try:
                    yf_period, yf_interval, is_intraday = LINE_MAP['1mo']
                    cache_key = f"{sym}:line:1mo"
                    _get_chart_cached(sym, cache_key, yf_period, yf_interval, is_intraday)
                except Exception:
                    pass
                _time.sleep(0.5)  # gentle pace to avoid rate limiting
            print(f'[prefetch] Charts cached for {len(chart_syms)} holdings', flush=True)

        print(f'[prefetch] Initial prefetch complete. Refreshing every {_PREFETCH_INTERVAL}s...', flush=True)

        # Loop: keep refreshing prices
        while True:
            _time.sleep(_PREFETCH_INTERVAL)
            try:
                _prefetch_once()
            except Exception as e:
                print(f'[prefetch] Refresh error: {e}', flush=True)

    except Exception as e:
        print(f'[prefetch] Error: {e}', flush=True)
    finally:
        _prefetch_running = False


# ── Stock News ────────────────────────────────────────────────────
NEWS_CACHE = {}      # { symbol: { 'result': ..., 'ts': datetime } }
NEWS_CACHE_TTL = 1800  # 30 minutes

@app.route('/api/news/<symbol>')
def stock_news(symbol):
    """Fetch recent news for a stock from multiple internet sources."""
    now = datetime.now()
    cached = NEWS_CACHE.get(symbol)
    if cached and (now - cached['ts']).total_seconds() < NEWS_CACHE_TTL:
        return jsonify(cached['result'])

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        company_name_en = info.get('longName', info.get('shortName', symbol))

        # Get Japanese company name from DB for better JP news search
        conn_tmp = get_db()
        row = conn_tmp.execute('SELECT name_jp FROM stocks WHERE symbol = ?', (symbol,)).fetchone()
        conn_tmp.close()
        company_name_jp = row[0] if row else company_name_en

        # Yahoo Finance news
        yahoo_news = []
        try:
            raw_news = ticker.news or []
            for item in raw_news[:8]:
                content = item.get('content', item)
                title = content.get('title', '')
                summary = content.get('summary', '')
                pub = content.get('pubDate', '')
                link = content.get('canonicalUrl', {}).get('url', '') if isinstance(content.get('canonicalUrl'), dict) else content.get('link', '')
                provider = ''
                prov_obj = content.get('provider')
                if isinstance(prov_obj, dict):
                    provider = prov_obj.get('displayName', '')
                if title:
                    yahoo_news.append({
                        'title': title,
                        'summary': re.sub(r'<[^>]+>', '', summary)[:200],
                        'date': pub,
                        'source': provider or 'Yahoo Finance',
                        'url': link
                    })
        except Exception:
            pass

        # Google News RSS — JP/EN depending on stock region
        code = symbol.replace('.T', '')
        if _is_us_symbol(symbol):
            # US stock: English news only
            jp_news = []
            en_news = fetch_google_news(f'{company_name_en} stock', max_items=10, lang='en')
        else:
            # JP stock: both languages
            jp_news = fetch_google_news(f'{company_name_jp} {code}', max_items=8, lang='ja')
            en_news = fetch_google_news(f'{company_name_en} {code} stock', max_items=6, lang='en')
        google_news = []
        for gn in jp_news + en_news:
            google_news.append({
                'title': gn.get('title', ''),
                'summary': gn.get('summary', '')[:200],
                'date': gn.get('date', ''),
                'source': gn.get('source', 'Google News'),
                'url': gn.get('url', '')
            })

        # Merge and deduplicate by title prefix
        existing_titles = set()
        def dedup_add(lst, item):
            key = item['title'].lower()[:40]
            if key not in existing_titles:
                existing_titles.add(key)
                lst.append(item)

        def parse_date_key(d):
            if not d:
                return ''
            try:
                from email.utils import parsedate_to_datetime
                return parsedate_to_datetime(d).isoformat()
            except Exception:
                return d

        # Separate JP and EN Google News
        jp_gn, en_gn = [], []
        for gn in google_news:
            if any(ord(c) > 0x3000 for c in gn.get('title', '')[:30]):
                jp_gn.append(gn)
            else:
                en_gn.append(gn)

        # Build result: Yahoo (up to 5) + JP Google (up to 3) + EN Google (fill to 10)
        all_news = []
        for n in sorted(yahoo_news, key=lambda x: parse_date_key(x.get('date','')), reverse=True)[:5]:
            dedup_add(all_news, n)
        for n in jp_gn:
            dedup_add(all_news, n)
        for n in sorted(en_gn, key=lambda x: parse_date_key(x.get('date','')), reverse=True):
            dedup_add(all_news, n)

        # Sort combined by date and cap at 10
        all_news.sort(key=lambda x: parse_date_key(x.get('date', '')), reverse=True)
        result = all_news[:10]

        NEWS_CACHE[symbol] = {'result': result, 'ts': now}
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Market-Wide News ──────────────────────────────────────────────
MARKET_NEWS_CACHE = {}
MARKET_NEWS_TTL = 1800  # 30 minutes

@app.route('/api/market-news')
def market_news():
    region = request.args.get('region', 'jp').strip().lower()
    now = datetime.now()
    cached = MARKET_NEWS_CACHE.get(region)
    if cached and (now - cached['ts']).total_seconds() < MARKET_NEWS_TTL:
        return jsonify(cached['result'])

    try:
        if region == 'us':
            queries = [
                ('US stock market today Wall Street', 'en', 8),
                ('S&P 500 Nasdaq Dow Jones', 'en', 5),
                ('Federal Reserve economy US', 'en', 5),
            ]
        else:
            queries = [
                ('日経平均 株式市場', 'ja', 8),
                ('東証 マーケット 相場', 'ja', 5),
                ('日本経済 金融政策', 'ja', 5),
                ('Japan stock market Nikkei', 'en', 5),
                ('Tokyo Stock Exchange economy', 'en', 4),
            ]

        all_items = []
        existing_titles = set()

        def parse_date_key(d):
            if not d:
                return ''
            try:
                from email.utils import parsedate_to_datetime
                return parsedate_to_datetime(d).isoformat()
            except Exception:
                return d

        for query, lang, limit in queries:
            items = fetch_google_news(query, max_items=limit, lang=lang)
            for item in items:
                key = item.get('title', '').lower()[:40]
                if key and key not in existing_titles:
                    existing_titles.add(key)
                    all_items.append({
                        'title': item.get('title', ''),
                        'summary': item.get('summary', '')[:200],
                        'date': item.get('date', ''),
                        'source': item.get('source', 'Google News'),
                        'url': item.get('url', '')
                    })

        all_items.sort(key=lambda x: parse_date_key(x.get('date', '')), reverse=True)
        result = all_items[:15]

        MARKET_NEWS_CACHE[region] = {'result': result, 'ts': now}
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── AI Analysis ───────────────────────────────────────────────────
AI_ANALYSIS_CACHE = {}   # { symbol: { 'result': ..., 'ts': datetime } }
AI_CACHE_TTL = 3600      # 1 hour cache

@app.route('/api/ai-analysis/<symbol>')
def ai_analysis(symbol):
    api_key = request.headers.get('X-AI-Key', '') or os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return jsonify({'error': 'NO_KEY', 'message': 'Anthropic API key not configured. Add it in Settings → AI Analyst.'}), 400
    if not HAS_ANTHROPIC:
        return jsonify({'error': 'NO_SDK', 'message': 'anthropic package not installed on server.'}), 500

    # Check cache
    now = datetime.now()
    cached = AI_ANALYSIS_CACHE.get(symbol)
    if cached and (now - cached['ts']).total_seconds() < AI_CACHE_TTL:
        return jsonify(cached['result'])

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        # Gather fundamentals
        def s(k, d=None):
            v = info.get(k)
            return v if v is not None else d
        fundamentals = {
            'name': s('longName', symbol),
            'sector': s('sector', ''),
            'industry': s('industry', ''),
            'marketCap': s('marketCap'),
            'trailingPE': s('trailingPE'),
            'forwardPE': s('forwardPE'),
            'priceToBook': s('priceToBook'),
            'dividendYield': s('dividendYield'),
            'profitMargin': s('profitMargins'),
            'operatingMargin': s('operatingMargins'),
            'returnOnEquity': s('returnOnEquity'),
            'revenueGrowth': s('revenueGrowth'),
            'earningsGrowth': s('earningsGrowth'),
            'debtToEquity': s('debtToEquity'),
            'currentRatio': s('currentRatio'),
            'fiftyTwoWeekHigh': s('fiftyTwoWeekHigh'),
            'fiftyTwoWeekLow': s('fiftyTwoWeekLow'),
            'fiftyDayAverage': s('fiftyDayAverage'),
            'twoHundredDayAverage': s('twoHundredDayAverage'),
            'currentPrice': s('currentPrice') or s('regularMarketPrice'),
            'beta': s('beta'),
            'analystRating': s('averageAnalystRating', ''),
            'targetMeanPrice': s('targetMeanPrice'),
            'numberOfAnalysts': s('numberOfAnalystOpinions'),
        }

        # Gather news from multiple sources
        yahoo_news = []
        try:
            raw_news = ticker.news or []
            for item in raw_news[:8]:
                content = item.get('content', item)
                title = content.get('title', '')
                summary = content.get('summary', '')
                pub = content.get('pubDate', '')
                provider = ''
                prov_obj = content.get('provider')
                if isinstance(prov_obj, dict):
                    provider = prov_obj.get('displayName', '')
                if title:
                    yahoo_news.append({'title': title, 'summary': summary[:300], 'date': pub, 'source': f'Yahoo/{provider}' if provider else 'Yahoo Finance'})
        except Exception:
            pass

        # Google News — use Japanese name for JP search, English for EN
        company_name_en = fundamentals.get('name', symbol)
        code = symbol.replace('.T', '')
        conn_tmp2 = get_db()
        row2 = conn_tmp2.execute('SELECT name_jp FROM stocks WHERE symbol = ?', (symbol,)).fetchone()
        conn_tmp2.close()
        company_name_jp2 = row2[0] if row2 else company_name_en
        jp_gnews = fetch_google_news(f'{company_name_jp2} {code}', max_items=8, lang='ja')
        en_gnews = fetch_google_news(f'{company_name_en} {code} stock', max_items=6, lang='en')
        google_news = jp_gnews + en_gnews
        for gn in google_news:
            gn['source'] = f"Google News/{gn.get('source', '')}" if gn.get('source') else 'Google News'

        # Merge and deduplicate by title similarity
        all_news = yahoo_news[:]
        yahoo_titles = set(n['title'].lower()[:40] for n in yahoo_news)
        for gn in google_news:
            if gn['title'].lower()[:40] not in yahoo_titles:
                all_news.append(gn)
        news_items = all_news[:15]  # Cap at 15 articles

        # Build prompt
        lang = request.args.get('lang', 'en')
        lang_instruction = {
            'ja': 'Respond entirely in Japanese (日本語で回答してください).',
            'zh': 'Respond entirely in Chinese (请用中文回答).',
            'en': 'Respond in English.'
        }.get(lang, 'Respond in English.')

        prompt = f"""You are a senior equity research analyst covering Japanese markets. Provide a professional investment opinion on this stock.

{lang_instruction}

## Stock: {symbol} — {fundamentals['name']}

## Key Fundamentals
{json.dumps(fundamentals, indent=2, default=str)}

## Recent News & Events ({len(news_items)} articles from Yahoo Finance + Google News)
{json.dumps(news_items, indent=2, ensure_ascii=False, default=str) if news_items else 'No recent news available.'}

## Your Task
Beyond the data above, also leverage your training knowledge about this company — including its competitive position, management quality, sector trends, regulatory environment, corporate governance, ESG factors, and any known strategic initiatives, M&A activity, or structural changes.

Provide a structured analysis:
1. **Rating**: One of: Strong Buy / Buy / Hold / Sell / Strong Sell
2. **Confidence**: Low / Medium / High
3. **Summary**: 2-3 sentence executive summary
4. **Bull Case**: 2-3 key reasons to buy (bullet points)
5. **Bear Case**: 2-3 key risks (bullet points)
6. **Technical View**: Price vs moving averages, 52-week range position, momentum
7. **News & Catalysts**: Key recent developments and upcoming catalysts (earnings, dividends, sector events)
8. **Sector Context**: How this stock compares to peers in the same sector

IMPORTANT: End with a clear disclaimer that this is AI-generated analysis for informational purposes only and does NOT constitute investment advice (投資助言ではありません / 不构成投资建议). Note that your knowledge has a training cutoff date.

Keep the response under 600 words. Be specific with numbers."""

        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        analysis_text = message.content[0].text

        # Parse rating from response
        rating = 'Hold'
        for r in ['Strong Buy', 'Strong Sell', 'Buy', 'Sell', 'Hold']:
            if r.lower() in analysis_text.lower()[:200]:
                rating = r
                break

        confidence = 'Medium'
        for c in ['High', 'Medium', 'Low']:
            if c.lower() in analysis_text.lower()[:400]:
                confidence = c
                break

        result = {
            'symbol': symbol,
            'name': fundamentals['name'],
            'rating': rating,
            'confidence': confidence,
            'analysis': analysis_text,
            'newsCount': len(news_items),
            'yahooCount': len(yahoo_news),
            'googleCount': len([n for n in news_items if 'Google' in n.get('source', '')]),
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S'),
        }

        AI_ANALYSIS_CACHE[symbol] = {'result': result, 'ts': now}
        return jsonify(result)

    except anthropic.AuthenticationError:
        return jsonify({'error': 'AUTH_FAILED', 'message': 'Invalid Anthropic API key. Please check your key in Settings.'}), 401
    except anthropic.RateLimitError:
        return jsonify({'error': 'RATE_LIMIT', 'message': 'API rate limit reached. Please try again in a moment.'}), 429
    except Exception as e:
        return jsonify({'error': 'ANALYSIS_FAILED', 'message': str(e)}), 500


@app.route('/api/portfolio')
def get_portfolio():
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    ensure_user_portfolio(conn, uid)
    pid = get_portfolio_id(uid, request.args.get('portfolio_id'))
    sp = conn.execute('SELECT cash, realized_pnl FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    if sp:
        cash = sp['cash']
        realized_pnl = sp['realized_pnl'] or 0.0
    else:
        cash_row = conn.execute('SELECT cash FROM portfolio WHERE user_id = ?', (uid,)).fetchone()
        cash = cash_row['cash'] if cash_row else 1000000.0
        realized_pnl = 0.0
    fund_amount = get_fund_amount(conn, uid, pid)
    if fund_amount == 0:
        fund_amount = 1000000.0
    total_deposits, total_withdrawals = get_fund_totals(conn, uid, pid)
    net_deposits = total_deposits - total_withdrawals
    if net_deposits == 0:
        net_deposits = 1000000.0
    holdings = [dict(r) for r in conn.execute('SELECT * FROM holdings WHERE user_id = ? AND portfolio_id = ?', (uid, pid)).fetchall()]
    conn.close()
    return jsonify({'cash': round(cash, 2), 'realized_pnl': round(realized_pnl, 2), 'fund_amount': fund_amount,
                    'total_deposits': total_deposits, 'total_withdrawals': total_withdrawals, 'net_deposits': net_deposits, 'holdings': holdings})

@app.route('/api/transactions')
def get_transactions():
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    show_all = request.args.get('all', '').lower() in ('1', 'true')
    include_funds = request.args.get('include_funds', '').lower() in ('1', 'true')
    if show_all:
        if include_funds:
            txns = [dict(r) for r in conn.execute(
                '''SELECT t.id, t.symbol, t.name, t.action, t.shares, t.price, t.total, t.pnl, t.commission, t.timestamp, t.portfolio_id, sp.name AS portfolio_name, 'trade' as source
                FROM transactions t LEFT JOIN sub_portfolios sp ON t.portfolio_id = sp.id
                WHERE t.user_id = ?
                UNION ALL
                SELECT f.id, '' as symbol, COALESCE(f.note, f.type) as name, f.type as action, 0 as shares, 0 as price, f.amount as total, 0 as pnl, 0 as commission, f.timestamp, f.portfolio_id, sp2.name AS portfolio_name, 'fund' as source
                FROM fund_transactions f LEFT JOIN sub_portfolios sp2 ON f.portfolio_id = sp2.id
                WHERE f.user_id = ?
                ORDER BY 10 DESC, 1 DESC LIMIT 200''', (uid, uid)
            ).fetchall()]
        else:
            txns = [dict(r) for r in conn.execute(
                'SELECT t.*, sp.name AS portfolio_name FROM transactions t LEFT JOIN sub_portfolios sp ON t.portfolio_id = sp.id WHERE t.user_id = ? ORDER BY t.timestamp DESC, t.id DESC LIMIT 200', (uid,)
            ).fetchall()]
    else:
        pid = get_portfolio_id(uid, request.args.get('portfolio_id'))
        if include_funds:
            txns = [dict(r) for r in conn.execute(
                '''SELECT t.id, t.symbol, t.name, t.action, t.shares, t.price, t.total, t.pnl, t.commission, t.timestamp, t.portfolio_id, 'trade' as source
                FROM transactions t
                WHERE t.user_id = ? AND t.portfolio_id = ?
                UNION ALL
                SELECT f.id, '' as symbol, COALESCE(f.note, f.type) as name, f.type as action, 0 as shares, 0 as price, f.amount as total, 0 as pnl, 0 as commission, f.timestamp, f.portfolio_id, 'fund' as source
                FROM fund_transactions f
                WHERE f.user_id = ? AND f.portfolio_id = ?
                ORDER BY 10 DESC, 1 DESC LIMIT 50''', (uid, pid, uid, pid)
            ).fetchall()]
        else:
            txns = [dict(r) for r in conn.execute(
                'SELECT * FROM transactions WHERE user_id = ? AND portfolio_id = ? ORDER BY timestamp DESC, id DESC LIMIT 50', (uid, pid)
            ).fetchall()]
    conn.close()
    return jsonify(txns)


def _recalculate_portfolio(conn, uid, pid):
    """Replay all transactions for a portfolio to rebuild holdings, cash, and realized P&L."""
    fund = get_fund_amount(conn, uid, pid)
    if fund == 0:
        fund = 1000000.0

    # Clear holdings for this portfolio
    conn.execute('DELETE FROM holdings WHERE user_id = ? AND portfolio_id = ?', (uid, pid))

    # Replay all transactions in order
    txns = conn.execute(
        'SELECT id, symbol, name, action, shares, price, commission FROM transactions WHERE user_id = ? AND portfolio_id = ? ORDER BY timestamp ASC, id ASC',
        (uid, pid)
    ).fetchall()

    cash = fund
    realized_pnl = 0.0
    # Track holdings in memory: {symbol: {shares, avg_cost, name}}
    holdings = {}

    for tx in txns:
        txn_id, sym, name, action, shares, price = tx['id'], tx['symbol'], tx['name'], tx['action'], tx['shares'], tx['price']
        commission = tx['commission'] or 0.0
        total = round(shares * price, 2)
        txn_pnl = 0.0

        if action == 'buy':
            cash -= total + commission
            h = holdings.get(sym)
            if h:
                new_shares = h['shares'] + shares
                h['avg_cost'] = (h['avg_cost'] * h['shares'] + total) / new_shares
                h['shares'] = new_shares
            else:
                holdings[sym] = {'shares': shares, 'avg_cost': price, 'name': name}
        elif action == 'sell':
            h = holdings.get(sym)
            if h and h['shares'] >= shares:
                txn_pnl = round((price - h['avg_cost']) * shares - commission, 2)
                realized_pnl += txn_pnl
                cash += total - commission
                h['shares'] -= shares
                if h['shares'] < 0.001:
                    del holdings[sym]
            else:
                # Not enough shares — still record but skip P&L
                cash += total - commission

        conn.execute('UPDATE transactions SET pnl = ?, total = ? WHERE id = ?', (txn_pnl, total, txn_id))

    # Write holdings back
    for sym, h in holdings.items():
        conn.execute(
            'INSERT INTO holdings (user_id, symbol, name, shares, avg_cost, portfolio_id) VALUES (?, ?, ?, ?, ?, ?)',
            (uid, sym, h['name'], h['shares'], round(h['avg_cost'], 2), pid)
        )

    # Update cash and realized P&L
    conn.execute('UPDATE sub_portfolios SET cash = ?, realized_pnl = ? WHERE id = ? AND user_id = ?',
                 (round(cash, 2), round(realized_pnl, 2), pid, uid))
    conn.commit()


@app.route('/api/transactions/<int:txn_id>', methods=['PUT'])
def edit_transaction(txn_id):
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    tx = conn.execute('SELECT * FROM transactions WHERE id = ? AND user_id = ?', (txn_id, uid)).fetchone()
    if not tx:
        conn.close()
        return jsonify({'error': 'Transaction not found'}), 404

    data = request.json or {}
    shares = float(data.get('shares', tx['shares']))
    price = float(data.get('price', tx['price']))
    action = data.get('action', tx['action']).strip().lower()
    date = data.get('date', tx['timestamp']).strip()

    if shares <= 0 or price <= 0:
        conn.close()
        return jsonify({'error': 'Shares and price must be positive'}), 400
    if action not in ('buy', 'sell'):
        conn.close()
        return jsonify({'error': 'Action must be buy or sell'}), 400

    total = round(shares * price, 2)
    # Accept flat commission amount or percentage
    if 'commission' in data:
        commission = round(float(data['commission']), 2)
    elif 'commission_pct' in data:
        commission = round(total * float(data['commission_pct']) / 100, 2)
    else:
        commission = tx['commission'] or 0.0
    conn.execute('UPDATE transactions SET action = ?, shares = ?, price = ?, total = ?, commission = ?, timestamp = ? WHERE id = ?',
                 (action, shares, price, total, commission, date, txn_id))

    # Recalculate entire portfolio from scratch
    pid = tx['portfolio_id']
    _recalculate_portfolio(conn, uid, pid)
    conn.close()
    # Rebuild snapshots so performance chart reflects the change
    _backfill_snapshots_internal(uid, pid)
    return jsonify({'success': True})


@app.route('/api/transactions/<int:txn_id>', methods=['DELETE'])
def delete_transaction(txn_id):
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    tx = conn.execute('SELECT * FROM transactions WHERE id = ? AND user_id = ?', (txn_id, uid)).fetchone()
    if not tx:
        conn.close()
        return jsonify({'error': 'Transaction not found'}), 404

    pid = tx['portfolio_id']
    conn.execute('DELETE FROM transactions WHERE id = ?', (txn_id,))
    # Recalculate entire portfolio from scratch
    _recalculate_portfolio(conn, uid, pid)
    conn.close()
    # Rebuild snapshots so performance chart reflects the change
    _backfill_snapshots_internal(uid, pid)
    return jsonify({'success': True})


@app.route('/api/trade', methods=['POST'])
def trade():
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    symbol = data.get('symbol', '').strip()
    name = data.get('name', '').strip()
    action = data.get('action', '').strip()
    if not symbol or not name or action not in ('buy', 'sell'):
        return jsonify({'error': 'Invalid trade parameters'}), 400
    try:
        shares = float(data['shares'])
        price = float(data['price'])
    except (KeyError, ValueError, TypeError):
        return jsonify({'error': 'Invalid shares or price'}), 400
    if shares <= 0 or price <= 0:
        return jsonify({'error': 'Shares and price must be positive'}), 400
    total = round(shares * price, 2)
    commission_pct = float(data.get('commission_pct', 0))
    commission = round(total * commission_pct / 100, 2) if commission_pct > 0 else 0.0
    pid = get_portfolio_id(uid, data.get('portfolio_id'))

    # For US stocks, convert USD total to JPY for cash deduction
    usdjpy_rate = 1.0
    is_us = _is_us_symbol(symbol)
    if is_us:
        fx = PRICE_CACHE.get('USDJPY=X')
        usdjpy_rate = fx['data']['price'] if fx and fx['data'].get('price') else 150.0  # fallback
    total_jpy = round(total * usdjpy_rate, 2) if is_us else total
    commission_jpy = round(commission * usdjpy_rate, 2) if is_us else commission

    conn = get_db()
    ensure_user_portfolio(conn, uid)
    sp = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    if not sp:
        conn.close()
        return jsonify({'error': 'Portfolio not found'}), 404
    cash = sp['cash']

    txn_pnl = 0.0
    if action == 'buy':
        if cash < total_jpy + commission_jpy:
            conn.close()
            return jsonify({'error': 'Insufficient funds'}), 400
        cash -= total_jpy + commission_jpy
        conn.execute('UPDATE sub_portfolios SET cash = ? WHERE id = ?', (cash, pid))
        existing = conn.execute('SELECT * FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid)).fetchone()
        if existing:
            new_shares = existing['shares'] + shares
            new_avg = (existing['avg_cost'] * existing['shares'] + total) / new_shares
            conn.execute('UPDATE holdings SET shares = ?, avg_cost = ? WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                        (new_shares, new_avg, uid, symbol, pid))
        else:
            conn.execute('INSERT INTO holdings (user_id, symbol, name, shares, avg_cost, portfolio_id) VALUES (?, ?, ?, ?, ?, ?)',
                        (uid, symbol, name, shares, price, pid))
    elif action == 'sell':
        existing = conn.execute('SELECT * FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid)).fetchone()
        if not existing or existing['shares'] < shares:
            conn.close()
            return jsonify({'error': 'Insufficient shares'}), 400
        cash += total_jpy - commission_jpy
        # Realized P&L = (sell_price - avg_cost) * shares_sold - commission (converted to JPY for US)
        txn_pnl_native = round((price - existing['avg_cost']) * shares - commission, 2)
        txn_pnl = round(txn_pnl_native * usdjpy_rate, 2) if is_us else txn_pnl_native
        conn.execute('UPDATE sub_portfolios SET cash = ?, realized_pnl = COALESCE(realized_pnl, 0) + ? WHERE id = ?', (cash, txn_pnl, pid))
        new_shares = existing['shares'] - shares
        if new_shares < 0.001:
            conn.execute('DELETE FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid))
        else:
            conn.execute('UPDATE holdings SET shares = ? WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (new_shares, uid, symbol, pid))

    trade_date = data.get('date', '').strip() or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('INSERT INTO transactions (user_id, symbol, name, action, shares, price, total, pnl, commission, timestamp, portfolio_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (uid, symbol, name, action, shares, price, total, txn_pnl, commission, trade_date, pid))
    conn.commit()
    conn.close()
    # Rebuild snapshots so performance chart reflects the trade date accurately
    _backfill_snapshots_internal(uid, pid)
    return jsonify({'success': True, 'cash': round(cash, 2), 'commission': commission, 'usdjpy_rate': usdjpy_rate if is_us else None})

@app.route('/api/settings', methods=['GET'])
def get_settings():
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    rows = conn.execute('SELECT key, value FROM settings WHERE user_id = ?', (uid,)).fetchall()
    conn.close()
    return jsonify({r['key']: r['value'] for r in rows})

@app.route('/api/settings', methods=['POST'])
def save_settings():
    uid, err = get_auth_user()
    if err: return err
    data = request.json
    conn = get_db()
    for key, value in data.items():
        conn.execute('INSERT OR REPLACE INTO settings (user_id, key, value) VALUES (?, ?, ?)', (uid, key, str(value)))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

@app.route('/api/holdings/set', methods=['POST'])
def set_holding():
    uid, err = get_auth_user()
    if err: return err
    data = request.json
    symbol = data['symbol']
    name = data['name']
    shares = float(data['shares'])
    avg_cost = float(data['avg_cost'])
    pid = get_portfolio_id(uid, data.get('portfolio_id'))

    conn = get_db()
    ensure_user_portfolio(conn, uid)
    sp = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    cash = sp['cash'] if sp else 1000000.0

    existing = conn.execute('SELECT * FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid)).fetchone()
    old_value = existing['shares'] * existing['avg_cost'] if existing else 0.0
    new_value = shares * avg_cost if shares > 0 else 0.0

    # Adjust cash: return old position value, deduct new position value
    cash = cash + old_value - new_value
    conn.execute('UPDATE sub_portfolios SET cash = ? WHERE id = ?', (cash, pid))

    if shares <= 0:
        conn.execute('DELETE FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid))
    elif existing:
        conn.execute('UPDATE holdings SET shares = ?, avg_cost = ?, name = ? WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                    (shares, avg_cost, name, uid, symbol, pid))
    else:
        conn.execute('INSERT INTO holdings (user_id, symbol, name, shares, avg_cost, portfolio_id) VALUES (?, ?, ?, ?, ?, ?)',
                    (uid, symbol, name, shares, avg_cost, pid))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'cash': round(cash, 2)})

@app.route('/api/holdings/<path:symbol>', methods=['DELETE'])
def delete_holding(symbol):
    uid, err = get_auth_user()
    if err: return err
    pid = get_portfolio_id(uid, request.args.get('portfolio_id'))
    conn = get_db()
    # Return holding value back to cash
    existing = conn.execute('SELECT shares, avg_cost FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid)).fetchone()
    if existing:
        value = existing['shares'] * existing['avg_cost']
        conn.execute('UPDATE sub_portfolios SET cash = cash + ? WHERE id = ? AND user_id = ?', (value, pid, uid))
    conn.execute('DELETE FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

@app.route('/api/cash', methods=['POST'])
def set_cash():
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    try:
        amount = float(data['amount'])
    except (KeyError, ValueError, TypeError):
        return jsonify({'error': 'Invalid amount'}), 400
    if amount < 0:
        return jsonify({'error': 'Cash cannot be negative'}), 400
    pid = get_portfolio_id(uid, data.get('portfolio_id'))
    conn = get_db()
    ensure_user_portfolio(conn, uid)
    conn.execute('UPDATE sub_portfolios SET cash = ? WHERE id = ? AND user_id = ?', (amount, pid, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'cash': round(amount, 2)})


@app.route('/api/portfolios/<pid>/deposit', methods=['POST'])
def deposit_funds(pid):
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    try:
        amount = float(data.get('amount', 0))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid amount'}), 400
    if amount <= 0:
        return jsonify({'error': 'Amount must be positive'}), 400
    note = (data.get('note') or '').strip()
    conn = get_db()
    ensure_user_portfolio(conn, uid)
    sp = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    if not sp:
        conn.close()
        return jsonify({'error': 'Portfolio not found'}), 404
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('INSERT INTO fund_transactions (user_id, portfolio_id, type, amount, note, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                 (uid, pid, 'deposit', amount, note, now))
    conn.execute('UPDATE sub_portfolios SET cash = cash + ? WHERE id = ? AND user_id = ?', (amount, pid, uid))
    conn.commit()
    new_cash = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()['cash']
    conn.close()
    return jsonify({'success': True, 'cash': round(new_cash, 2), 'amount': amount})


@app.route('/api/portfolios/<pid>/withdraw', methods=['POST'])
def withdraw_funds(pid):
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    try:
        amount = float(data.get('amount', 0))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid amount'}), 400
    if amount <= 0:
        return jsonify({'error': 'Amount must be positive'}), 400
    note = (data.get('note') or '').strip()
    conn = get_db()
    ensure_user_portfolio(conn, uid)
    sp = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    if not sp:
        conn.close()
        return jsonify({'error': 'Portfolio not found'}), 404
    current_cash = sp['cash'] or 0
    if amount > current_cash:
        conn.close()
        return jsonify({'error': f'Insufficient cash. Available: ¥{current_cash:,.0f}'}), 400
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('INSERT INTO fund_transactions (user_id, portfolio_id, type, amount, note, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                 (uid, pid, 'withdrawal', amount, note, now))
    conn.execute('UPDATE sub_portfolios SET cash = cash - ? WHERE id = ? AND user_id = ?', (amount, pid, uid))
    conn.commit()
    new_cash = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()['cash']
    conn.close()
    return jsonify({'success': True, 'cash': round(new_cash, 2), 'amount': amount})


@app.route('/api/portfolios/<pid>/fund-history', methods=['GET'])
def fund_history(pid):
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    rows = conn.execute(
        'SELECT id, type, amount, note, timestamp FROM fund_transactions WHERE user_id = ? AND portfolio_id = ? ORDER BY timestamp DESC, id DESC',
        (uid, pid)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/fund-transactions/<int:fid>', methods=['PUT'])
def edit_fund_transaction(fid):
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    ft = conn.execute('SELECT * FROM fund_transactions WHERE id = ? AND user_id = ?', (fid, uid)).fetchone()
    if not ft:
        conn.close()
        return jsonify({'error': 'Fund transaction not found'}), 404

    data = request.json or {}
    try:
        amount = float(data.get('amount', ft['amount']))
    except (ValueError, TypeError):
        conn.close()
        return jsonify({'error': 'Invalid amount'}), 400
    if amount <= 0:
        conn.close()
        return jsonify({'error': 'Amount must be positive'}), 400

    txn_type = data.get('type', ft['type']).strip().lower()
    if txn_type not in ('deposit', 'withdrawal'):
        conn.close()
        return jsonify({'error': 'Type must be deposit or withdrawal'}), 400

    note = (data.get('note') if data.get('note') is not None else ft['note']) or ''
    date = str(data.get('date', ft['timestamp']) or ft['timestamp']).strip()

    conn.execute('UPDATE fund_transactions SET type = ?, amount = ?, note = ?, timestamp = ? WHERE id = ?',
                 (txn_type, amount, note.strip(), date, fid))
    conn.commit()

    pid = ft['portfolio_id']
    _recalculate_portfolio(conn, uid, pid)
    conn.close()
    # Rebuild snapshots so performance chart reflects the change
    _backfill_snapshots_internal(uid, pid)
    return jsonify({'success': True})


@app.route('/api/fund-transactions/<int:fid>', methods=['DELETE'])
def delete_fund_transaction(fid):
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    ft = conn.execute('SELECT * FROM fund_transactions WHERE id = ? AND user_id = ?', (fid, uid)).fetchone()
    if not ft:
        conn.close()
        return jsonify({'error': 'Fund transaction not found'}), 404

    pid = ft['portfolio_id']
    conn.execute('DELETE FROM fund_transactions WHERE id = ?', (fid,))
    _recalculate_portfolio(conn, uid, pid)
    conn.close()
    # Rebuild snapshots so performance chart reflects the change
    _backfill_snapshots_internal(uid, pid)
    return jsonify({'success': True})


@app.route('/api/reset', methods=['POST'])
def reset_portfolio():
    uid, err = get_auth_user()
    if err: return err
    pid = get_portfolio_id(uid, request.args.get('portfolio_id') or (request.json or {}).get('portfolio_id'))
    conn = get_db()
    # Use fund_amount from fund_transactions
    fund = get_fund_amount(conn, uid, pid)
    if fund == 0:
        fund = 1000000.0
    conn.execute('UPDATE sub_portfolios SET cash = ?, realized_pnl = 0 WHERE id = ? AND user_id = ?', (fund, pid, uid))
    conn.execute('DELETE FROM holdings WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    conn.execute('DELETE FROM transactions WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    # Keep fund_transactions — only clear trade transactions
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'cash': fund})

@app.route('/api/portfolios', methods=['GET'])
def list_portfolios():
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    ensure_user_portfolio(conn, uid)
    rows = conn.execute('SELECT id, name, cash, created_at FROM sub_portfolios WHERE user_id = ? ORDER BY created_at', (uid,)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['fund_amount'] = get_fund_amount(conn, uid, d['id'])
        if d['fund_amount'] == 0:
            d['fund_amount'] = 1000000.0
        result.append(d)
    conn.close()
    return jsonify(result)


@app.route('/api/portfolios', methods=['POST'])
def create_portfolio():
    uid, err = get_auth_user()
    if err: return err
    data = request.json
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name is required'}), 400
    conn = get_db()
    ensure_user_portfolio(conn, uid)
    # Limit to 10 portfolios
    count = conn.execute('SELECT COUNT(*) FROM sub_portfolios WHERE user_id = ?', (uid,)).fetchone()[0]
    if count >= 10:
        conn.close()
        return jsonify({'error': 'Maximum 10 portfolios'}), 400
    # Use provided initial_deposit / fund_amount, or fall back to user setting
    fund = None
    if data.get('initial_deposit') is not None:
        try:
            fund = float(data['initial_deposit'])
        except (ValueError, TypeError):
            pass
    if fund is None and data.get('fund_amount') is not None:
        try:
            fund = float(data['fund_amount'])
        except (ValueError, TypeError):
            pass
    if fund is None or fund <= 0:
        fund_row = conn.execute("SELECT value FROM settings WHERE user_id = ? AND key = 'fund_amount'", (uid,)).fetchone()
        fund = float(fund_row['value']) if fund_row else 1000000.0
    pid = str(uuid.uuid4())
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('INSERT INTO sub_portfolios (id, user_id, name, cash, fund_amount, created_at) VALUES (?, ?, ?, ?, ?, ?)',
                 (pid, uid, name, fund, fund, now))
    # Insert initial deposit into fund_transactions
    conn.execute('INSERT INTO fund_transactions (user_id, portfolio_id, type, amount, note, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                 (uid, pid, 'deposit', fund, 'Initial deposit', now))
    conn.commit()
    conn.close()
    return jsonify({'id': pid, 'name': name, 'cash': fund, 'fund_amount': fund})


@app.route('/api/portfolios/<pid>', methods=['PUT'])
def rename_portfolio(pid):
    uid, err = get_auth_user()
    if err: return err
    data = request.json
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name is required'}), 400
    conn = get_db()
    conn.execute('UPDATE sub_portfolios SET name = ? WHERE id = ? AND user_id = ?', (name, pid, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/portfolios/<pid>/fund', methods=['POST'])
def set_portfolio_fund(pid):
    uid, err = get_auth_user()
    if err: return err
    data = request.json
    try:
        amount = float(data.get('fund_amount', 0))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid amount'}), 400
    if amount <= 0:
        return jsonify({'error': 'Amount must be positive'}), 400
    conn = get_db()
    conn.execute('UPDATE sub_portfolios SET fund_amount = ? WHERE id = ? AND user_id = ?', (amount, pid, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'fund_amount': amount})


@app.route('/api/portfolios/<pid>', methods=['DELETE'])
def delete_portfolio(pid):
    uid, err = get_auth_user()
    if err: return err
    main_id = f'main_{uid}'
    if pid == main_id:
        return jsonify({'error': "Can't delete the main portfolio"}), 400
    conn = get_db()
    conn.execute('DELETE FROM holdings WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    conn.execute('DELETE FROM transactions WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    conn.execute('DELETE FROM fund_transactions WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    conn.execute('DELETE FROM portfolio_snapshots WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    conn.execute('DELETE FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/import-transaction', methods=['POST'])
def import_transaction():
    uid, err = get_auth_user()
    if err: return err
    data = request.json
    symbol = data.get('symbol', '').strip()
    name = data.get('name', symbol)
    action = data.get('action', '').lower()
    shares = float(data.get('shares', 0))
    price = float(data.get('price', 0))
    date = data.get('date', '').strip() or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    pid = get_portfolio_id(uid, data.get('portfolio_id'))

    if not symbol or action not in ('buy', 'sell') or shares <= 0 or price <= 0:
        return jsonify({'error': 'Invalid parameters'}), 400

    total = round(shares * price, 2)
    commission_pct = float(data.get('commission_pct', 0))
    commission = round(total * commission_pct / 100, 2) if commission_pct > 0 else 0.0
    conn = get_db()
    ensure_user_portfolio(conn, uid)
    sp = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    if not sp:
        conn.close()
        return jsonify({'error': 'Portfolio not found'}), 404
    cash = sp['cash']

    txn_pnl = 0.0
    if action == 'buy':
        cash -= total + commission
        conn.execute('UPDATE sub_portfolios SET cash = ? WHERE id = ?', (cash, pid))
        existing = conn.execute('SELECT * FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid)).fetchone()
        if existing:
            new_shares = existing['shares'] + shares
            new_avg = (existing['avg_cost'] * existing['shares'] + total) / new_shares
            conn.execute('UPDATE holdings SET shares = ?, avg_cost = ? WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                        (new_shares, new_avg, uid, symbol, pid))
        else:
            conn.execute('INSERT INTO holdings (user_id, symbol, name, shares, avg_cost, portfolio_id) VALUES (?, ?, ?, ?, ?, ?)',
                        (uid, symbol, name, shares, price, pid))
    elif action == 'sell':
        existing = conn.execute('SELECT * FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid)).fetchone()
        if not existing or existing['shares'] < shares:
            conn.close()
            return jsonify({'error': 'Insufficient shares'}), 400
        cash += total - commission
        # Realized P&L = (sell_price - avg_cost) * shares_sold - commission
        txn_pnl = round((price - existing['avg_cost']) * shares - commission, 2)
        conn.execute('UPDATE sub_portfolios SET cash = ?, realized_pnl = COALESCE(realized_pnl, 0) + ? WHERE id = ?', (cash, txn_pnl, pid))
        new_shares = existing['shares'] - shares
        if new_shares < 0.001:
            conn.execute('DELETE FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid))
        else:
            conn.execute('UPDATE holdings SET shares = ? WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (new_shares, uid, symbol, pid))

    conn.execute('INSERT INTO transactions (user_id, symbol, name, action, shares, price, total, pnl, commission, timestamp, portfolio_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (uid, symbol, name, action, shares, price, total, txn_pnl, commission, date, pid))
    conn.commit()
    conn.close()
    # Rebuild snapshots so performance chart reflects the import
    _backfill_snapshots_internal(uid, pid)
    return jsonify({'success': True, 'cash': round(cash, 2), 'commission': commission})


# ── Price Alerts ──────────────────────────────────────────────────

@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    uid, err = get_auth_user()
    if err: return err
    symbol = request.args.get('symbol', '').strip()
    conn = get_db()
    if symbol:
        rows = conn.execute('SELECT * FROM price_alerts WHERE user_id = ? AND symbol = ? ORDER BY created_at DESC', (uid, symbol)).fetchall()
    else:
        rows = conn.execute('SELECT * FROM price_alerts WHERE user_id = ? ORDER BY created_at DESC', (uid,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/alerts', methods=['POST'])
def create_alert():
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    symbol = data.get('symbol', '').strip()
    name = data.get('name', '').strip()
    try:
        ref_price = float(data['reference_price'])
    except (KeyError, ValueError, TypeError):
        return jsonify({'error': 'Invalid reference price'}), 400
    up_pct = data.get('up_pct')
    down_pct = data.get('down_pct')
    if up_pct is not None:
        try: up_pct = float(up_pct)
        except (ValueError, TypeError): up_pct = None
    if down_pct is not None:
        try: down_pct = float(down_pct)
        except (ValueError, TypeError): down_pct = None
    if not up_pct and not down_pct:
        return jsonify({'error': 'Set at least one threshold (up or down)'}), 400
    if not symbol or ref_price <= 0:
        return jsonify({'error': 'Invalid symbol or reference price'}), 400

    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT INTO price_alerts (user_id, symbol, name, reference_price, up_pct, down_pct, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
              (uid, symbol, name, ref_price, up_pct, down_pct, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    alert_id = c.lastrowid
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'id': alert_id})


@app.route('/api/alerts/<int:alert_id>', methods=['DELETE'])
def delete_alert(alert_id):
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    conn.execute('DELETE FROM price_alerts WHERE id = ? AND user_id = ?', (alert_id, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/alerts/<int:alert_id>/trigger', methods=['POST'])
def trigger_alert(alert_id):
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    conn.execute('UPDATE price_alerts SET triggered = 1 WHERE id = ? AND user_id = ?', (alert_id, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ── Portfolio Snapshots (for performance chart) ──────────────────
@app.route('/api/portfolio-history')
def portfolio_history():
    """Return daily portfolio value snapshots for charting.
    Auto-backfills if transactions exist before earliest snapshot."""
    uid, err = get_auth_user()
    if err: return err
    pid = get_portfolio_id(uid, request.args.get('portfolio_id'))
    conn = get_db()

    # Check if backfill is needed: transactions or fund_transactions exist before earliest snapshot
    earliest_snap = conn.execute(
        'SELECT MIN(date) as d FROM portfolio_snapshots WHERE user_id = ? AND portfolio_id = ?',
        (uid, pid)
    ).fetchone()
    earliest_txn = conn.execute(
        'SELECT MIN(timestamp) as d FROM transactions WHERE user_id = ? AND portfolio_id = ?',
        (uid, pid)
    ).fetchone()
    earliest_fund = conn.execute(
        'SELECT MIN(timestamp) as d FROM fund_transactions WHERE user_id = ? AND portfolio_id = ?',
        (uid, pid)
    ).fetchone()
    conn.close()

    snap_date = earliest_snap['d'] if earliest_snap else None
    txn_date = earliest_txn['d'][:10] if earliest_txn and earliest_txn['d'] else None
    fund_date = earliest_fund['d'][:10] if earliest_fund and earliest_fund['d'] else None
    earliest_activity = min(d for d in [txn_date, fund_date] if d) if (txn_date or fund_date) else None

    if earliest_activity and (not snap_date or earliest_activity < snap_date):
        _backfill_snapshots_internal(uid, pid)

    conn = get_db()
    rows = conn.execute(
        'SELECT date, total_value, cash, invested, net_deposits FROM portfolio_snapshots WHERE user_id = ? AND portfolio_id = ? ORDER BY date ASC',
        (uid, pid)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/portfolio-snapshot', methods=['POST'])
def save_portfolio_snapshot():
    """Save today's portfolio value snapshot (called by frontend daily)."""
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    pid = get_portfolio_id(uid, data.get('portfolio_id'))
    total_value = float(data.get('total_value', 0))
    cash = float(data.get('cash', 0))
    invested = float(data.get('invested', 0))
    net_deposits = float(data.get('net_deposits', 0))
    today = datetime.now().strftime('%Y-%m-%d')
    conn = get_db()
    conn.execute('''INSERT OR REPLACE INTO portfolio_snapshots (user_id, portfolio_id, date, total_value, cash, invested, net_deposits)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''', (uid, pid, today, total_value, cash, invested, net_deposits))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


def _backfill_snapshots_internal(uid, pid):
    """Rebuild portfolio snapshots from transaction history.
    Replays fund transactions and trade transactions day-by-day to compute historical portfolio values."""
    conn = get_db()

    txns = conn.execute(
        'SELECT symbol, name, action, shares, price, commission, timestamp FROM transactions '
        'WHERE user_id = ? AND portfolio_id = ? ORDER BY timestamp ASC, id ASC',
        (uid, pid)
    ).fetchall()

    fund_txns = conn.execute(
        'SELECT type, amount, note, timestamp FROM fund_transactions '
        'WHERE user_id = ? AND portfolio_id = ? ORDER BY timestamp ASC, id ASC',
        (uid, pid)
    ).fetchall()

    if not txns and not fund_txns:
        conn.close()
        return 0

    from datetime import timedelta as td

    # Determine first_date from earliest of trade or fund transactions
    first_trade = txns[0]['timestamp'][:10] if txns else None
    first_fund = fund_txns[0]['timestamp'][:10] if fund_txns else None
    if first_trade and first_fund:
        first_date = min(first_trade, first_fund)
    else:
        first_date = first_trade or first_fund
    today = datetime.now().strftime('%Y-%m-%d')

    txn_by_date = {}
    for tx in txns:
        d = tx['timestamp'][:10]
        txn_by_date.setdefault(d, []).append(tx)

    fund_by_date = {}
    for ft in fund_txns:
        d = ft['timestamp'][:10]
        fund_by_date.setdefault(d, []).append(ft)

    # If trades exist before the first fund deposit, seed cash with fund_amount
    # so the portfolio makes financial sense from day 1.
    sp_row = conn.execute('SELECT fund_amount FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    starting_fund = sp_row['fund_amount'] if sp_row and sp_row['fund_amount'] else 0

    # Check if we need to re-anchor the initial deposit
    first_fund_date = fund_txns[0]['timestamp'][:10] if fund_txns else None
    seeded_early = False
    if first_trade and (not first_fund_date or first_trade < first_fund_date) and starting_fund > 0:
        # Trades exist before any fund deposit — inject starting fund on first_trade date
        cash = starting_fund
        net_deposits = starting_fund
        seeded_early = True
        # Remove the initial deposit to avoid double-counting (handles both "migrated" and "Initial deposit" notes)
        for d_key in list(fund_by_date.keys()):
            fund_by_date[d_key] = [ft for ft in fund_by_date[d_key]
                                   if not (ft['type'] == 'deposit' and ft['amount'] == starting_fund
                                           and ('migrated' in (ft['note'] or '').lower() or 'initial' in (ft['note'] or '').lower()))]
            if not fund_by_date[d_key]:
                del fund_by_date[d_key]
    else:
        cash = 0
        net_deposits = 0

    holdings = {}
    snapshots = []
    last_known_price = {}

    current = datetime.strptime(first_date, '%Y-%m-%d')
    end = datetime.strptime(today, '%Y-%m-%d')

    while current <= end:
        d = current.strftime('%Y-%m-%d')

        # Process fund transactions first (deposits/withdrawals)
        for ft in fund_by_date.get(d, []):
            if ft['type'] == 'deposit':
                cash += ft['amount']
                net_deposits += ft['amount']
            else:
                cash -= ft['amount']
                net_deposits -= ft['amount']

        # Process trade transactions
        for tx in txn_by_date.get(d, []):
            sym = tx['symbol']
            shares = tx['shares']
            price = tx['price']
            commission = tx['commission'] or 0.0
            last_known_price[sym] = price

            if tx['action'] == 'buy':
                cash -= (shares * price + commission)
                if sym in holdings:
                    h = holdings[sym]
                    old_total = h['shares'] * h['avg_cost']
                    h['shares'] += shares
                    h['avg_cost'] = (old_total + shares * price) / h['shares'] if h['shares'] else 0
                else:
                    holdings[sym] = {'shares': shares, 'avg_cost': price, 'name': tx['name']}
            elif tx['action'] == 'sell':
                cash += (shares * price - commission)
                if sym in holdings:
                    h = holdings[sym]
                    h['shares'] -= shares
                    if h['shares'] <= 0.001:
                        del holdings[sym]

        invested = sum(h['shares'] * last_known_price.get(sym, h['avg_cost']) for sym, h in holdings.items())
        total_value = cash + invested
        snapshots.append((uid, pid, d, round(total_value, 2), round(cash, 2), round(invested, 2), round(net_deposits, 2)))
        current += td(days=1)

    conn.execute('DELETE FROM portfolio_snapshots WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    conn.executemany(
        'INSERT INTO portfolio_snapshots (user_id, portfolio_id, date, total_value, cash, invested, net_deposits) VALUES (?, ?, ?, ?, ?, ?, ?)',
        snapshots
    )
    conn.commit()
    conn.close()
    return len(snapshots)


@app.route('/api/backfill-snapshots', methods=['POST'])
def backfill_snapshots():
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    pid = get_portfolio_id(uid, data.get('portfolio_id'))
    n = _backfill_snapshots_internal(uid, pid)
    return jsonify({'success': True, 'snapshots': n})


# ── Batch Import (CSV) ───────────────────────────────────────────
@app.route('/api/import-csv', methods=['POST'])
def import_csv():
    """Import multiple transactions from CSV text data."""
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    pid = get_portfolio_id(uid, data.get('portfolio_id'))
    rows = data.get('rows', [])
    if not rows:
        return jsonify({'error': 'No rows provided'}), 400

    conn = get_db()
    ensure_user_portfolio(conn, uid)
    imported = 0
    errors = []

    # Commission % applies to all rows in this batch
    commission_pct = float(data.get('commission_pct', 0))

    for i, row in enumerate(rows):
        try:
            symbol = row.get('symbol', '').strip()
            name = row.get('name', symbol)
            action = row.get('action', '').strip().lower()
            shares = float(row.get('shares', 0))
            price = float(row.get('price', 0))
            date = row.get('date', '').strip() or datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if not symbol or action not in ('buy', 'sell') or shares <= 0 or price <= 0:
                errors.append(f"Row {i+1}: invalid data")
                continue

            total = round(shares * price, 2)
            commission = round(total * commission_pct / 100, 2) if commission_pct > 0 else 0.0
            conn.execute(
                'INSERT INTO transactions (user_id, symbol, name, action, shares, price, total, pnl, commission, timestamp, portfolio_id) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)',
                (uid, symbol, name, action, shares, price, total, commission, date, pid)
            )
            imported += 1
        except Exception as e:
            errors.append(f"Row {i+1}: {str(e)}")

    # Recalculate portfolio from all transactions
    _recalculate_portfolio(conn, uid, pid)
    conn.close()
    # Backfill snapshots so the performance chart shows historical data
    _backfill_snapshots_internal(uid, pid)
    return jsonify({'success': True, 'imported': imported, 'errors': errors})


# ── Watchlist ────────────────────────────────────────────────────
@app.route('/api/watchlist', methods=['GET'])
def get_watchlist():
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    rows = conn.execute('SELECT symbol, name, added_at FROM watchlist WHERE user_id = ? ORDER BY added_at DESC', (uid,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/watchlist', methods=['POST'])
def add_watchlist():
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    symbol = data.get('symbol', '').strip()
    name = data.get('name', '').strip()
    if not symbol:
        return jsonify({'error': 'Symbol required'}), 400
    conn = get_db()
    conn.execute('INSERT OR IGNORE INTO watchlist (user_id, symbol, name, added_at) VALUES (?, ?, ?, ?)',
                 (uid, symbol, name, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/watchlist/<path:symbol>', methods=['DELETE'])
def remove_watchlist(symbol):
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    conn.execute('DELETE FROM watchlist WHERE user_id = ? AND symbol = ?', (uid, symbol))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


def _fix_initial_deposit_timestamps():
    """One-time fix: ensure 'Initial deposit' fund transactions have a timestamp
    earlier than all trade transactions so they sort to the bottom of history."""
    try:
        conn = get_db()
        initials = conn.execute(
            "SELECT f.id, f.user_id, f.portfolio_id, f.timestamp FROM fund_transactions f WHERE f.note = 'Initial deposit'"
        ).fetchall()
        for ft in initials:
            earliest = conn.execute(
                'SELECT MIN(timestamp) as ts FROM transactions WHERE user_id = ? AND portfolio_id = ?',
                (ft['user_id'], ft['portfolio_id'])
            ).fetchone()
            if earliest and earliest['ts'] and earliest['ts'] < ft['timestamp']:
                try:
                    from datetime import timedelta
                    dt = datetime.strptime(earliest['ts'][:19], '%Y-%m-%d %H:%M:%S')
                    new_ts = (dt - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
                    conn.execute('UPDATE fund_transactions SET timestamp = ? WHERE id = ?', (new_ts, ft['id']))
                except Exception:
                    pass
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'[fix] Initial deposit timestamp fix: {e}', flush=True)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    _fix_initial_deposit_timestamps()  # Fix initial deposit ordering
    _load_stock_info_cache()  # Load persisted stock info immediately
    # Pre-warm caches for portfolio holdings, watchlist, and indices (runs first)
    threading.Thread(target=_prefetch_background, daemon=True).start()
    # Auto-refresh stock list from JPX every 6 hours (detects new listings / delistings)
    threading.Thread(target=_stock_list_auto_refresh, daemon=True).start()
    # Auto-refresh US stock list from Finnhub every 24 hours
    if FINNHUB_API_KEY:
        threading.Thread(target=_us_stock_list_auto_refresh, daemon=True).start()
    app.run(debug=False, port=port, threaded=True)
