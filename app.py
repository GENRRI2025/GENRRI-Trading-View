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

try:
    from kabu import KabuClient, KabuError
    import kabu_ws
    HAS_KABU = True
except ImportError:
    HAS_KABU = False
    class KabuError(Exception):
        def __init__(self, code='', message=''):
            super().__init__(message); self.code = code; self.message = message


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
FINNHUB_API_KEY = os.environ.get('FINNHUB_API_KEY', 'd77gag1r01qp6afli200d77gag1r01qp6afli20g')
_FINNHUB_BASE = 'https://finnhub.io/api/v1'
_finnhub_call_times = []  # timestamps of recent calls for rate limiting
_FINNHUB_RATE_LIMIT = 55  # stay under 60/min
_US_STOCK_REFRESH_INTERVAL = 24 * 3600  # 24 hours
_last_us_stock_refresh = None

# ── Kabu Station integration (real-time JP quotes + live trading) ──
KABU_API_PASSWORD = os.environ.get('KABU_API_PASSWORD', '')
KABU_ORDER_PASSWORD = os.environ.get('KABU_ORDER_PASSWORD', '')
_kabu_client = None  # lazy singleton
KABU_PRICE_CACHE_TTL = 10  # 10 seconds for real-time data


def _get_kabu_client():
    """Get or create the Kabu Station client singleton."""
    global _kabu_client
    if not HAS_KABU:
        return None
    if _kabu_client is None:
        _kabu_client = KabuClient()
        if KABU_API_PASSWORD:
            _kabu_client.set_passwords(KABU_API_PASSWORD, KABU_ORDER_PASSWORD)
            try:
                _kabu_client.authenticate()
                kabu_ws.start()
                print('[kabu] Auto-connected via env var', flush=True)
                # Register watchlist symbols in background
                threading.Thread(target=_register_kabu_watchlist, daemon=True).start()
            except Exception as e:
                print(f'[kabu] Auto-connect failed: {e}', flush=True)
    return _kabu_client


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
        if item.get('type') not in ('Common Stock', 'ADR'):
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

    import tempfile
    tmp = os.path.join(tempfile.gettempdir(), 'jpx_listed.xls')
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


def _detect_live_cash_flow(conn, uid, pid, current_total, total_pnl, realized_pnl):
    """Detect deposits/withdrawals for live portfolio by comparing expected vs actual account value.
    Kabu Station has no deposit/withdrawal API, so we infer cash flows from value changes.
    """
    sp = conn.execute('SELECT fund_amount FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    if not sp or not float(sp['fund_amount'] or 0):
        return 0  # No baseline yet (first sync hasn't happened)
    baseline = float(sp['fund_amount'])
    # Expected total = baseline + unrealized P/L + realized P/L
    expected_total = baseline + total_pnl + realized_pnl
    cash_flow = current_total - expected_total
    if abs(cash_flow) > 1000:  # Threshold ¥1000 to avoid rounding noise
        flow_type = 'deposit' if cash_flow > 0 else 'withdrawal'
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(
            'INSERT INTO fund_transactions (user_id, portfolio_id, type, amount, timestamp) VALUES (?, ?, ?, ?, ?)',
            (uid, pid, flow_type, round(abs(cash_flow), 2), now)
        )
        # Update baseline so future checks don't re-detect the same flow
        new_baseline = baseline + cash_flow
        conn.execute('UPDATE sub_portfolios SET fund_amount = ? WHERE id = ? AND user_id = ?',
                     (round(new_baseline, 2), pid, uid))
        return round(cash_flow, 2)
    return 0


@app.route('/')
def index():
    resp = send_from_directory('static', 'index.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp


@app.route('/legacy')
def legacy_index():
    """Serve the old dual-system UI for reference / fallback during rebuild.
    Some features (full chart drawing tools, CSV import, Kabu full UI, etc.)
    still live here until sessions 2-3 port them to the new index.html."""
    resp = send_from_directory('static', 'index.legacy.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
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


@app.route('/api/admin/reset-password', methods=['POST'])
def admin_reset_password():
    """Reset a user's password. Requires invite code for authentication."""
    data = request.json or {}
    invite_code = (data.get('invite_code') or '').strip()
    expected_code = os.environ.get('INVITE_CODE', 'GENRRI')
    if not expected_code or invite_code != expected_code:
        return jsonify({'error': 'Invalid invite code'}), 403
    username = (data.get('username') or '').strip()
    new_password = (data.get('new_password') or '').strip()
    if not username or not new_password:
        return jsonify({'error': 'Username and new_password are required'}), 400
    if len(new_password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    conn = get_db()
    user = conn.execute('SELECT id FROM users WHERE name = ?', (username,)).fetchone()
    if not user:
        conn.close()
        return jsonify({'error': 'User not found'}), 404
    password_hash, salt = hash_password(new_password)
    conn.execute('UPDATE users SET password_hash = ?, salt = ? WHERE name = ?', (password_hash, salt, username))
    conn.execute('DELETE FROM sessions WHERE user_id = ?', (user['id'],))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'message': f'Password reset for {username}'})


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

    # Accept both ?q= and ?search= as the search term (frontend compatibility)
    q      = (request.args.get('q') or request.args.get('search') or '').strip()
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
        where.append("(UPPER(name_jp) LIKE UPPER(?) OR UPPER(symbol) LIKE UPPER(?) OR UPPER(code) LIKE UPPER(?))")
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
        'volume': 0,
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

    # Try Kabu Station for JP stocks (real-time via PUSH or REST)
    if not _is_us_symbol(symbol) and HAS_KABU:
        kabu = _get_kabu_client()
        if kabu and kabu.is_connected():
            # Check PUSH data first (instant, no API call)
            code, _ = KabuClient.to_kabu_symbol(symbol)
            push = kabu_ws.get_push_data(code)
            if push and push.get('CurrentPrice'):
                result = {
                    'symbol': symbol,
                    'price': round(float(push['CurrentPrice']), 2),
                    'change': round(float(push.get('ChangePreviousClose') or 0), 2),
                    'change_pct': round(float(push.get('ChangePreviousClosePer') or 0), 2),
                    'prev_close': round(float(push.get('PreviousClose') or 0), 2),
                    'volume': int(push.get('TradingVolume') or 0),
                    'source': 'kabu_push',
                    'chart': []
                }
                PRICE_CACHE[symbol] = {'data': result, 'ts': now}
                return result
            # Fallback to REST board
            board_quote = kabu.get_board_as_quote(symbol)
            if 'error' not in board_quote and board_quote.get('price'):
                PRICE_CACHE[symbol] = {'data': board_quote, 'ts': now}
                return board_quote

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

        vol = 0
        try:
            vol = int(info.last_volume) if hasattr(info, 'last_volume') and info.last_volume else 0
        except Exception:
            pass

        result = {
            'symbol': symbol,
            'price': round(price, 2),
            'change': round(change, 2),
            'change_pct': round(change_pct, 2),
            'prev_close': round(prev_close, 2),
            'volume': vol,
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

    # Fetch US symbols via Finnhub (real-time, parallel calls with rate limiting)
    if us_to_fetch and FINNHUB_API_KEY:
        from concurrent.futures import ThreadPoolExecutor
        batch = us_to_fetch[:50]  # cap at 50 to stay within rate limit
        def _fh_fetch(sym):
            try:
                return sym, _fetch_finnhub_quote(sym)
            except Exception:
                return sym, None
        with ThreadPoolExecutor(max_workers=10) as ex:
            for sym, fh in ex.map(_fh_fetch, batch):
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
                        vol = 0
                        try:
                            vols = sym_df['Volume'].dropna()
                            if len(vols) > 0:
                                vol = int(vols.iloc[-1])
                        except Exception:
                            pass
                        result = {
                            'symbol': sym,
                            'price': price,
                            'change': change,
                            'change_pct': change_pct,
                            'prev_close': prev,
                            'volume': vol,
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


# ── Sparkline data (5-day close prices for mini-charts) ──────────
SPARK_CACHE = {}  # { symbol: { 'data': [float,...], 'ts': datetime } }
SPARK_CACHE_TTL = 3600  # 1 hour

@app.route('/api/sparklines')
def get_sparklines():
    """Return 5-day close prices for mini sparkline charts."""
    symbols_str = request.args.get('symbols', '')
    symbols = [s.strip() for s in symbols_str.split(',') if s.strip()][:30]
    if not symbols:
        return jsonify({})

    now = datetime.now()
    results = {}
    to_fetch = []

    for sym in symbols:
        cached = SPARK_CACHE.get(sym)
        if cached and (now - cached['ts']).total_seconds() < SPARK_CACHE_TTL:
            results[sym] = cached['data']
        else:
            to_fetch.append(sym)

    if to_fetch:
        try:
            import pandas as pd
            tickers_str = ' '.join(to_fetch)
            df = yf.download(tickers_str, period='5d', interval='1h',
                           group_by='ticker', progress=False, threads=True)
            for sym in to_fetch:
                try:
                    if len(to_fetch) == 1:
                        sym_df = df
                    else:
                        sym_df = df[sym] if sym in df.columns.get_level_values(0) else None
                    if sym_df is not None and not sym_df.empty:
                        closes = sym_df['Close'].dropna().tolist()
                        closes = [round(float(c), 2) for c in closes]
                        if len(closes) > 1:
                            results[sym] = closes
                            SPARK_CACHE[sym] = {'data': closes, 'ts': now}
                except Exception:
                    pass
        except Exception:
            pass

    return jsonify(results)



# ── Chart data cache ─────────────────────────────────────────────
CHART_CACHE = {}        # { 'symbol:key': { 'data': [...], 'ts': datetime } }
CHART_CACHE_TTL_INTRADAY = 300     # 5 minutes for intraday charts (yfinance ~15min delay, refresh often)
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


INTERVAL_SECONDS = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '60m': 3600}

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

    # Append real-time Kabu Station candles to fill the gap after delayed yfinance data
    if is_intraday and data and not _is_us_symbol(symbol) and HAS_KABU:
        try:
            kabu = _get_kabu_client()
            if kabu and kabu.is_connected():
                code, _ = KabuClient.to_kabu_symbol(symbol)
                # Determine interval in seconds
                active_interval = interval_req or {'1d': '1m', '5d': '5m', '1mo': '60m'}.get(period, '5m')
                int_sec = INTERVAL_SECONDS.get(active_interval, 300)
                # Get last yfinance candle time
                last_yf_time = data[-1]['time'] if data else 0
                # Build candles from collected tick data
                kabu_candles = kabu_ws.build_candles(code, interval_sec=int_sec, after_ts=last_yf_time)
                if kabu_candles:
                    data = data + kabu_candles
        except Exception as e:
            print(f'[chart] Kabu candle append error: {e}', flush=True)

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


# ═══════════════════════════════════════════════════════════════════
# ═══ FINANCIAL STATEMENTS / ANALYST / CORPORATE ACTIONS / HOLDERS ═══
# ═══════════════════════════════════════════════════════════════════
# All backed by yfinance. Heavily cached (60 min) since fundamentals
# only change quarterly. Disk persistence would be nice-to-have later.
FUNDAMENTALS_CACHE = {}       # { (kind, symbol): { data, ts } }
FUNDAMENTALS_CACHE_TTL = 3600  # 60 minutes

def _cached_fundamentals(kind, symbol, builder):
    """Generic cache wrapper for fundamentals endpoints."""
    now = datetime.now()
    key = (kind, symbol)
    cached = FUNDAMENTALS_CACHE.get(key)
    if cached and (now - cached['ts']).total_seconds() < FUNDAMENTALS_CACHE_TTL:
        return cached['data']
    try:
        data = builder()
        FUNDAMENTALS_CACHE[key] = {'data': data, 'ts': now}
        return data
    except Exception as e:
        if cached:
            return cached['data']  # stale-on-error
        raise

def _df_to_periods(df, max_periods=8):
    """Convert a yfinance financials DataFrame (rows=line items, cols=dates)
    into [{period:'YYYY-MM-DD', values:{line_item: number, ...}}, ...].
    Newest first, limited to max_periods.
    """
    if df is None or df.empty:
        return []
    out = []
    # Columns are Timestamps, newest first in yfinance
    cols = list(df.columns)[:max_periods]
    for col in cols:
        period = col.strftime('%Y-%m-%d') if hasattr(col, 'strftime') else str(col)
        values = {}
        for idx in df.index:
            v = df.at[idx, col]
            try:
                if v is None:
                    continue
                # pandas NaN check
                import math
                fv = float(v)
                if math.isnan(fv) or math.isinf(fv):
                    continue
                values[str(idx)] = fv
            except Exception:
                continue
        out.append({'period': period, 'values': values})
    return out


@app.route('/api/financials/<symbol>')
def get_financials(symbol):
    """Income Statement / Balance Sheet / Cash Flow — quarterly + annual."""
    def build():
        ticker = yf.Ticker(symbol)
        return {
            'symbol': symbol,
            'income_quarterly': _df_to_periods(getattr(ticker, 'quarterly_income_stmt', None)),
            'income_annual':    _df_to_periods(getattr(ticker, 'income_stmt', None), max_periods=5),
            'balance_quarterly':_df_to_periods(getattr(ticker, 'quarterly_balance_sheet', None)),
            'balance_annual':   _df_to_periods(getattr(ticker, 'balance_sheet', None), max_periods=5),
            'cashflow_quarterly': _df_to_periods(getattr(ticker, 'quarterly_cashflow', None)),
            'cashflow_annual':    _df_to_periods(getattr(ticker, 'cashflow', None), max_periods=5),
        }
    try:
        return jsonify(_cached_fundamentals('financials', symbol, build))
    except Exception as e:
        return jsonify({'error': str(e), 'symbol': symbol,
                        'income_quarterly': [], 'income_annual': [],
                        'balance_quarterly': [], 'balance_annual': [],
                        'cashflow_quarterly': [], 'cashflow_annual': []}), 200


@app.route('/api/analyst/<symbol>')
def get_analyst(symbol):
    """Analyst ratings, price targets, upgrades/downgrades, earnings/revenue estimates."""
    def build():
        ticker = yf.Ticker(symbol)
        info = ticker.info if hasattr(ticker, 'info') else {}

        # Recommendations summary — DataFrame with period / strongBuy / buy / hold / sell / strongSell
        recs = []
        try:
            df = ticker.recommendations
            if df is not None and not df.empty:
                # yfinance returns either with date index or with 'period' column; handle both
                df_reset = df.reset_index() if df.index.name else df.copy()
                for _, row in df_reset.head(4).iterrows():
                    rec = {}
                    for k in ['period', 'strongBuy', 'buy', 'hold', 'sell', 'strongSell']:
                        if k in row and row[k] is not None:
                            try:
                                rec[k] = int(row[k]) if k != 'period' else str(row[k])
                            except Exception:
                                rec[k] = row[k] if k == 'period' else 0
                    if rec:
                        recs.append(rec)
        except Exception:
            pass

        # Upgrades / downgrades
        upgrades = []
        try:
            df = ticker.upgrades_downgrades
            if df is not None and not df.empty:
                df_reset = df.reset_index()
                for _, row in df_reset.head(15).iterrows():
                    item = {}
                    gd = row.get('GradeDate') if 'GradeDate' in row else row.get('index')
                    if gd is not None:
                        item['date'] = gd.strftime('%Y-%m-%d') if hasattr(gd, 'strftime') else str(gd)
                    for k_src, k_dst in [('Firm', 'firm'), ('ToGrade', 'toGrade'),
                                         ('FromGrade', 'fromGrade'), ('Action', 'action')]:
                        if k_src in row and row[k_src] is not None:
                            item[k_dst] = str(row[k_src])
                    if item:
                        upgrades.append(item)
        except Exception:
            pass

        # Earnings estimate (forward quarters)
        earnings_est = []
        try:
            df = ticker.earnings_estimate
            if df is not None and not df.empty:
                for idx, row in df.iterrows():
                    item = {'period': str(idx)}
                    for k in ['avg', 'low', 'high', 'numberOfAnalysts', 'yearAgoEps', 'growth']:
                        v = row.get(k)
                        if v is not None:
                            try:
                                import math
                                fv = float(v)
                                if not (math.isnan(fv) or math.isinf(fv)):
                                    item[k] = fv
                            except Exception:
                                pass
                    earnings_est.append(item)
        except Exception:
            pass

        # Revenue estimate
        revenue_est = []
        try:
            df = ticker.revenue_estimate
            if df is not None and not df.empty:
                for idx, row in df.iterrows():
                    item = {'period': str(idx)}
                    for k in ['avg', 'low', 'high', 'numberOfAnalysts', 'yearAgoRevenue', 'growth']:
                        v = row.get(k)
                        if v is not None:
                            try:
                                import math
                                fv = float(v)
                                if not (math.isnan(fv) or math.isinf(fv)):
                                    item[k] = fv
                            except Exception:
                                pass
                    revenue_est.append(item)
        except Exception:
            pass

        return {
            'symbol': symbol,
            'recommendations': recs,  # bucket breakdown for pie/bars
            'upgradesDowngrades': upgrades,
            'earningsEstimate': earnings_est,
            'revenueEstimate': revenue_est,
            # From .info (already rate-limit friendly, reuse)
            'currentPrice': info.get('currentPrice') or info.get('regularMarketPrice'),
            'targetHigh': info.get('targetHighPrice'),
            'targetLow': info.get('targetLowPrice'),
            'targetMean': info.get('targetMeanPrice'),
            'targetMedian': info.get('targetMedianPrice'),
            'numAnalysts': info.get('numberOfAnalystOpinions'),
            'recommendationKey': info.get('recommendationKey', ''),
            'recommendationMean': info.get('recommendationMean'),
        }
    try:
        return jsonify(_cached_fundamentals('analyst', symbol, build))
    except Exception as e:
        return jsonify({'error': str(e), 'symbol': symbol,
                        'recommendations': [], 'upgradesDowngrades': [],
                        'earningsEstimate': [], 'revenueEstimate': []}), 200


@app.route('/api/corporate-actions/<symbol>')
def get_corporate_actions(symbol):
    """Dividends, splits, earnings history, next earnings date."""
    def build():
        ticker = yf.Ticker(symbol)

        dividends = []
        try:
            s = ticker.dividends
            if s is not None and not s.empty:
                for dt, v in s.tail(20).items():
                    dividends.append({
                        'date': dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else str(dt),
                        'amount': float(v),
                    })
                dividends.reverse()  # newest first
        except Exception:
            pass

        splits = []
        try:
            s = ticker.splits
            if s is not None and not s.empty:
                for dt, v in s.tail(10).items():
                    splits.append({
                        'date': dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else str(dt),
                        'ratio': float(v),
                    })
                splits.reverse()
        except Exception:
            pass

        earnings_history = []
        try:
            df = ticker.earnings_history
            if df is not None and not df.empty:
                df_reset = df.reset_index()
                for _, row in df_reset.head(8).iterrows():
                    item = {}
                    for k_src, k_dst in [('quarter', 'quarter'), ('epsActual', 'epsActual'),
                                         ('epsEstimate', 'epsEstimate'), ('epsDifference', 'epsDifference'),
                                         ('surprisePercent', 'surprisePercent')]:
                        v = row.get(k_src)
                        if v is not None:
                            try:
                                if k_dst == 'quarter':
                                    item[k_dst] = v.strftime('%Y-%m-%d') if hasattr(v, 'strftime') else str(v)
                                else:
                                    import math
                                    fv = float(v)
                                    if not (math.isnan(fv) or math.isinf(fv)):
                                        item[k_dst] = fv
                            except Exception:
                                pass
                    if item:
                        earnings_history.append(item)
        except Exception:
            pass

        # Next earnings date via calendar
        next_earnings = None
        try:
            cal = ticker.calendar
            if isinstance(cal, dict):
                ed = cal.get('Earnings Date')
                if ed:
                    first = ed[0] if isinstance(ed, list) and ed else ed
                    if hasattr(first, 'strftime'):
                        next_earnings = first.strftime('%Y-%m-%d')
                    elif first:
                        next_earnings = str(first)
        except Exception:
            pass

        return {
            'symbol': symbol,
            'dividends': dividends,
            'splits': splits,
            'earningsHistory': earnings_history,
            'nextEarnings': next_earnings,
        }
    try:
        return jsonify(_cached_fundamentals('actions', symbol, build))
    except Exception as e:
        return jsonify({'error': str(e), 'symbol': symbol,
                        'dividends': [], 'splits': [], 'earningsHistory': [],
                        'nextEarnings': None}), 200


@app.route('/api/holders/<symbol>')
def get_holders(symbol):
    """Major, institutional, and mutual-fund holders + insider transactions.
    US coverage is strong; JP is spotty (yfinance limitation)."""
    def build():
        ticker = yf.Ticker(symbol)

        def df_to_records(df, limit=15):
            if df is None or df.empty:
                return []
            out = []
            df_reset = df.reset_index() if df.index.name else df.copy()
            for _, row in df_reset.head(limit).iterrows():
                rec = {}
                for col in row.index:
                    v = row[col]
                    if v is None:
                        continue
                    try:
                        # Datetime
                        if hasattr(v, 'strftime'):
                            rec[str(col)] = v.strftime('%Y-%m-%d')
                            continue
                        # Numeric
                        import math
                        try:
                            fv = float(v)
                            if not (math.isnan(fv) or math.isinf(fv)):
                                rec[str(col)] = fv
                                continue
                        except (TypeError, ValueError):
                            pass
                        # String
                        rec[str(col)] = str(v)
                    except Exception:
                        continue
                if rec:
                    out.append(rec)
            return out

        result = {'symbol': symbol, 'major': [], 'institutional': [], 'mutualfund': [], 'insider': []}
        try: result['major'] = df_to_records(ticker.major_holders, limit=10)
        except Exception: pass
        try: result['institutional'] = df_to_records(ticker.institutional_holders, limit=15)
        except Exception: pass
        try: result['mutualfund'] = df_to_records(ticker.mutualfund_holders, limit=10)
        except Exception: pass
        try: result['insider'] = df_to_records(ticker.insider_transactions, limit=15)
        except Exception: pass
        return result
    try:
        return jsonify(_cached_fundamentals('holders', symbol, build))
    except Exception as e:
        return jsonify({'error': str(e), 'symbol': symbol,
                        'major': [], 'institutional': [], 'mutualfund': [], 'insider': []}), 200


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


# ── Background portfolio snapshots ────────────────────────────────
_SNAPSHOT_INTERVAL = 15 * 60  # 15 minutes
_snapshot_running = False


def _take_all_snapshots():
    """Compute and save portfolio snapshots for all users/portfolios."""
    conn = get_db()
    try:
        portfolios = conn.execute(
            'SELECT id, user_id, cash, fund_amount, COALESCE(is_live, 0) as is_live FROM sub_portfolios'
        ).fetchall()

        if not portfolios:
            return

        # For live portfolios, try Kabu once (shared across all live portfolios)
        kabu_positions = None
        kabu_wallet = None
        if HAS_KABU:
            kabu = _get_kabu_client()
            if kabu and kabu.is_connected():
                try:
                    kabu_positions = kabu.get_positions()
                    kabu_wallet = kabu.get_wallet_cash()
                    if not isinstance(kabu_positions, list):
                        kabu_positions = None
                    if isinstance(kabu_wallet, dict) and 'error' in kabu_wallet:
                        kabu_wallet = None
                except Exception:
                    kabu_positions = None
                    kabu_wallet = None

        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        today = datetime.now().strftime('%Y-%m-%d')
        count = 0

        for pf in portfolios:
            try:
                pid = pf['id']
                uid = pf['user_id']
                is_live = pf['is_live']
                db_cash = float(pf['cash'] or 0)
                fund_amount = float(pf['fund_amount'] or 0)

                total_value = 0
                cash = 0
                invested = 0

                if is_live and kabu_positions is not None and kabu_wallet is not None:
                    # Live portfolio: use Kabu data
                    live_cash = float(kabu_wallet.get('StockAccountWallet') or 0)
                    if live_cash == 0:
                        for key in ('FreeMargin', 'CashBalance', 'BuyingPower'):
                            val = kabu_wallet.get(key)
                            if val:
                                live_cash = float(val)
                                break
                    cash = live_cash
                    total_valuation = 0
                    for p in kabu_positions:
                        qty = float(p.get('LeavesQty') or p.get('Qty') or 0)
                        if qty <= 0:
                            continue
                        val = float(p.get('Valuation') or 0)
                        total_valuation += val
                    invested = total_valuation
                    total_value = cash + invested

                else:
                    # Simulation portfolio (or live with Kabu offline): use PRICE_CACHE
                    cash = db_cash
                    holdings = conn.execute(
                        'SELECT symbol, shares, avg_cost FROM holdings WHERE portfolio_id = ?', (pid,)
                    ).fetchall()
                    for h in holdings:
                        sym = h['symbol']
                        shares = float(h['shares'] or 0)
                        avg_cost = float(h['avg_cost'] or 0)
                        cached = PRICE_CACHE.get(sym)
                        price = cached['data']['price'] if cached and cached['data'].get('price') else avg_cost
                        invested += shares * price
                    total_value = cash + invested

                if total_value <= 0 and invested <= 0 and cash <= 0:
                    continue  # Skip empty portfolios

                net_deposits = fund_amount if fund_amount else 0

                # Write daily snapshot (INSERT OR REPLACE — keeps latest value for today)
                conn.execute(
                    '''INSERT OR REPLACE INTO portfolio_snapshots
                       (user_id, portfolio_id, date, total_value, cash, invested, net_deposits)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (uid, pid, today, round(total_value, 2), round(cash, 2), round(invested, 2), round(net_deposits, 2))
                )

                # Write intraday snapshot (INSERT — accumulates throughout the day)
                conn.execute(
                    '''INSERT INTO portfolio_snapshots_intraday
                       (user_id, portfolio_id, timestamp, total_value, cash, invested, net_deposits)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (uid, pid, now_ts, round(total_value, 2), round(cash, 2), round(invested, 2), round(net_deposits, 2))
                )
                count += 1

            except Exception as e:
                print(f'[snapshot] Error for portfolio {pf["id"]}: {e}', flush=True)

        # Prune intraday snapshots older than 7 days
        cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('DELETE FROM portfolio_snapshots_intraday WHERE timestamp < ?', (cutoff,))

        conn.commit()
        if count > 0:
            print(f'[snapshot] Saved snapshots for {count} portfolios', flush=True)

    except Exception as e:
        print(f'[snapshot] Error: {e}', flush=True)
    finally:
        conn.close()


def _snapshot_background():
    """Take portfolio snapshots for all users every 15 minutes."""
    global _snapshot_running
    if _snapshot_running:
        return
    _snapshot_running = True
    import time as _time

    _time.sleep(60)  # Wait 1 min for app + prefetch to start

    print(f'[snapshot] Background snapshots started (every {_SNAPSHOT_INTERVAL}s)', flush=True)

    while True:
        try:
            _take_all_snapshots()
        except Exception as e:
            print(f'[snapshot] Error: {e}', flush=True)
        _time.sleep(_SNAPSHOT_INTERVAL)


# ── Market Movers (top gainers/losers) + Sector Heat Map ──────────
# Curated universe to stay within Yahoo rate limits (~8 batch calls per refresh).
_MOVERS_INTERVAL = 300  # 5 minutes
_MOVERS_CACHE = {
    'jp': {'gainers': [], 'losers': [], 'ts': None, 'universe_size': 0},
    'us': {'gainers': [], 'losers': [], 'ts': None, 'universe_size': 0},
}
_HEATMAP_CACHE = {'jp': {'sectors': [], 'ts': None}, 'us': {'sectors': [], 'ts': None}}
_movers_running = False

# Nikkei 225 constituents (as of 2026 — subset suffices; covers ~80% of market cap)
NIKKEI_225_TICKERS = [
    '7203.T','9984.T','6758.T','8306.T','6861.T','7974.T','9432.T','8316.T','9433.T','4063.T',
    '6098.T','8035.T','6367.T','8058.T','6501.T','7267.T','6702.T','4519.T','6594.T','4502.T',
    '9983.T','7741.T','6273.T','4543.T','6954.T','7832.T','4568.T','6981.T','9022.T','4901.T',
    '7751.T','8031.T','8766.T','7269.T','6301.T','8001.T','5108.T','8053.T','9434.T','6752.T',
    '4452.T','4503.T','2914.T','6971.T','7011.T','6146.T','6503.T','8267.T','4911.T','6723.T',
    '7832.T','9020.T','4523.T','7733.T','9101.T','9613.T','8601.T','8591.T','6674.T','4005.T',
    '1925.T','5401.T','7276.T','5802.T','4507.T','3382.T','7182.T','6988.T','8802.T','8801.T',
    '4506.T','8308.T','7202.T','6178.T','9501.T','9502.T','4578.T','4689.T','5020.T','4042.T',
    '9766.T','6770.T','6506.T','6326.T','6367.T','7004.T','7012.T','7013.T','7261.T','7270.T',
    '7309.T','7731.T','7762.T','7912.T','8002.T','8015.T','8031.T','8331.T','8411.T','8725.T',
    '8750.T','8804.T','8830.T','9007.T','9008.T','9009.T','9021.T','9062.T','9064.T','9104.T',
    '9107.T','9147.T','9202.T','9301.T','9531.T','9532.T','9602.T','9735.T','9735.T','9843.T',
    '9989.T','1332.T','1605.T','1721.T','1801.T','1802.T','1803.T','1808.T','1812.T','1928.T',
    '1963.T','2002.T','2269.T','2282.T','2371.T','2413.T','2432.T','2501.T','2502.T','2503.T',
    '2531.T','2768.T','2801.T','2802.T','2871.T','3086.T','3099.T','3101.T','3103.T','3105.T',
    '3289.T','3401.T','3402.T','3405.T','3407.T','3436.T','3659.T','3863.T','3861.T','3864.T',
    '4004.T','4021.T','4061.T','4183.T','4188.T','4208.T','4272.T','4324.T','4443.T','4452.T',
    '4504.T','4523.T','4528.T','4543.T','4544.T','4568.T','4631.T','4631.T','4661.T','4676.T',
    '4704.T','4751.T','4755.T','4911.T','5019.T','5020.T','5101.T','5108.T','5201.T','5202.T',
    '5214.T','5232.T','5233.T','5301.T','5332.T','5333.T','5411.T','5541.T','5631.T','5703.T',
    '5706.T','5707.T','5711.T','5713.T','5714.T','5801.T','5803.T','5831.T','5901.T','6098.T',
    '6103.T','6113.T','6178.T','6273.T','6302.T','6305.T','6326.T','6361.T','6367.T','6471.T',
    '6472.T','6473.T','6479.T','6501.T','6503.T','6504.T','6506.T','6594.T','6645.T','6674.T',
]
NIKKEI_225_TICKERS = list(dict.fromkeys(NIKKEI_225_TICKERS))  # dedupe

# S&P 500 top liquid names (subset of 100 is enough for "movers" feature)
SP500_TOP_TICKERS = [
    'AAPL','MSFT','NVDA','GOOGL','GOOG','AMZN','META','TSLA','BRK-B','JPM',
    'V','UNH','XOM','JNJ','WMT','MA','PG','AVGO','HD','CVX',
    'MRK','ABBV','LLY','COST','KO','PEP','ADBE','BAC','CRM','MCD',
    'TMO','CSCO','NFLX','ACN','ABT','WFC','AMD','CMCSA','DIS','DHR',
    'LIN','VZ','TXN','NKE','NEE','QCOM','PM','PFE','RTX','MS',
    'UNP','INTC','IBM','LOW','AMGN','ORCL','INTU','BMY','HON','GS',
    'COP','DE','CAT','T','SBUX','AXP','SPGI','AMT','BKNG','MDT',
    'GE','BLK','ISRG','UPS','LMT','SYK','GILD','CVS','ADI','CI',
    'SCHW','TJX','MDLZ','MO','REGN','VRTX','ELV','ETN','C','ZTS',
    'LRCX','SO','PANW','ADP','TMUS','BSX','SLB','MU','ICE','EQIX',
]


def _movers_refresh():
    """Batch-fetch the curated universe via yf.download, compute top gainers/losers
    and per-sector aggregates. Writes to _MOVERS_CACHE and _HEATMAP_CACHE."""
    global _MOVERS_CACHE, _HEATMAP_CACHE
    from datetime import datetime as _dt

    def _fetch_universe(tickers, region_label):
        if not tickers:
            return []
        results = []
        batch_size = 100
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            try:
                df = yf.download(' '.join(batch), period='5d', interval='1d',
                                 group_by='ticker', progress=False, threads=True)
                for sym in batch:
                    try:
                        sym_df = df if len(batch) == 1 else (df[sym] if sym in df.columns.get_level_values(0) else None)
                        if sym_df is None or sym_df.empty:
                            continue
                        closes = sym_df['Close'].dropna()
                        if len(closes) < 2:
                            continue
                        price = float(closes.iloc[-1])
                        prev = float(closes.iloc[-2])
                        change = price - prev
                        change_pct = (change / prev * 100) if prev else 0
                        results.append({
                            'symbol': sym,
                            'price': round(price, 2),
                            'change': round(change, 2),
                            'change_pct': round(change_pct, 2),
                            'prev_close': round(prev, 2),
                        })
                    except Exception:
                        pass
            except Exception as e:
                print(f'[movers] batch error ({region_label}): {e}', flush=True)
        return results

    now = _dt.now()

    # JP universe
    jp_results = _fetch_universe(NIKKEI_225_TICKERS, 'jp')
    if jp_results:
        sorted_by_pct = sorted(jp_results, key=lambda r: r['change_pct'], reverse=True)
        _MOVERS_CACHE['jp'] = {
            'gainers': sorted_by_pct[:10],
            'losers': list(reversed(sorted_by_pct[-10:])),
            'ts': now.isoformat(),
            'universe_size': len(jp_results),
        }
        # Heat map: aggregate by sector using STOCK_INFO_CACHE
        _HEATMAP_CACHE['jp'] = _compute_heatmap(jp_results)

    # US universe
    us_results = _fetch_universe(SP500_TOP_TICKERS, 'us')
    if us_results:
        sorted_by_pct = sorted(us_results, key=lambda r: r['change_pct'], reverse=True)
        _MOVERS_CACHE['us'] = {
            'gainers': sorted_by_pct[:10],
            'losers': list(reversed(sorted_by_pct[-10:])),
            'ts': now.isoformat(),
            'universe_size': len(us_results),
        }
        _HEATMAP_CACHE['us'] = _compute_heatmap(us_results)

    print(f'[movers] Refreshed: JP={len(jp_results)} US={len(us_results)}', flush=True)


def _compute_heatmap(results):
    """Group results by sector (from STOCK_INFO_CACHE) and compute aggregates."""
    sectors = {}
    for r in results:
        sym = r['symbol']
        # Look up sector from cached stock info (populated lazily elsewhere)
        cached_info = STOCK_INFO_CACHE.get(sym, {}).get('data') if STOCK_INFO_CACHE.get(sym) else None
        sector = (cached_info or {}).get('sector') or 'Unknown'
        if sector not in sectors:
            sectors[sector] = {'sector': sector, 'stocks': [], 'change_sum': 0.0, 'count': 0}
        sectors[sector]['stocks'].append(r)
        sectors[sector]['change_sum'] += r['change_pct']
        sectors[sector]['count'] += 1

    out = []
    for s in sectors.values():
        avg = (s['change_sum'] / s['count']) if s['count'] else 0
        top = sorted(s['stocks'], key=lambda x: x['change_pct'], reverse=True)[:3]
        bottom = sorted(s['stocks'], key=lambda x: x['change_pct'])[:3]
        out.append({
            'sector': s['sector'],
            'count': s['count'],
            'avg_change_pct': round(avg, 2),
            'top_stocks': top,
            'bottom_stocks': bottom,
        })
    # Sort by count desc (biggest sectors first)
    out.sort(key=lambda x: x['count'], reverse=True)
    return {'sectors': out, 'ts': datetime.now().isoformat()}


def _movers_background():
    """Daemon thread: refresh movers + heatmap every 5 minutes."""
    global _movers_running
    if _movers_running:
        return
    _movers_running = True
    import time as _time
    _time.sleep(90)  # Let prefetch complete first so STOCK_INFO_CACHE is populated
    print(f'[movers] Background thread started (every {_MOVERS_INTERVAL}s)', flush=True)
    while True:
        try:
            _movers_refresh()
        except Exception as e:
            print(f'[movers] Error: {e}', flush=True)
        _time.sleep(_MOVERS_INTERVAL)


@app.route('/api/market/movers')
def market_movers():
    """Top gainers and losers from curated universe.
    Query: ?region=jp|us (default jp)."""
    region = request.args.get('region', 'jp').lower()
    if region not in ('jp', 'us'):
        region = 'jp'
    return jsonify(_MOVERS_CACHE.get(region, {'gainers': [], 'losers': [], 'ts': None}))


@app.route('/api/market/heatmap')
def market_heatmap():
    """Sector heat map for curated universe.
    Query: ?region=jp|us (default jp)."""
    region = request.args.get('region', 'jp').lower()
    if region not in ('jp', 'us'):
        region = 'jp'
    return jsonify(_HEATMAP_CACHE.get(region, {'sectors': [], 'ts': None}))


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
    try:
        ensure_user_portfolio(conn, uid)
        pid = get_portfolio_id(uid, request.args.get('portfolio_id'))
        sp = conn.execute('SELECT cash, cash_usd, realized_pnl, COALESCE(is_live, 0) as is_live, fund_amount FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
        if sp:
            cash = sp['cash']
            cash_usd = sp['cash_usd'] or 0.0
            realized_pnl = sp['realized_pnl'] or 0.0
            is_live = sp['is_live']
            db_fund_amount = float(sp['fund_amount'] or 0)
            # Guard: live accounts should never use the ¥1M default
            if is_live and db_fund_amount == 1000000.0:
                # Check if this is the unmodified default (no fund_transactions recorded)
                has_funds = conn.execute(
                    'SELECT COUNT(*) as c FROM fund_transactions WHERE user_id = ? AND portfolio_id = ?', (uid, pid)
                ).fetchone()['c']
                if has_funds == 0:
                    db_fund_amount = 0
                    conn.execute('UPDATE sub_portfolios SET fund_amount = 0 WHERE id = ? AND user_id = ?', (pid, uid))
        else:
            cash_row = conn.execute('SELECT cash FROM portfolio WHERE user_id = ?', (uid,)).fetchone()
            cash = cash_row['cash'] if cash_row else 1000000.0
            cash_usd = 0.0
            realized_pnl = 0.0
            is_live = 0
            db_fund_amount = 0
        fund_amount = get_fund_amount(conn, uid, pid)
        if fund_amount == 0 and not is_live:
            fund_amount = 1000000.0
        total_deposits, total_withdrawals = get_fund_totals(conn, uid, pid)
        net_deposits = total_deposits - total_withdrawals
        if net_deposits == 0 and not is_live:
            net_deposits = 1000000.0

        # Get USDJPY rate for frontend total calculation
        fx = PRICE_CACHE.get('USDJPY=X')
        usdjpy_rate = fx['data']['price'] if fx and fx['data'].get('price') else 150.0

        # ── Live portfolio: try Kabu Station for real-time data ──
        if is_live and HAS_KABU:
            kabu = _get_kabu_client()
            if kabu and kabu.is_connected():
                try:
                    positions = kabu.get_positions()
                    wallet = kabu.get_wallet_cash()
                    if isinstance(positions, list) and isinstance(wallet, dict) and 'error' not in wallet:
                        live_cash = float(wallet.get('StockAccountWallet') or 0)
                        if live_cash == 0:
                            for key in ('FreeMargin', 'CashBalance', 'BuyingPower'):
                                val = wallet.get(key)
                                if val:
                                    live_cash = float(val)
                                    break

                        live_holdings = []
                        total_invested = 0
                        total_valuation = 0
                        total_pnl = 0
                        for p in positions:
                            symbol = KabuClient.from_kabu_symbol(str(p.get('Symbol', '')), p.get('Exchange', 1))
                            name = p.get('SymbolName', symbol)
                            qty = float(p.get('LeavesQty') or p.get('Qty') or 0)
                            avg_cost = float(p.get('Price') or 0)
                            current_price = float(p.get('CurrentPrice') or 0)
                            pnl = float(p.get('ProfitLoss') or 0)
                            value = float(p.get('Valuation') or 0)
                            if qty <= 0:
                                continue
                            cost = qty * avg_cost
                            total_invested += cost
                            total_valuation += value
                            total_pnl += pnl
                            live_holdings.append({
                                'symbol': symbol, 'name': name, 'shares': qty,
                                'avg_cost': avg_cost, 'current_price': current_price,
                                'pnl': round(pnl, 2), 'value': round(value, 2),
                                'portfolio_id': pid
                            })

                        total_value = live_cash + total_valuation

                        # Auto-detect deposits/withdrawals by comparing expected vs actual value
                        detected_flow = _detect_live_cash_flow(conn, uid, pid, total_value, total_pnl, realized_pnl)

                        # Re-read baseline (may have been updated by cash flow detection)
                        baseline = db_fund_amount
                        if detected_flow:
                            sp_updated = conn.execute('SELECT fund_amount FROM sub_portfolios WHERE id = ?', (pid,)).fetchone()
                            baseline = float(sp_updated['fund_amount'] or 0) if sp_updated else db_fund_amount
                        all_time_pnl = (total_value - baseline) if baseline else 0
                        all_time_pct = (all_time_pnl / baseline * 100) if baseline else 0

                        # Update DB cash to stay in sync (deferred commit — only after response is fully built)
                        conn.execute('UPDATE sub_portfolios SET cash = ? WHERE id = ? AND user_id = ?', (live_cash, pid, uid))
                        conn.commit()

                        return jsonify({
                            'cash': round(live_cash, 2), 'cash_usd': 0, 'realized_pnl': round(realized_pnl, 2),
                            'fund_amount': baseline, 'net_deposits': baseline,
                            'total_deposits': baseline, 'total_withdrawals': 0,
                            'usdjpy_rate': round(usdjpy_rate, 2),
                            'holdings': live_holdings,
                            'source': 'kabu_live',
                            'total_value': round(total_value, 2),
                            'total_invested': round(total_invested, 2),
                            'total_valuation': round(total_valuation, 2),
                            'total_pnl': round(total_pnl, 2),
                            'all_time_pnl': round(all_time_pnl, 2),
                            'all_time_pct': round(all_time_pct, 2)
                        })
                except Exception:
                    pass  # Fall through to DB-based path

        # ── Standard path: DB holdings (simulation accounts or Kabu offline) ──
        holdings = [dict(r) for r in conn.execute('SELECT * FROM holdings WHERE user_id = ? AND portfolio_id = ?', (uid, pid)).fetchall()]

        # For live accounts when Kabu is offline, enrich holdings with cached/Yahoo prices
        source = 'local'
        if is_live:
            source = 'delayed'
            for h in holdings:
                quote = _fetch_single_quote(h['symbol'])
                if quote and 'error' not in quote:
                    h['current_price'] = quote.get('price', 0)
                    h['pnl'] = round((quote.get('price', 0) - h['avg_cost']) * h['shares'], 2)
                    h['value'] = round(quote.get('price', 0) * h['shares'], 2)

        return jsonify({'cash': round(cash, 2), 'cash_usd': round(cash_usd, 2), 'realized_pnl': round(realized_pnl, 2), 'fund_amount': fund_amount,
                        'total_deposits': total_deposits, 'total_withdrawals': total_withdrawals, 'net_deposits': net_deposits,
                        'usdjpy_rate': round(usdjpy_rate, 2), 'holdings': holdings, 'source': source})
    finally:
        conn.close()

@app.route('/api/portfolio/live-prices')
def live_portfolio_prices():
    """Fast price update for live portfolio holdings — used by frontend 5s polling."""
    uid, err = get_auth_user()
    if err: return err
    pid = get_portfolio_id(uid, request.args.get('portfolio_id'))

    # Try Kabu Station first
    if HAS_KABU:
        kabu = _get_kabu_client()
        if kabu and kabu.is_connected():
            try:
                positions = kabu.get_positions()
                wallet = kabu.get_wallet_cash()
                if isinstance(positions, list) and isinstance(wallet, dict) and 'error' not in wallet:
                    cash = float(wallet.get('StockAccountWallet') or 0)
                    if cash == 0:
                        for key in ('FreeMargin', 'CashBalance', 'BuyingPower'):
                            val = wallet.get(key)
                            if val:
                                cash = float(val)
                                break
                    prices = {}
                    total_valuation = 0
                    total_pnl = 0
                    for p in positions:
                        symbol = KabuClient.from_kabu_symbol(str(p.get('Symbol', '')), p.get('Exchange', 1))
                        qty = float(p.get('LeavesQty') or p.get('Qty') or 0)
                        if qty <= 0:
                            continue
                        cp = float(p.get('CurrentPrice') or 0)
                        pnl = float(p.get('ProfitLoss') or 0)
                        val = float(p.get('Valuation') or 0)
                        total_valuation += val
                        total_pnl += pnl
                        prices[symbol] = {'price': cp, 'pnl': round(pnl, 2), 'value': round(val, 2)}
                    return jsonify({'source': 'kabu_live', 'cash': round(cash, 2),
                                    'total_value': round(cash + total_valuation, 2),
                                    'total_pnl': round(total_pnl, 2), 'prices': prices})
            except Exception:
                pass

    # Fallback: fetch from price cache (Yahoo/Finnhub) — uses PRICE_CACHE so no burst of API calls
    conn = get_db()
    try:
        holdings = conn.execute('SELECT symbol, shares, avg_cost FROM holdings WHERE user_id = ? AND portfolio_id = ?', (uid, pid)).fetchall()
        sp = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
        cash = sp['cash'] if sp else 0
    finally:
        conn.close()
    prices = {}
    total_valuation = 0
    total_pnl = 0
    for h in holdings:
        quote = _fetch_single_quote(h['symbol'])
        cp = quote.get('price', 0) if quote and 'error' not in quote else 0
        val = cp * h['shares'] if cp else h['avg_cost'] * h['shares']
        pnl = (cp - h['avg_cost']) * h['shares'] if cp else 0
        total_valuation += val
        total_pnl += pnl
        prices[h['symbol']] = {'price': cp, 'pnl': round(pnl, 2), 'value': round(val, 2)}
    return jsonify({'source': 'delayed', 'cash': round(cash, 2),
                    'total_value': round(cash + total_valuation, 2),
                    'total_pnl': round(total_pnl, 2), 'prices': prices})

@app.route('/api/transactions')
def get_transactions():
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    show_all = request.args.get('all', '').lower() in ('1', 'true')
    include_funds = request.args.get('include_funds', '').lower() in ('1', 'true')
    filter_symbol = request.args.get('symbol', '').strip()

    # Quick path: fetch trades for a single symbol (for chart markers)
    if filter_symbol:
        txns = [dict(r) for r in conn.execute(
            'SELECT * FROM transactions WHERE user_id = ? AND symbol = ? ORDER BY timestamp ASC', (uid, filter_symbol)
        ).fetchall()]
        conn.close()
        return jsonify(txns)

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
                UNION ALL
                SELECT fx.id, '' as symbol, '' as name, fx.direction as action, fx.usd_amount as shares, fx.rate as price, fx.jpy_amount as total, 0 as pnl, fx.spread as commission, fx.timestamp, fx.portfolio_id, sp3.name AS portfolio_name, 'fx' as source
                FROM fx_transactions fx LEFT JOIN sub_portfolios sp3 ON fx.portfolio_id = sp3.id
                WHERE fx.user_id = ?
                ORDER BY 10 DESC, 1 DESC LIMIT 200''', (uid, uid, uid)
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
                UNION ALL
                SELECT fx.id, '' as symbol, '' as name, fx.direction as action, fx.usd_amount as shares, fx.rate as price, fx.jpy_amount as total, 0 as pnl, fx.spread as commission, fx.timestamp, fx.portfolio_id, 'fx' as source
                FROM fx_transactions fx
                WHERE fx.user_id = ? AND fx.portfolio_id = ?
                ORDER BY 10 DESC, 1 DESC LIMIT 50''', (uid, pid, uid, pid, uid, pid)
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
    # Block editing transactions in live portfolios
    sp = conn.execute('SELECT COALESCE(is_live, 0) as is_live FROM sub_portfolios WHERE id = ?', (tx['portfolio_id'],)).fetchone()
    if sp and sp['is_live']:
        conn.close()
        return jsonify({'error': 'Cannot edit transactions in a live account'}), 403

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
    # Block deleting transactions in live portfolios
    sp = conn.execute('SELECT COALESCE(is_live, 0) as is_live FROM sub_portfolios WHERE id = ?', (tx['portfolio_id'],)).fetchone()
    if sp and sp['is_live']:
        conn.close()
        return jsonify({'error': 'Cannot delete transactions in a live account'}), 403

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

    # ── Kabu Station live trading branch ──
    if HAS_KABU and not _is_us_symbol(symbol):
        kabu = _get_kabu_client()
        if kabu and kabu.is_connected() and not data.get('simulation_mode'):
            if not data.get('live_trade'):
                return jsonify({
                    'error': 'Live trading requires confirmation',
                    'requires_live_confirm': True,
                    'kabu_connected': True
                }), 400
            # Place real order via Kabu Station
            order_type = data.get('order_type', 'market')
            limit_price = float(data.get('limit_price', 0)) if order_type == 'limit' else 0
            result = kabu.send_order(
                app_symbol=symbol,
                side=action,
                qty=int(shares),
                price=limit_price,
                order_type=order_type
            )
            if isinstance(result, dict) and 'error' in result:
                return jsonify({'error': f'Kabu Station: {result["error"]}'}), 400
            kabu_order_id = result.get('OrderId', '')
            # Record in local DB for tracking (fall through to existing logic)
            # Store kabu_order_id with the transaction
            data['_kabu_order_id'] = kabu_order_id

    # ── Original trade logic continues below ──
    try:
        shares = float(data['shares'])
        price = float(data['price'])
    except (KeyError, ValueError, TypeError):
        return jsonify({'error': 'Invalid shares or price'}), 400
    if shares <= 0 or price <= 0:
        return jsonify({'error': 'Shares and price must be positive'}), 400
    total = round(shares * price, 2)
    commission_pct = max(0.0, min(100.0, float(data.get('commission_pct', 0))))
    commission = round(total * commission_pct / 100, 2) if commission_pct > 0 else 0.0
    pid = get_portfolio_id(uid, data.get('portfolio_id'))
    is_us = _is_us_symbol(symbol)

    conn = get_db()
    ensure_user_portfolio(conn, uid)
    sp = conn.execute('SELECT cash, cash_usd FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    if not sp:
        conn.close()
        return jsonify({'error': 'Portfolio not found'}), 404

    # SBI-style: US stocks use USD cash, JP stocks use JPY cash
    cash_jpy = sp['cash'] or 0
    cash_usd = sp['cash_usd'] or 0

    # Check if this is a live trade (already executed on Kabu Station)
    is_live_trade = bool(data.get('_kabu_order_id'))

    txn_pnl = 0.0
    if action == 'buy':
        if not is_live_trade:
            # Simulation: check cash and update local balance
            if is_us:
                if cash_usd < total + commission:
                    conn.close()
                    return jsonify({'error': f'Insufficient USD. Available: ${cash_usd:,.2f}. Convert JPY→USD first.'}), 400
                cash_usd -= total + commission
                conn.execute('UPDATE sub_portfolios SET cash_usd = ? WHERE id = ?', (cash_usd, pid))
            else:
                if cash_jpy < total + commission:
                    conn.close()
                    return jsonify({'error': 'Insufficient funds'}), 400
                cash_jpy -= total + commission
                conn.execute('UPDATE sub_portfolios SET cash = ? WHERE id = ?', (cash_jpy, pid))
            # Update local holdings for simulation
            existing = conn.execute('SELECT * FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid)).fetchone()
            if existing:
                new_shares = existing['shares'] + shares
                new_avg = (existing['avg_cost'] * existing['shares'] + total) / new_shares
                conn.execute('UPDATE holdings SET shares = ?, avg_cost = ? WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                            (new_shares, new_avg, uid, symbol, pid))
            else:
                conn.execute('INSERT INTO holdings (user_id, symbol, name, shares, avg_cost, portfolio_id) VALUES (?, ?, ?, ?, ?, ?)',
                            (uid, symbol, name, shares, price, pid))
        # Live trades: skip cash/holdings update — Kabu Station manages the real state.
        # Holdings will sync from Kabu on next portfolio sync.
    elif action == 'sell':
        existing = conn.execute('SELECT * FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid)).fetchone()
        if existing:
            txn_pnl = round((price - existing['avg_cost']) * shares - commission, 2)
        if not is_live_trade:
            # Simulation: check shares and update local balance
            if not existing or existing['shares'] < shares:
                conn.close()
                return jsonify({'error': 'Insufficient shares'}), 400
            if is_us:
                cash_usd += total - commission
                conn.execute('UPDATE sub_portfolios SET cash_usd = ?, realized_pnl = COALESCE(realized_pnl, 0) + ? WHERE id = ?', (cash_usd, txn_pnl, pid))
            else:
                cash_jpy += total - commission
                conn.execute('UPDATE sub_portfolios SET cash = ?, realized_pnl = COALESCE(realized_pnl, 0) + ? WHERE id = ?', (cash_jpy, txn_pnl, pid))
            new_shares = existing['shares'] - shares
            if new_shares < 0.001:
                conn.execute('DELETE FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (uid, symbol, pid))
            else:
                conn.execute('UPDATE holdings SET shares = ? WHERE user_id = ? AND symbol = ? AND portfolio_id = ?', (new_shares, uid, symbol, pid))

    trade_date = data.get('date', '').strip() or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    kabu_oid = data.get('_kabu_order_id', '')
    conn.execute('INSERT INTO transactions (user_id, symbol, name, action, shares, price, total, pnl, commission, timestamp, portfolio_id, kabu_order_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (uid, symbol, name, action, shares, price, total, txn_pnl, commission, trade_date, pid, kabu_oid or None))
    conn.commit()
    conn.close()
    # Rebuild snapshots so performance chart reflects the trade date accurately
    _backfill_snapshots_internal(uid, pid)
    resp = {'success': True, 'cash': round(cash_jpy, 2), 'cash_usd': round(cash_usd, 2), 'commission': commission}
    if kabu_oid:
        resp['kabu_order_id'] = kabu_oid
    return jsonify(resp)

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
    is_live = conn.execute('SELECT COALESCE(is_live, 0) as is_live FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    conn.execute('INSERT INTO fund_transactions (user_id, portfolio_id, type, amount, note, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                 (uid, pid, 'deposit', amount, note, now))
    # For live accounts, update baseline (fund_amount) instead of local cash — Kabu controls real cash
    if is_live and is_live['is_live']:
        conn.execute('UPDATE sub_portfolios SET fund_amount = fund_amount + ? WHERE id = ? AND user_id = ?', (amount, pid, uid))
    else:
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
    is_live = conn.execute('SELECT COALESCE(is_live, 0) as is_live FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    current_cash = sp['cash'] or 0
    # Skip cash sufficiency check for live accounts (Kabu controls real cash)
    if (not is_live or not is_live['is_live']) and amount > current_cash:
        conn.close()
        return jsonify({'error': f'Insufficient cash. Available: ¥{current_cash:,.0f}'}), 400
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('INSERT INTO fund_transactions (user_id, portfolio_id, type, amount, note, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                 (uid, pid, 'withdrawal', amount, note, now))
    # For live accounts, update baseline (fund_amount) instead of local cash
    if is_live and is_live['is_live']:
        conn.execute('UPDATE sub_portfolios SET fund_amount = CASE WHEN fund_amount - ? > 0 THEN fund_amount - ? ELSE 0 END WHERE id = ? AND user_id = ?', (amount, amount, pid, uid))
    else:
        conn.execute('UPDATE sub_portfolios SET cash = cash - ? WHERE id = ? AND user_id = ?', (amount, pid, uid))
    conn.commit()
    new_cash = conn.execute('SELECT cash FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()['cash']
    conn.close()
    return jsonify({'success': True, 'cash': round(new_cash, 2), 'amount': amount})


@app.route('/api/portfolios/<pid>/fx-convert', methods=['POST'])
def fx_convert(pid):
    """SBI-style currency conversion between JPY and USD with spread."""
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    direction = data.get('direction', '').strip()  # 'buy_usd' or 'sell_usd'
    if direction not in ('buy_usd', 'sell_usd'):
        return jsonify({'error': 'Direction must be buy_usd or sell_usd'}), 400
    try:
        amount = float(data.get('amount', 0))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid amount'}), 400
    if amount <= 0:
        return jsonify({'error': 'Amount must be positive'}), 400
    amount_currency = data.get('amount_currency', 'usd').strip().lower()

    # Exact amounts mode: user provides both JPY and USD amounts directly
    exact_jpy = data.get('jpy_amount')
    exact_mode = exact_jpy is not None
    if exact_mode:
        try:
            exact_jpy = float(exact_jpy)
        except (ValueError, TypeError):
            return jsonify({'error': 'Invalid JPY amount'}), 400
        if exact_jpy <= 0 or amount <= 0:
            return jsonify({'error': 'Both amounts must be positive'}), 400

    # Manual rate override: user provides their own rate (e.g. from SBI)
    manual_rate = data.get('manual_rate')
    if exact_mode:
        mid_rate = round(exact_jpy / amount, 4)  # Derive rate from exact amounts
        FX_SPREAD = 0
    elif manual_rate is not None:
        try:
            manual_rate = float(manual_rate)
        except (ValueError, TypeError):
            return jsonify({'error': 'Invalid manual rate'}), 400
        if manual_rate <= 0:
            return jsonify({'error': 'Manual rate must be positive'}), 400
        mid_rate = manual_rate
        FX_SPREAD = 0
    else:
        # Get live USDJPY rate
        fx = PRICE_CACHE.get('USDJPY=X')
        mid_rate = fx['data']['price'] if fx and fx['data'].get('price') else None
        if not mid_rate:
            return jsonify({'error': 'USDJPY rate not available. Try again in a moment.'}), 503
        FX_SPREAD = 0.25  # SBI-style spread (¥0.25 per dollar)

    conn = get_db()
    ensure_user_portfolio(conn, uid)
    sp = conn.execute('SELECT cash, cash_usd FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    if not sp:
        conn.close()
        return jsonify({'error': 'Portfolio not found'}), 404
    cash_jpy = sp['cash'] or 0
    cash_usd = sp['cash_usd'] or 0

    if direction == 'buy_usd':
        if exact_mode:
            usd_amount = amount
            jpy_amount = exact_jpy
        else:
            rate = mid_rate + FX_SPREAD
            if amount_currency == 'usd':
                usd_amount = amount
                jpy_amount = round(usd_amount * rate, 2)
            else:
                jpy_amount = amount
                usd_amount = round(jpy_amount / rate, 2)
        if jpy_amount > cash_jpy:
            conn.close()
            return jsonify({'error': f'Insufficient JPY. Available: ¥{cash_jpy:,.0f}, Required: ¥{jpy_amount:,.0f}'}), 400
        conn.execute('UPDATE sub_portfolios SET cash = cash - ?, cash_usd = cash_usd + ? WHERE id = ?', (jpy_amount, usd_amount, pid))
    else:  # sell_usd
        if exact_mode:
            usd_amount = amount
            jpy_amount = exact_jpy
        else:
            rate = mid_rate - FX_SPREAD
            if amount_currency == 'usd':
                usd_amount = amount
                jpy_amount = round(usd_amount * rate, 2)
            else:
                jpy_amount = amount
                usd_amount = round(jpy_amount / rate, 2)
        if usd_amount > cash_usd:
            conn.close()
            return jsonify({'error': f'Insufficient USD. Available: ${cash_usd:,.2f}, Required: ${usd_amount:,.2f}'}), 400
        conn.execute('UPDATE sub_portfolios SET cash = cash + ?, cash_usd = cash_usd - ? WHERE id = ?', (jpy_amount, usd_amount, pid))

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    effective_rate = round(jpy_amount / usd_amount, 4) if usd_amount > 0 else mid_rate
    conn.execute('INSERT INTO fx_transactions (user_id, portfolio_id, direction, usd_amount, jpy_amount, rate, spread, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                 (uid, pid, direction, usd_amount, jpy_amount, effective_rate, FX_SPREAD, now))
    conn.commit()
    updated = conn.execute('SELECT cash, cash_usd FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    conn.close()
    return jsonify({
        'success': True,
        'cash': round(updated['cash'], 2),
        'cash_usd': round(updated['cash_usd'], 2),
        'usd_amount': round(usd_amount, 2),
        'jpy_amount': round(jpy_amount, 2),
        'rate': round(effective_rate, 2),
        'mid_rate': round(mid_rate, 4),
        'spread': FX_SPREAD,
        'direction': direction
    })


@app.route('/api/portfolios/<pid>/fx-history', methods=['GET'])
def fx_history(pid):
    """Get FX conversion history for a portfolio."""
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    rows = conn.execute('SELECT * FROM fx_transactions WHERE user_id = ? AND portfolio_id = ? ORDER BY timestamp DESC', (uid, pid)).fetchall()
    conn.close()
    return jsonify([{
        'id': r['id'], 'direction': r['direction'],
        'usd_amount': r['usd_amount'], 'jpy_amount': r['jpy_amount'],
        'rate': r['rate'], 'spread': r['spread'], 'timestamp': r['timestamp']
    } for r in rows])


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


@app.route('/api/fx-transactions/<int:fxid>', methods=['DELETE'])
def delete_fx_transaction(fxid):
    """Delete an FX conversion and reverse the cash changes."""
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    fx = conn.execute('SELECT * FROM fx_transactions WHERE id = ? AND user_id = ?', (fxid, uid)).fetchone()
    if not fx:
        conn.close()
        return jsonify({'error': 'FX transaction not found'}), 404

    pid = fx['portfolio_id']
    direction = fx['direction']
    usd_amount = fx['usd_amount']
    jpy_amount = fx['jpy_amount']

    # Reverse the conversion
    if direction == 'buy_usd':
        # Was: JPY decreased, USD increased → reverse: add JPY, subtract USD
        conn.execute('UPDATE sub_portfolios SET cash = cash + ?, cash_usd = cash_usd - ? WHERE id = ?', (jpy_amount, usd_amount, pid))
    else:
        # Was: USD decreased, JPY increased → reverse: add USD, subtract JPY
        conn.execute('UPDATE sub_portfolios SET cash = cash - ?, cash_usd = cash_usd + ? WHERE id = ?', (jpy_amount, usd_amount, pid))

    conn.execute('DELETE FROM fx_transactions WHERE id = ?', (fxid,))
    conn.commit()

    updated = conn.execute('SELECT cash, cash_usd FROM sub_portfolios WHERE id = ? AND user_id = ?', (pid, uid)).fetchone()
    conn.close()
    return jsonify({'success': True, 'cash': round(updated['cash'], 2), 'cash_usd': round(updated['cash_usd'], 2)})


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
    conn.execute('UPDATE sub_portfolios SET cash = ?, cash_usd = 0, realized_pnl = 0 WHERE id = ? AND user_id = ?', (fund, pid, uid))
    conn.execute('DELETE FROM holdings WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    conn.execute('DELETE FROM transactions WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    conn.execute('DELETE FROM fx_transactions WHERE user_id = ? AND portfolio_id = ?', (uid, pid))
    # Keep fund_transactions — only clear trade transactions and FX history
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'cash': fund, 'cash_usd': 0})

@app.route('/api/portfolios', methods=['GET'])
def list_portfolios():
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    ensure_user_portfolio(conn, uid)
    rows = conn.execute('SELECT id, name, cash, cash_usd, created_at, COALESCE(is_live, 0) as is_live FROM sub_portfolios WHERE user_id = ? ORDER BY is_live DESC, created_at', (uid,)).fetchall()
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


@app.route('/api/portfolio-history-intraday')
def portfolio_history_intraday():
    """Return intraday portfolio snapshots for today (or specified date).
    Used for the 'Today' chart view showing intraday portfolio movement."""
    uid, err = get_auth_user()
    if err: return err
    pid = get_portfolio_id(uid, request.args.get('portfolio_id'))
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    conn = get_db()
    rows = conn.execute(
        '''SELECT timestamp, total_value, cash, invested
           FROM portfolio_snapshots_intraday
           WHERE user_id = ? AND portfolio_id = ? AND timestamp LIKE ?
           ORDER BY timestamp ASC''',
        (uid, pid, f'{date}%')
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


# ── Named watchlists (multiple groups per user) ──────────────────
@app.route('/api/watchlists', methods=['GET'])
def list_named_watchlists():
    """List user's named watchlists with item counts."""
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    rows = conn.execute(
        'SELECT id, name, sort_order, created_at FROM watchlists WHERE user_id = ? ORDER BY sort_order ASC, id ASC',
        (uid,)
    ).fetchall()
    result = []
    for r in rows:
        count_row = conn.execute(
            'SELECT COUNT(*) as c FROM watchlist_items WHERE list_id = ?', (r['id'],)
        ).fetchone()
        result.append({
            'id': r['id'],
            'name': r['name'],
            'sort_order': r['sort_order'] or 0,
            'created_at': r['created_at'],
            'symbol_count': count_row['c'] if count_row else 0
        })
    conn.close()
    return jsonify(result)


@app.route('/api/watchlists', methods=['POST'])
def create_named_watchlist():
    """Create a new named watchlist."""
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name required'}), 400
    conn = get_db()
    max_order = conn.execute(
        'SELECT COALESCE(MAX(sort_order), -1) as m FROM watchlists WHERE user_id = ?', (uid,)
    ).fetchone()['m']
    conn.execute(
        'INSERT INTO watchlists (user_id, name, sort_order, created_at) VALUES (?, ?, ?, ?)',
        (uid, name, (max_order or -1) + 1, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()
    new_id = conn.execute('SELECT id FROM watchlists WHERE user_id = ? ORDER BY id DESC LIMIT 1', (uid,)).fetchone()['id']
    conn.close()
    return jsonify({'success': True, 'id': new_id, 'name': name})


@app.route('/api/watchlists/<int:list_id>', methods=['PUT'])
def rename_named_watchlist(list_id):
    """Rename a watchlist."""
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name required'}), 400
    conn = get_db()
    result = conn.execute(
        'UPDATE watchlists SET name = ? WHERE id = ? AND user_id = ?', (name, list_id, uid)
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/watchlists/<int:list_id>', methods=['DELETE'])
def delete_named_watchlist(list_id):
    """Delete a watchlist and all its items."""
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    # Make sure it belongs to the user
    row = conn.execute('SELECT id FROM watchlists WHERE id = ? AND user_id = ?', (list_id, uid)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    conn.execute('DELETE FROM watchlist_items WHERE list_id = ? AND user_id = ?', (list_id, uid))
    conn.execute('DELETE FROM watchlists WHERE id = ? AND user_id = ?', (list_id, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/watchlists/<int:list_id>/symbols', methods=['GET'])
def get_watchlist_symbols(list_id):
    """Get all symbols in a named watchlist."""
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    # Verify ownership
    owned = conn.execute('SELECT id FROM watchlists WHERE id = ? AND user_id = ?', (list_id, uid)).fetchone()
    if not owned:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    rows = conn.execute(
        'SELECT symbol, added_at FROM watchlist_items WHERE list_id = ? AND user_id = ? ORDER BY added_at ASC',
        (list_id, uid)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/watchlists/<int:list_id>/symbols', methods=['POST'])
def add_watchlist_symbol(list_id):
    """Add a symbol to a named watchlist."""
    uid, err = get_auth_user()
    if err: return err
    data = request.json or {}
    symbol = (data.get('symbol') or '').strip()
    if not symbol:
        return jsonify({'error': 'Symbol required'}), 400
    conn = get_db()
    owned = conn.execute('SELECT id FROM watchlists WHERE id = ? AND user_id = ?', (list_id, uid)).fetchone()
    if not owned:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    try:
        conn.execute(
            'INSERT INTO watchlist_items (list_id, user_id, symbol, added_at) VALUES (?, ?, ?, ?)',
            (list_id, uid, symbol, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        )
        conn.commit()
    except Exception:
        pass  # already in list — idempotent
    conn.close()
    return jsonify({'success': True})


@app.route('/api/watchlists/<int:list_id>/symbols/<path:symbol>', methods=['DELETE'])
def remove_watchlist_symbol(list_id, symbol):
    """Remove a symbol from a named watchlist."""
    uid, err = get_auth_user()
    if err: return err
    conn = get_db()
    conn.execute(
        'DELETE FROM watchlist_items WHERE list_id = ? AND user_id = ? AND symbol = ?',
        (list_id, uid, symbol)
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ══════════════════════════════════════════════════════════════════
# ═══ KABU STATION API ROUTES ═════════════════════════════════════
# ══════════════════════════════════════════════════════════════════

@app.route('/api/kabu/status')
def kabu_status():
    """Check Kabu Station connection status. Actively verifies token health
    if the stored verification is stale (>60s old)."""
    kabu = _get_kabu_client()
    available = KabuClient.is_available() if HAS_KABU else False
    connected = False
    last_error = None
    if kabu and kabu.is_connected():
        # Token exists — check if it's still valid
        if kabu.is_healthy():
            connected = True  # Verified recently (<60s)
        else:
            # Stale — actively verify against Kabu
            connected = kabu.verify_token()
            last_error = kabu._last_error
    # WS is only meaningful if REST token is actually working —
    # otherwise WS socket might be open but no data flows
    ws_connected = (kabu_ws.is_connected() if HAS_KABU else False) and connected
    return jsonify({
        'available': available,
        'connected': connected,
        'ws_connected': ws_connected,
        'has_kabu': HAS_KABU,
        'last_error': last_error
    })


def _register_kabu_watchlist():
    """Register all watchlist + portfolio JP symbols for PUSH streaming."""
    try:
        kabu = _get_kabu_client()
        if not kabu or not kabu.is_connected():
            return
        conn = get_db()
        symbols = set()
        # Get all holdings across all users (JP stocks only)
        try:
            rows = conn.execute('SELECT DISTINCT symbol FROM holdings WHERE symbol LIKE ?', ('%.T',)).fetchall()
            for r in rows:
                symbols.add(r[0])
        except Exception:
            pass
        # Get all watchlist items (JP stocks only)
        try:
            rows = conn.execute('SELECT DISTINCT symbol FROM watchlist WHERE symbol LIKE ?', ('%.T',)).fetchall()
            for r in rows:
                symbols.add(r[0])
        except Exception:
            pass
        conn.close()
        # Cap at 50 (Kabu Station limit)
        symbols = list(symbols)[:50]
        if symbols:
            kabu.register_symbols(symbols)
            print(f'[kabu] Registered {len(symbols)} watchlist/portfolio symbols for PUSH', flush=True)
    except Exception as e:
        print(f'[kabu] Watchlist registration error: {e}', flush=True)


@app.route('/api/kabu/connect', methods=['POST'])
def kabu_connect():
    """Authenticate with Kabu Station."""
    if not HAS_KABU:
        return jsonify({'error': 'Kabu Station module not available', 'error_code': 'kabu_err_not_available'}), 400
    data = request.json or {}
    api_pw = data.get('api_password', '').strip() or KABU_API_PASSWORD
    order_pw = data.get('order_password', '').strip() or KABU_ORDER_PASSWORD
    if not api_pw:
        return jsonify({'error': 'API password required', 'error_code': 'kabu_err_no_password'}), 400

    kabu = _get_kabu_client()
    print(f'[kabu-connect] api_pw length={len(api_pw)}, first2={api_pw[:2] if len(api_pw)>=2 else "?"}, from_env={bool(KABU_API_PASSWORD)}', flush=True)
    kabu.set_passwords(api_pw, order_pw)
    try:
        token = kabu.authenticate()
        # Start WebSocket PUSH listener
        kabu_ws.start()
        # Register watchlist + portfolio symbols for PUSH streaming
        threading.Thread(target=_register_kabu_watchlist, daemon=True).start()
        return jsonify({'success': True, 'connected': True})
    except KabuError as e:
        return jsonify({'error': e.message, 'error_code': e.code}), 400
    except Exception as e:
        return jsonify({'error': str(e), 'error_code': 'kabu_err_unknown'}), 400


@app.route('/api/kabu/register', methods=['POST'])
def kabu_register():
    """Register symbols for PUSH streaming."""
    if not HAS_KABU:
        return jsonify({'error': 'Kabu Station not available'}), 400
    kabu = _get_kabu_client()
    if not kabu or not kabu.is_connected():
        return jsonify({'error': 'Not connected'}), 400
    data = request.json or {}
    symbols = data.get('symbols', [])
    if not symbols:
        return jsonify({'error': 'No symbols provided'}), 400
    result = kabu.register_symbols(symbols)
    if isinstance(result, dict) and 'error' in result:
        return jsonify(result), 400
    return jsonify({'success': True, 'registered': result})


@app.route('/api/kabu/board/<path:symbol>')
def kabu_board(symbol):
    """Get full order book + price for a symbol."""
    if not HAS_KABU:
        return jsonify({'error': 'Kabu Station not available'}), 400
    kabu = _get_kabu_client()
    if not kabu or not kabu.is_connected():
        return jsonify({'error': 'Not connected'}), 400

    # Check PUSH data first (instant, no API call)
    code, _ = KabuClient.to_kabu_symbol(symbol)
    push = kabu_ws.get_push_data(code)
    if push:
        # Format push data like get_board_full
        asks = push.get('_asks', [])
        bids = push.get('_bids', [])
        return jsonify({
            'symbol': symbol,
            'price': float(push.get('CurrentPrice') or 0),
            'prev_close': float(push.get('PreviousClose') or 0),
            'change': float(push.get('ChangePreviousClose') or 0),
            'change_pct': float(push.get('ChangePreviousClosePer') or 0),
            'open': float(push.get('OpeningPrice') or 0),
            'high': float(push.get('HighPrice') or 0),
            'low': float(push.get('LowPrice') or 0),
            'volume': int(push.get('TradingVolume') or 0),
            'vwap': float(push.get('VWAP') or 0),
            'asks': asks,
            'bids': bids,
            'over_sell_qty': int(push.get('OverSellQty') or 0),
            'under_buy_qty': int(push.get('UnderBuyQty') or 0),
            'source': 'kabu_push'
        })

    # Fallback to REST
    result = kabu.get_board_full(symbol)
    if 'error' in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route('/api/kabu/stream')
def kabu_stream():
    """SSE endpoint — streams real-time price + order book from PUSH data."""
    if not HAS_KABU:
        return 'Kabu Station not available', 400

    symbol = request.args.get('symbol', '').strip()
    if not symbol:
        return 'Symbol required', 400

    code, _ = KabuClient.to_kabu_symbol(symbol)

    # Register symbol for PUSH if connected
    kabu = _get_kabu_client()
    if kabu and kabu.is_connected():
        kabu.register_symbols([symbol])

    def event_stream():
        import queue
        q = queue.Queue(maxsize=50)

        def on_push(sym_code, data):
            if sym_code == code:
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass

        kabu_ws.add_callback(on_push)
        try:
            # Send initial data if available
            initial = kabu_ws.get_push_data(code)
            if initial:
                yield f'data: {json.dumps(_format_sse_data(symbol, initial))}\n\n'

            while True:
                try:
                    data = q.get(timeout=30)
                    yield f'data: {json.dumps(_format_sse_data(symbol, data))}\n\n'
                except queue.Empty:
                    # Send keepalive comment
                    yield ': keepalive\n\n'
        except GeneratorExit:
            pass
        finally:
            kabu_ws.remove_callback(on_push)

    from flask import Response
    return Response(event_stream(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


def _format_sse_data(symbol, push_data):
    """Format push data for SSE event."""
    asks = push_data.get('_asks', [])
    bids = push_data.get('_bids', [])
    return {
        'symbol': symbol,
        'price': float(push_data.get('CurrentPrice') or 0),
        'prev_close': float(push_data.get('PreviousClose') or 0),
        'change': float(push_data.get('ChangePreviousClose') or 0),
        'change_pct': float(push_data.get('ChangePreviousClosePer') or 0),
        'open': float(push_data.get('OpeningPrice') or 0),
        'high': float(push_data.get('HighPrice') or 0),
        'low': float(push_data.get('LowPrice') or 0),
        'volume': int(push_data.get('TradingVolume') or 0),
        'vwap': float(push_data.get('VWAP') or 0),
        'asks': asks,
        'bids': bids,
        'over_sell_qty': int(push_data.get('OverSellQty') or 0),
        'under_buy_qty': int(push_data.get('UnderBuyQty') or 0),
        'ts': push_data.get('_ts', '')
    }


@app.route('/api/kabu/balance')
def kabu_balance():
    """Get real cash balance from Kabu Station."""
    if not HAS_KABU:
        return jsonify({'error': 'Kabu Station not available'}), 400
    kabu = _get_kabu_client()
    if not kabu or not kabu.is_connected():
        return jsonify({'error': 'Not connected'}), 400
    result = kabu.get_wallet_cash()
    if isinstance(result, dict) and 'error' in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route('/api/kabu/positions')
def kabu_positions():
    """Get real positions from Kabu Station."""
    if not HAS_KABU:
        return jsonify({'error': 'Kabu Station not available'}), 400
    kabu = _get_kabu_client()
    if not kabu or not kabu.is_connected():
        return jsonify({'error': 'Not connected'}), 400
    positions = kabu.get_positions()
    if isinstance(positions, dict) and 'error' in positions:
        return jsonify(positions), 400
    # Format for frontend
    formatted = []
    for p in positions:
        formatted.append({
            'symbol': KabuClient.from_kabu_symbol(str(p.get('Symbol', '')), p.get('Exchange', 1)),
            'name': p.get('SymbolName', ''),
            'side': 'buy' if str(p.get('Side')) == '2' else 'sell',
            'qty': p.get('LeavesQty', p.get('Qty', 0)),
            'avg_cost': p.get('Price', 0),
            'current_price': p.get('CurrentPrice', 0),
            'pnl': p.get('ProfitLoss', 0),
            'value': p.get('Valuation', 0)
        })
    return jsonify(formatted)


@app.route('/api/kabu/orders')
def kabu_orders():
    """Get order list from Kabu Station."""
    if not HAS_KABU:
        return jsonify({'error': 'Kabu Station not available'}), 400
    kabu = _get_kabu_client()
    if not kabu or not kabu.is_connected():
        return jsonify({'error': 'Not connected'}), 400
    orders = kabu.get_orders()
    if isinstance(orders, dict) and 'error' in orders:
        return jsonify(orders), 400
    return jsonify(orders)


@app.route('/api/kabu/sync-portfolio', methods=['POST'])
def kabu_sync_portfolio():
    """Sync Kabu Station real account: create/update live portfolio, sync positions & cash."""
    uid, err = get_auth_user()
    if err: return err
    if not HAS_KABU:
        return jsonify({'error': 'Kabu Station not available'}), 400
    kabu = _get_kabu_client()
    if not kabu or not kabu.is_connected():
        return jsonify({'error': 'Not connected'}), 400

    conn = get_db()
    ensure_user_portfolio(conn, uid)

    # Find or create live portfolio
    live_pid = f'live_{uid}'
    live_pf = conn.execute('SELECT id FROM sub_portfolios WHERE id = ? AND user_id = ?', (live_pid, uid)).fetchone()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    is_first_sync = not live_pf
    if is_first_sync:
        conn.execute('INSERT INTO sub_portfolios (id, user_id, name, cash, cash_usd, realized_pnl, fund_amount, is_live, created_at) VALUES (?, ?, ?, 0, 0, 0, 0, 1, ?)',
                     (live_pid, uid, 'Real Account', now))

    # Sync cash balance from Kabu wallet
    cash_jpy = 0
    try:
        wallet = kabu.get_wallet_cash()
        if isinstance(wallet, dict) and 'error' not in wallet:
            # Kabu Station wallet/cash response: StockAccountWallet or similar
            cash_jpy = float(wallet.get('StockAccountWallet', 0))
            if cash_jpy == 0:
                # Try alternative field names
                for key in ('FreeMargin', 'CashBalance', 'BuyingPower'):
                    if wallet.get(key):
                        cash_jpy = float(wallet[key])
                        break
    except Exception:
        pass
    # Only update DB cash if we got a valid value (don't zero out on API failure)
    if cash_jpy > 0 or is_first_sync:
        conn.execute('UPDATE sub_portfolios SET cash = ? WHERE id = ? AND user_id = ?', (cash_jpy, live_pid, uid))

    # Sync positions from Kabu
    positions = kabu.get_positions()
    kabu_symbols = set()
    synced = 0
    if isinstance(positions, dict) and 'error' in positions:
        # API error — don't wipe holdings, just sync cash
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'portfolio_id': live_pid, 'synced': 0, 'cash': cash_jpy,
                        'warning': 'Could not fetch positions'})
    if isinstance(positions, list):
        for p in positions:
            symbol = KabuClient.from_kabu_symbol(str(p.get('Symbol', '')), p.get('Exchange', 1))
            name = p.get('SymbolName', symbol)
            qty = float(p.get('LeavesQty', p.get('Qty', 0)))
            avg_cost = float(p.get('Price', 0))
            if qty <= 0:
                continue
            kabu_symbols.add(symbol)
            existing = conn.execute(
                'SELECT * FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                (uid, symbol, live_pid)).fetchone()
            if existing:
                conn.execute(
                    'UPDATE holdings SET shares = ?, avg_cost = ?, name = ? WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                    (qty, avg_cost, name, uid, symbol, live_pid))
            else:
                conn.execute(
                    'INSERT INTO holdings (user_id, symbol, name, shares, avg_cost, portfolio_id) VALUES (?, ?, ?, ?, ?, ?)',
                    (uid, symbol, name, qty, avg_cost, live_pid))
            synced += 1

    # Remove holdings that no longer exist on Kabu Station
    existing_holdings = conn.execute(
        'SELECT symbol FROM holdings WHERE user_id = ? AND portfolio_id = ?', (uid, live_pid)).fetchall()
    for h in existing_holdings:
        if h['symbol'] not in kabu_symbols:
            conn.execute('DELETE FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                        (uid, h['symbol'], live_pid))

    # On first sync, record initial account value as baseline for "All Time" calculation
    if is_first_sync and isinstance(positions, list):
        total_valuation = sum(float(p.get('Valuation') or 0) for p in positions if float(p.get('LeavesQty') or p.get('Qty') or 0) > 0)
        initial_value = cash_jpy + total_valuation
        if initial_value > 0:
            conn.execute('UPDATE sub_portfolios SET fund_amount = ? WHERE id = ? AND user_id = ?',
                         (initial_value, live_pid, uid))

    # ── Sync order history from Kabu Station ──
    # Import filled orders as transactions so they appear in trade history
    orders_synced = 0
    try:
        orders = kabu.get_orders()
        if isinstance(orders, list):
            for order in orders:
                kabu_oid = str(order.get('ID', ''))
                if not kabu_oid:
                    continue

                # Only sync fully or partially filled orders (State: 5=done, 6=partial)
                state = order.get('State', 0)
                if state not in (5, 6):
                    continue

                # Skip if already synced (check by kabu_order_id)
                existing_txn = conn.execute(
                    'SELECT id FROM transactions WHERE kabu_order_id = ? AND user_id = ?',
                    (kabu_oid, uid)
                ).fetchone()
                if existing_txn:
                    continue

                # Parse order details
                order_symbol = str(order.get('Symbol', ''))
                order_exchange = order.get('Exchange', 1)
                symbol = KabuClient.from_kabu_symbol(order_symbol, order_exchange)
                order_name = order.get('SymbolName', symbol)
                order_side = order.get('Side', '')
                # Side: '1' = sell, '2' = buy
                action = 'buy' if str(order_side) == '2' else 'sell'

                # Get execution details from order details (Details array has individual fills)
                details = order.get('Details', [])
                if isinstance(details, list) and details:
                    for detail in details:
                        exec_qty = float(detail.get('Qty') or 0)
                        exec_price = float(detail.get('Price') or 0)
                        exec_id = str(detail.get('SeqNum', detail.get('ID', '')))
                        if exec_qty <= 0 or exec_price <= 0:
                            continue

                        # Use detail-level ID to avoid duplicates on partial fills
                        detail_oid = f'{kabu_oid}_{exec_id}' if exec_id else kabu_oid
                        existing_detail = conn.execute(
                            'SELECT id FROM transactions WHERE kabu_order_id = ? AND user_id = ?',
                            (detail_oid, uid)
                        ).fetchone()
                        if existing_detail:
                            continue

                        exec_total = round(exec_qty * exec_price, 2)

                        # Calculate P/L for sells
                        txn_pnl = 0.0
                        if action == 'sell':
                            held = conn.execute(
                                'SELECT avg_cost FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                                (uid, symbol, live_pid)
                            ).fetchone()
                            if held:
                                txn_pnl = round((exec_price - float(held['avg_cost'] or 0)) * exec_qty, 2)

                        # Parse execution timestamp
                        exec_time = detail.get('ExecutionDay') or order.get('RecvTime') or now
                        # Kabu timestamps can be in various formats — normalize
                        if isinstance(exec_time, str) and 'T' in exec_time:
                            exec_time = exec_time.replace('T', ' ')[:19]

                        conn.execute(
                            'INSERT INTO transactions (user_id, symbol, name, action, shares, price, total, pnl, commission, timestamp, portfolio_id, kabu_order_id) '
                            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)',
                            (uid, symbol, order_name, action, exec_qty, exec_price, exec_total, txn_pnl, exec_time, live_pid, detail_oid)
                        )
                        orders_synced += 1
                else:
                    # No Details array — use top-level order data
                    exec_qty = float(order.get('CumQty') or order.get('OrderQty') or 0)
                    exec_price = float(order.get('Price') or 0)
                    if exec_qty <= 0 or exec_price <= 0:
                        continue
                    exec_total = round(exec_qty * exec_price, 2)

                    txn_pnl = 0.0
                    if action == 'sell':
                        held = conn.execute(
                            'SELECT avg_cost FROM holdings WHERE user_id = ? AND symbol = ? AND portfolio_id = ?',
                            (uid, symbol, live_pid)
                        ).fetchone()
                        if held:
                            txn_pnl = round((exec_price - float(held['avg_cost'] or 0)) * exec_qty, 2)

                    exec_time = order.get('RecvTime', now)
                    if isinstance(exec_time, str) and 'T' in exec_time:
                        exec_time = exec_time.replace('T', ' ')[:19]

                    conn.execute(
                        'INSERT INTO transactions (user_id, symbol, name, action, shares, price, total, pnl, commission, timestamp, portfolio_id, kabu_order_id) '
                        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)',
                        (uid, symbol, order_name, action, exec_qty, exec_price, exec_total, txn_pnl, exec_time, live_pid, kabu_oid)
                    )
                    orders_synced += 1
    except Exception as e:
        print(f'[kabu-sync] Order history sync error: {e}', flush=True)

    conn.commit()
    conn.close()
    return jsonify({'success': True, 'portfolio_id': live_pid, 'synced': synced, 'orders_synced': orders_synced, 'cash': cash_jpy})

# Legacy alias
@app.route('/api/kabu/sync-positions', methods=['POST'])
def kabu_sync_positions():
    return kabu_sync_portfolio()


@app.route('/api/kabu/cancel-order', methods=['POST'])
def kabu_cancel_order():
    """Cancel a live order on Kabu Station."""
    if not HAS_KABU:
        return jsonify({'error': 'Kabu Station not available'}), 400
    kabu = _get_kabu_client()
    if not kabu or not kabu.is_connected():
        return jsonify({'error': 'Not connected'}), 400
    data = request.json or {}
    order_id = data.get('order_id', '').strip()
    if not order_id:
        return jsonify({'error': 'OrderId required'}), 400
    result = kabu.cancel_order(order_id)
    if isinstance(result, dict) and 'error' in result:
        return jsonify(result), 400
    return jsonify({'success': True, 'result': result})


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
    # Clean up old tick history (keep 3 days)
    try:
        from db import cleanup_old_ticks
        cleanup_old_ticks(days=3)
    except Exception:
        pass
    # Pre-warm caches for portfolio holdings, watchlist, and indices (runs first)
    threading.Thread(target=_prefetch_background, daemon=True).start()
    # Auto-refresh stock list from JPX every 6 hours (detects new listings / delistings)
    threading.Thread(target=_stock_list_auto_refresh, daemon=True).start()
    # Auto-refresh US stock list from Finnhub every 24 hours
    if FINNHUB_API_KEY:
        threading.Thread(target=_us_stock_list_auto_refresh, daemon=True).start()
    # Background portfolio snapshots every 15 minutes (for daily + intraday charts)
    threading.Thread(target=_snapshot_background, daemon=True).start()
    # Background market movers + heatmap refresh every 5 minutes
    threading.Thread(target=_movers_background, daemon=True).start()
    app.run(debug=False, port=port, threaded=True)
