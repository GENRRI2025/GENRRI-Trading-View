"""
Kabu Station API REST client.
Communicates with the local Kabu Station desktop app at localhost:18080.
Handles authentication, quotes (board), orders, positions, and balance.
"""

import json
import urllib.request
import urllib.parse
from datetime import datetime
import threading


class KabuError(Exception):
    """Kabu Station error with a stable code for i18n on the frontend."""
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message

# ── Config ────────────────────────────────────────────────────────
KABU_BASE_URL = 'http://localhost:18080/kabusapi'
KABU_TEST_URL = 'http://localhost:18081/kabusapi'

# Rate limit: sliding window (same pattern as _finnhub_call_times in app.py)
_data_call_times = []
_order_call_times = []
_DATA_RATE_LIMIT = 10   # 10 req/s for data endpoints
_ORDER_RATE_LIMIT = 5   # 5 req/s for order endpoints
_lock = threading.Lock()


def _rate_check(call_times, limit):
    """Check and record a rate-limited call. Returns True if allowed."""
    now = datetime.now().timestamp()
    with _lock:
        call_times[:] = [t for t in call_times if now - t < 1.0]
        if len(call_times) >= limit:
            return False
        call_times.append(now)
        return True


class KabuClient:
    """REST client for Kabu Station API."""

    def __init__(self, base_url=None):
        self.base_url = (base_url or KABU_BASE_URL).rstrip('/')
        self._token = None
        self._api_password = None
        self._order_password = None
        self._last_verified_at = None  # datetime of last successful REST call
        self._last_error = None        # last error message (for diagnostics)

    # ── Auth ──────────────────────────────────────────────────────

    def set_passwords(self, api_password, order_password=''):
        self._api_password = api_password
        self._order_password = order_password

    def authenticate(self, api_password=None):
        """POST /token — get session token. Returns token string or raises KabuError."""
        pw = api_password or self._api_password
        if not pw:
            raise KabuError('kabu_err_no_password', 'API password not set')
        # Clear any stale token before attempting — on failure, we should NOT claim to be connected
        self._token = None
        body = json.dumps({'APIPassword': pw}).encode('utf-8')
        req = urllib.request.Request(
            f'{self.base_url}/token',
            data=body,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            if data.get('ResultCode') == 0 and data.get('Token'):
                self._token = data['Token']
                self._api_password = pw
                return self._token
            raise KabuError('kabu_err_auth_failed', f'Auth failed: ResultCode={data.get("ResultCode")}')
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise KabuError('kabu_err_auth', 'Kabu Station: wrong API password or session expired. Re-enter password or restart Kabu Station.')
            raise KabuError('kabu_err_http', f'Kabu Station HTTP error: {e}')
        except urllib.error.URLError as e:
            raise KabuError('kabu_err_unreachable', f'Cannot reach Kabu Station: {e}')

    def is_connected(self):
        """Check if we have a stored token. Fast — no network call.
        This does NOT guarantee the token is still valid server-side —
        use is_healthy() or verify_token() for that."""
        return bool(self._token)

    def is_healthy(self):
        """Return True if token exists AND has been verified within the last 60s.
        For cheaper status checks — avoids hammering Kabu with verify calls."""
        if not self._token:
            return False
        if not self._last_verified_at:
            return False
        from datetime import timedelta
        return (datetime.now() - self._last_verified_at) < timedelta(seconds=60)

    def verify_token(self):
        """Make a cheap API call to check if the token is still valid.
        Clears the token if it's rejected. Returns True/False.
        Note: this bypasses rate limiting because it's meant for status checks."""
        if not self._token:
            return False
        try:
            req = urllib.request.Request(
                f'{self.base_url}/wallet/cash',
                headers={'X-API-KEY': self._token, 'Content-Type': 'application/json'},
                method='GET'
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                resp.read()  # drain body
                self._last_verified_at = datetime.now()
                self._last_error = None
                return True
        except urllib.error.HTTPError as e:
            if e.code == 401:
                self._token = None
                self._last_verified_at = None
                self._last_error = 'Token invalid — session expired or Kabu restarted'
                return False
            # Other HTTP errors — don't clear token (could be transient)
            return True
        except Exception as e:
            # Network error — assume token still valid, don't clear it
            return bool(self._token)

    def _request(self, method, path, body=None, is_order=False):
        """Make an authenticated request. Auto re-auth on 401 once."""
        if not self._token:
            return {'error': 'Not authenticated'}

        # Rate limit
        times = _order_call_times if is_order else _data_call_times
        limit = _ORDER_RATE_LIMIT if is_order else _DATA_RATE_LIMIT
        if not _rate_check(times, limit):
            return {'error': 'Rate limited'}

        headers = {
            'Content-Type': 'application/json',
            'X-API-KEY': self._token
        }
        data = json.dumps(body).encode('utf-8') if body else None
        req = urllib.request.Request(
            f'{self.base_url}{path}',
            data=data,
            headers=headers,
            method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read())
                self._last_verified_at = datetime.now()  # Mark token verified
                self._last_error = None
                return result
        except urllib.error.HTTPError as e:
            if e.code == 401:
                # Token is invalid — clear it
                self._token = None
                self._last_verified_at = None
                if self._api_password:
                    # Re-auth once
                    try:
                        self.authenticate()
                        headers['X-API-KEY'] = self._token
                        req = urllib.request.Request(
                            f'{self.base_url}{path}',
                            data=data,
                            headers=headers,
                            method=method
                        )
                        with urllib.request.urlopen(req, timeout=10) as resp:
                            result = json.loads(resp.read())
                            self._last_verified_at = datetime.now()
                            self._last_error = None
                            return result
                    except Exception as reauth_err:
                        self._last_error = f'Re-auth failed: {reauth_err}'
            try:
                err_body = json.loads(e.read())
                msg = err_body.get('Message', str(e))
            except Exception:
                msg = f'HTTP {e.code}'
            self._last_error = msg
            return {'error': msg}
        except urllib.error.URLError as e:
            self._last_error = f'Connection failed: {e}'
            return {'error': f'Connection failed: {e}'}
        except Exception as e:
            self._last_error = str(e)
            return {'error': str(e)}

    # ── Symbol conversion ─────────────────────────────────────────

    # Kabu Station index codes on exchange 1 (Tokyo).
    # Reference: Kabu Station API docs, 指数コード.
    INDEX_CODE_MAP = {
        '^N225':  ('101', 1),   # Nikkei 225 (日経平均株価)
        '^TPX':   ('108', 1),   # TOPIX (東証株価指数)
        '^TOPX':  ('108', 1),   # TOPIX alias
        '^JPX400':('112', 1),   # JPX-Nikkei 400
        '^N400':  ('112', 1),   # JPX-Nikkei 400 alias
    }

    @staticmethod
    def to_kabu_symbol(app_symbol):
        """Convert app symbol to Kabu Station format.
        '9984.T' -> ('9984', 1)   # Tokyo stock
        '^N225'  -> ('101', 1)    # Nikkei 225 index
        '^TPX'   -> ('108', 1)    # TOPIX index
        """
        # Index symbols (^...)
        if app_symbol in KabuClient.INDEX_CODE_MAP:
            return KabuClient.INDEX_CODE_MAP[app_symbol]
        # Regular Tokyo stock
        code = app_symbol.replace('.T', '')
        return code, 1  # exchange=1 for Tokyo

    @staticmethod
    def from_kabu_symbol(code, exchange=1):
        """Convert Kabu Station symbol to app format.
        ('9984', 1) -> '9984.T'
        """
        return f'{code}.T'

    # ── Board / Quotes ────────────────────────────────────────────

    def get_board(self, app_symbol):
        """GET /board/{symbol}@{exchange} — real-time price + order book.
        Returns raw Kabu Station board data.
        """
        code, exchange = self.to_kabu_symbol(app_symbol)
        return self._request('GET', f'/board/{code}@{exchange}')

    def get_board_as_quote(self, app_symbol):
        """Get board data formatted as the app's standard quote format."""
        board = self.get_board(app_symbol)
        if 'error' in board:
            return board
        price      = board.get('CurrentPrice') or 0
        prev_close = board.get('PreviousClose') or 0
        change     = board.get('ChangePreviousClose') or 0
        change_pct = board.get('ChangePreviousClosePer') or 0
        day_high   = board.get('HighPrice') or 0
        day_low    = board.get('LowPrice') or 0
        day_open   = board.get('OpeningPrice') or 0
        volume     = board.get('TradingVolume') or 0
        turnover   = board.get('TradingValue') or 0
        vwap       = board.get('VWAP') or 0
        def rp(x):  # round price or 0
            return round(float(x), 2) if x else 0
        def ri(x):
            return int(x) if x else 0
        return {
            'symbol': app_symbol,
            'price':       rp(price),
            'change':      rp(change),
            'change_pct':  rp(change_pct),
            'prev_close':  rp(prev_close),
            'day_high':    rp(day_high),
            'day_low':     rp(day_low),
            'day_open':    rp(day_open),
            'volume':      ri(volume),
            'turnover':    ri(turnover),
            'vwap':        rp(vwap),
            'source': 'kabu_station',
            'chart': []
        }

    def get_board_full(self, app_symbol):
        """Get full board data including order book, formatted for frontend."""
        board = self.get_board(app_symbol)
        if 'error' in board:
            return board

        # Note: Kabu Station reverses Bid/Ask naming.
        # Their "Sell" = ask side, "Buy" = bid side — which is actually correct
        # from the perspective of the order book display.
        asks = []
        for i in range(1, 11):
            p = board.get(f'Sell{i}', {})
            if isinstance(p, dict):
                price = p.get('Price')
                qty = p.get('Qty')
            else:
                price = board.get(f'Sell{i}Price')
                qty = board.get(f'Sell{i}Qty')
            if price:
                asks.append({'price': float(price), 'qty': int(qty or 0)})

        bids = []
        for i in range(1, 11):
            p = board.get(f'Buy{i}', {})
            if isinstance(p, dict):
                price = p.get('Price')
                qty = p.get('Qty')
            else:
                price = board.get(f'Buy{i}Price')
                qty = board.get(f'Buy{i}Qty')
            if price:
                bids.append({'price': float(price), 'qty': int(qty or 0)})

        return {
            'symbol': app_symbol,
            'price': float(board.get('CurrentPrice') or 0),
            'prev_close': float(board.get('PreviousClose') or 0),
            'change': float(board.get('ChangePreviousClose') or 0),
            'change_pct': float(board.get('ChangePreviousClosePer') or 0),
            'open': float(board.get('OpeningPrice') or 0),
            'high': float(board.get('HighPrice') or 0),
            'low': float(board.get('LowPrice') or 0),
            'volume': int(board.get('TradingVolume') or 0),
            'vwap': float(board.get('VWAP') or 0),
            'asks': asks,
            'bids': bids,
            'over_sell_qty': int(board.get('OverSellQty') or 0),
            'under_buy_qty': int(board.get('UnderBuyQty') or 0),
            'source': 'kabu_station'
        }

    # ── Symbol Registration (for PUSH) ────────────────────────────

    def register_symbols(self, app_symbols):
        """PUT /register — register symbols for WebSocket push data.
        Max 50 symbols. Symbols accumulate (call multiple times if needed).
        """
        symbols = []
        for s in app_symbols:
            code, exchange = self.to_kabu_symbol(s)
            symbols.append({'Symbol': code, 'Exchange': exchange})
        return self._request('PUT', '/register', {'Symbols': symbols})

    def unregister_all(self):
        """PUT /unregister/all — clear all registered symbols."""
        return self._request('PUT', '/unregister/all')

    # ── Orders ────────────────────────────────────────────────────

    def send_order(self, app_symbol, side, qty, price=0, order_type='market',
                   account_type=4, order_password=None):
        """POST /sendorder — place a cash stock order.

        Args:
            app_symbol: e.g. '9984.T'
            side: 'buy' or 'sell'
            qty: number of shares (int)
            price: 0 for market, actual price for limit
            order_type: 'market' or 'limit'
            account_type: 4=specific(特定), 2=general(一般), 12=NISA
            order_password: order password (uses stored password if not provided)
        """
        pw = order_password or self._order_password
        if not pw:
            return {'error': 'Order password not set'}

        code, exchange = self.to_kabu_symbol(app_symbol)
        kabu_side = '2' if side == 'buy' else '1'
        front_order_type = 10 if order_type == 'market' else 20
        order_price = 0 if order_type == 'market' else float(price)

        body = {
            'Password': pw,
            'Symbol': code,
            'Exchange': exchange,
            'SecurityType': 1,
            'Side': kabu_side,
            'CashMargin': 1,        # 1 = Cash (現物)
            'DelivType': 2,          # 2 = deposit (預り金)
            'FundType': 'AA',
            'AccountType': account_type,
            'Qty': int(qty),
            'FrontOrderType': front_order_type,
            'Price': order_price,
            'ExpireDay': 0           # 0 = today
        }
        return self._request('POST', '/sendorder', body, is_order=True)

    def cancel_order(self, order_id, order_password=None):
        """PUT /cancelorder — cancel an existing order."""
        pw = order_password or self._order_password
        if not pw:
            return {'error': 'Order password not set'}
        return self._request('PUT', '/cancelorder',
                             {'OrderId': order_id, 'Password': pw}, is_order=True)

    # ── Positions ─────────────────────────────────────────────────

    def get_positions(self):
        """GET /positions — current holdings."""
        result = self._request('GET', '/positions?product=1&addinfo=true')
        if isinstance(result, list):
            return result
        return result  # error dict

    # ── Balance ───────────────────────────────────────────────────

    def get_wallet_cash(self):
        """GET /wallet/cash — cash trading capacity."""
        return self._request('GET', '/wallet/cash')

    # ── Orders ────────────────────────────────────────────────────

    def get_orders(self):
        """GET /orders — order history."""
        result = self._request('GET', '/orders?product=1')
        if isinstance(result, list):
            return result
        return result

    # ── Symbol Info ───────────────────────────────────────────────

    def get_symbol_info(self, app_symbol):
        """GET /symbol/{symbol} — security details."""
        code, exchange = self.to_kabu_symbol(app_symbol)
        return self._request('GET', f'/symbol/{code}@{exchange}')

    # ── Availability Check ────────────────────────────────────────

    @staticmethod
    def is_available():
        """Check if Kabu Station API is reachable (no auth needed)."""
        try:
            req = urllib.request.Request(
                f'{KABU_BASE_URL}/token',
                data=json.dumps({'APIPassword': ''}).encode('utf-8'),
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=2) as resp:
                return True
        except urllib.error.HTTPError:
            return True  # got a response, server is up (just bad password)
        except Exception:
            return False
