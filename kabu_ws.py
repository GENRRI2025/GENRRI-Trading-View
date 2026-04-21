"""
Kabu Station WebSocket PUSH client.
Runs in a background daemon thread, receives real-time price + order book
updates, and stores them in a thread-safe dict for Flask routes to read.
"""

import asyncio
import json
import threading
import time
from datetime import datetime

try:
    import websockets
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

KABU_WS_URL = 'ws://localhost:18080/kabusapi/websocket'

# Thread-safe storage for latest push data per symbol
_push_data = {}       # { 'symbol_code': { ...board data..., '_ts': datetime } }
_push_lock = threading.Lock()
_callbacks = []       # list of callback functions called on each push message
_callbacks_lock = threading.Lock()

# Trade flow accumulator — aggregates executed trade value per size bucket
# per side. Kabu's PUSH doesn't emit individual trade events, so we infer
# them from deltas in TradingVolume between successive messages, and
# classify each delta's direction from whether it hit the ask (buy) or
# bid (sell) side.
#   { 'symbol_code': {
#       'last_volume': int, 'last_price': float, 'last_bid': float, 'last_ask': float,
#       'session_date': 'YYYY-MM-DD',
#       'buckets': { 'XL': {'buy': yen, 'sell': yen}, 'L': {...}, 'M': {...}, 'S': {...} },
#     } }
_trade_flow = {}
_tf_lock = threading.Lock()
TF_BUCKETS = [('XL', 100_000_000), ('L', 10_000_000), ('M', 1_000_000), ('S', 0)]

def _classify_bucket(yen_value):
    for key, threshold in TF_BUCKETS:
        if yen_value >= threshold:
            return key
    return 'S'

def _update_trade_flow(symbol_code, msg):
    """Process a PUSH message — compute volume delta, classify as buy/sell,
    bucket by size, accumulate. Called from the main WS loop."""
    try:
        cur_price = msg.get('CurrentPrice')
        cur_vol   = msg.get('TradingVolume')
        if not cur_price or cur_vol is None:
            return
        cur_price = float(cur_price)
        cur_vol = int(cur_vol)
        # Best bid/ask to determine direction
        best_ask = None
        s1 = msg.get('Sell1')
        if isinstance(s1, dict): best_ask = s1.get('Price')
        else: best_ask = msg.get('Sell1Price')
        best_bid = None
        b1 = msg.get('Buy1')
        if isinstance(b1, dict): best_bid = b1.get('Price')
        else: best_bid = msg.get('Buy1Price')
        best_ask = float(best_ask) if best_ask else None
        best_bid = float(best_bid) if best_bid else None

        today = datetime.now().strftime('%Y-%m-%d')
        with _tf_lock:
            entry = _trade_flow.get(symbol_code)
            if not entry or entry.get('session_date') != today:
                # New session (or first message) — reset
                _trade_flow[symbol_code] = {
                    'last_volume': cur_vol, 'last_price': cur_price,
                    'last_bid': best_bid, 'last_ask': best_ask,
                    'session_date': today,
                    'buckets': {k: {'buy': 0.0, 'sell': 0.0} for k, _ in TF_BUCKETS},
                    'total_buy': 0.0, 'total_sell': 0.0,
                }
                return

            delta_vol = cur_vol - entry['last_volume']
            if delta_vol > 0:
                # A trade (or batch of trades) happened. Estimate direction:
                #   - If cur_price >= last_ask: buyer lifted the offer → BUY
                #   - If cur_price <= last_bid: seller hit the bid → SELL
                #   - Else: use price delta. Up → buy, down → sell, flat → split
                last_ask = entry.get('last_ask')
                last_bid = entry.get('last_bid')
                last_price = entry.get('last_price')
                direction = None
                if last_ask and cur_price >= last_ask:
                    direction = 'buy'
                elif last_bid and cur_price <= last_bid:
                    direction = 'sell'
                elif last_price:
                    if cur_price > last_price: direction = 'buy'
                    elif cur_price < last_price: direction = 'sell'
                if direction:
                    trade_yen = delta_vol * cur_price
                    bucket = _classify_bucket(trade_yen)
                    entry['buckets'][bucket][direction] += trade_yen
                    if direction == 'buy': entry['total_buy'] += trade_yen
                    else: entry['total_sell'] += trade_yen
                # If direction was unclear (price flat, no bid/ask), split
                # 50/50 to avoid losing the volume completely
                else:
                    trade_yen = delta_vol * cur_price
                    bucket = _classify_bucket(trade_yen)
                    entry['buckets'][bucket]['buy']  += trade_yen / 2
                    entry['buckets'][bucket]['sell'] += trade_yen / 2
                    entry['total_buy']  += trade_yen / 2
                    entry['total_sell'] += trade_yen / 2

            entry['last_volume'] = cur_vol
            entry['last_price']  = cur_price
            if best_ask: entry['last_ask'] = best_ask
            if best_bid: entry['last_bid'] = best_bid
    except Exception:
        pass


def get_trade_flow(symbol_code):
    """Return accumulated trade flow for a symbol. Deep-copied snapshot."""
    with _tf_lock:
        entry = _trade_flow.get(str(symbol_code))
        if not entry:
            return None
        return {
            'session_date': entry['session_date'],
            'buckets': {k: {'buy': v['buy'], 'sell': v['sell']} for k, v in entry['buckets'].items()},
            'total_buy':  entry['total_buy'],
            'total_sell': entry['total_sell'],
        }

# Price tick history for building candles
_price_history = {}   # { 'symbol_code': [(unix_ts, price, volume), ...] }
_history_lock = threading.Lock()
MAX_HISTORY_TICKS = 5000  # per symbol, ~1 day of frequent ticks

# Pending ticks to flush to DB
_pending_ticks = []   # [(symbol_code, unix_ts, price, volume), ...]
_pending_lock = threading.Lock()
_last_flush = 0

# Connection state
_ws_thread = None
_ws_running = False
_ws_connected = False


def get_push_data(symbol_code):
    """Get latest push data for a symbol. Returns dict or None."""
    with _push_lock:
        entry = _push_data.get(str(symbol_code))
        if entry:
            return entry.copy()
        return None


def get_all_push_data():
    """Get all push data. Returns dict of symbol -> data."""
    with _push_lock:
        return {k: v.copy() for k, v in _push_data.items()}


def add_callback(fn):
    """Register a callback called on each push message: fn(symbol_code, data)."""
    with _callbacks_lock:
        _callbacks.append(fn)


def remove_callback(fn):
    """Remove a previously registered callback."""
    with _callbacks_lock:
        try:
            _callbacks.remove(fn)
        except ValueError:
            pass


def is_connected():
    """Check if WebSocket is currently connected."""
    return _ws_connected


def _parse_push_message(msg):
    """Parse a push message and extract order book data."""
    data = msg.copy()

    # Extract order book arrays for frontend
    asks = []
    for i in range(1, 11):
        sell = msg.get(f'Sell{i}')
        if isinstance(sell, dict):
            p, q = sell.get('Price'), sell.get('Qty')
        else:
            p = msg.get(f'Sell{i}Price')
            q = msg.get(f'Sell{i}Qty')
        if p:
            asks.append({'price': float(p), 'qty': int(q or 0)})

    bids = []
    for i in range(1, 11):
        buy = msg.get(f'Buy{i}')
        if isinstance(buy, dict):
            p, q = buy.get('Price'), buy.get('Qty')
        else:
            p = msg.get(f'Buy{i}Price')
            q = msg.get(f'Buy{i}Qty')
        if p:
            bids.append({'price': float(p), 'qty': int(q or 0)})

    data['_asks'] = asks
    data['_bids'] = bids
    data['_ts'] = datetime.now().isoformat()
    return data


async def _ws_loop():
    """Main WebSocket event loop. Auto-reconnects with backoff."""
    global _ws_connected
    backoff = 1

    while _ws_running:
        try:
            async with websockets.connect(
                KABU_WS_URL,
                ping_timeout=None,   # Kabu Station has no heartbeat
                close_timeout=5
            ) as ws:
                _ws_connected = True
                backoff = 1
                print('[kabu-ws] Connected to PUSH API', flush=True)

                while _ws_running and not ws.closed:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                        msg = json.loads(raw)
                        symbol = str(msg.get('Symbol', ''))

                        if symbol:
                            parsed = _parse_push_message(msg)
                            with _push_lock:
                                _push_data[symbol] = parsed

                            # Update trade flow buckets from volume delta
                            _update_trade_flow(symbol, msg)

                            # Store price tick for candle building
                            cur_price = msg.get('CurrentPrice')
                            if cur_price:
                                ts_now = time.time()
                                price_f = float(cur_price)
                                vol_i = int(msg.get('TradingVolume') or 0)
                                tick = (ts_now, price_f, vol_i)
                                with _history_lock:
                                    if symbol not in _price_history:
                                        _price_history[symbol] = []
                                    _price_history[symbol].append(tick)
                                    if len(_price_history[symbol]) > MAX_HISTORY_TICKS:
                                        _price_history[symbol] = _price_history[symbol][-MAX_HISTORY_TICKS:]
                                # Buffer for DB flush
                                with _pending_lock:
                                    _pending_ticks.append((symbol, ts_now, price_f, vol_i))
                                _maybe_flush_ticks()

                            # Notify callbacks
                            with _callbacks_lock:
                                cbs = list(_callbacks)
                            for cb in cbs:
                                try:
                                    cb(symbol, parsed)
                                except Exception:
                                    pass

                    except asyncio.TimeoutError:
                        # No data for 60s — normal during market close
                        continue
                    except websockets.ConnectionClosed:
                        break

        except Exception as e:
            print(f'[kabu-ws] Connection error: {e}', flush=True)

        _ws_connected = False
        if _ws_running:
            print(f'[kabu-ws] Reconnecting in {backoff}s...', flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def _run_ws_thread():
    """Entry point for the background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_ws_loop())
    except Exception as e:
        print(f'[kabu-ws] Thread error: {e}', flush=True)
    finally:
        loop.close()


def start():
    """Start the WebSocket listener in a background daemon thread."""
    global _ws_thread, _ws_running

    if not HAS_WEBSOCKETS:
        print('[kabu-ws] websockets library not installed, PUSH disabled', flush=True)
        return False

    if _ws_thread and _ws_thread.is_alive():
        return True  # already running

    _ws_running = True
    _ws_thread = threading.Thread(target=_run_ws_thread, daemon=True)
    _ws_thread.start()
    return True


def stop():
    """Stop the WebSocket listener."""
    global _ws_running, _ws_connected
    _ws_running = False
    _ws_connected = False


def _maybe_flush_ticks():
    """Flush pending ticks to database every ~10 seconds."""
    global _last_flush
    now = time.time()
    if now - _last_flush < 10:
        return
    _last_flush = now
    with _pending_lock:
        batch = list(_pending_ticks)
        _pending_ticks.clear()
    if batch:
        try:
            from db import insert_ticks_batch
            # Convert symbol codes to app symbols (e.g. '9984' -> '9984.T')
            rows = [(sym + '.T', ts, price, vol) for sym, ts, price, vol in batch]
            insert_ticks_batch(rows)
        except Exception as e:
            print(f'[kabu-ws] Tick flush error: {e}', flush=True)


def flush_ticks_now():
    """Force flush all pending ticks to DB (e.g. on shutdown)."""
    global _last_flush
    _last_flush = 0
    _maybe_flush_ticks()


def get_price_history(symbol_code):
    """Get raw tick history for a symbol. Returns list of (unix_ts, price, volume)."""
    with _history_lock:
        ticks = _price_history.get(str(symbol_code), [])
        return list(ticks)


def build_candles(symbol_code, interval_sec=300, after_ts=0):
    """Build OHLCV candles from tick history (memory + DB).

    Args:
        symbol_code: e.g. '9984'
        interval_sec: candle interval in seconds (60=1m, 300=5m, etc.)
        after_ts: only include candles after this unix timestamp (JST-adjusted)

    Returns list of dicts: [{time, open, high, low, close, volume}, ...]
    """
    JST_OFFSET = 9 * 3600

    # Convert after_ts from JST chart time back to UTC for DB query
    after_utc = after_ts - JST_OFFSET if after_ts > JST_OFFSET else 0

    # Get ticks from memory
    mem_ticks = get_price_history(symbol_code)

    # Get ticks from DB (covers periods before current session)
    db_ticks = []
    try:
        from db import get_ticks
        app_symbol = str(symbol_code) + '.T'
        db_ticks = get_ticks(app_symbol, after_ts=after_utc)
    except Exception:
        pass

    # Merge: DB ticks first, then memory ticks, deduplicate by rounding ts
    all_ticks = {}
    for ts, price, vol in db_ticks:
        key = round(ts, 1)
        all_ticks[key] = (ts, price, vol)
    for ts, price, vol in mem_ticks:
        key = round(ts, 1)
        all_ticks[key] = (ts, price, vol)

    if not all_ticks:
        return []

    candles = {}
    for ts, price, vol in all_ticks.values():
        jst_ts = ts + JST_OFFSET
        bucket = int(jst_ts // interval_sec) * interval_sec
        if bucket <= after_ts:
            continue
        if bucket not in candles:
            candles[bucket] = {'time': bucket, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': vol, '_first_ts': ts}
        else:
            c = candles[bucket]
            c['high'] = max(c['high'], price)
            c['low'] = min(c['low'], price)
            # Use chronological order for open/close
            if ts < c['_first_ts']:
                c['open'] = price
                c['_first_ts'] = ts
            else:
                c['close'] = price
            c['volume'] = max(c['volume'], vol)

    result = sorted(candles.values(), key=lambda c: c['time'])
    for c in result:
        c.pop('_first_ts', None)
        c['open'] = round(c['open'], 2)
        c['high'] = round(c['high'], 2)
        c['low'] = round(c['low'], 2)
        c['close'] = round(c['close'], 2)
    return result


def clear_data():
    """Clear all cached push data."""
    with _push_lock:
        _push_data.clear()
    with _history_lock:
        _price_history.clear()
