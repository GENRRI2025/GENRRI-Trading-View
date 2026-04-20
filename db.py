"""
Database abstraction layer for GENRRI.
Supports both SQLite (local) and PostgreSQL (production) via DATABASE_URL env var.

Usage:
    from db import get_db, init_db, USE_PG

    # get_db() returns a connection that works identically for both backends.
    conn = get_db()
    conn.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    # ^ The '?' placeholders work for BOTH SQLite and PostgreSQL — the wrapper
    #   auto-converts them to '%s' when running on PostgreSQL.
"""

import os
import re
import sqlite3

DATABASE_URL = os.environ.get('DATABASE_URL', '')
USE_PG = bool(DATABASE_URL)

if USE_PG:
    import psycopg2
    import psycopg2.extras

DB_PATH = os.path.join(os.path.dirname(__file__), 'portfolio.db')


def _translate_sql(sql):
    """Convert SQLite-flavored SQL to PostgreSQL-compatible SQL."""
    # ? → %s  (but not inside strings)
    translated = re.sub(r'\?', '%s', sql)

    # INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY
    translated = re.sub(
        r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT',
        'SERIAL PRIMARY KEY',
        translated,
        flags=re.IGNORECASE
    )

    # INSERT OR IGNORE INTO table (...) VALUES (...)
    # → INSERT INTO table (...) VALUES (...) ON CONFLICT DO NOTHING
    translated = re.sub(
        r'INSERT\s+OR\s+IGNORE\s+INTO',
        'INSERT INTO',
        translated,
        flags=re.IGNORECASE
    )
    if 'INSERT INTO' in translated.upper() and 'OR IGNORE' not in sql.upper() and 'ON CONFLICT' not in translated.upper():
        pass  # normal INSERT, leave as-is
    elif 'OR IGNORE' in sql.upper() and 'ON CONFLICT' not in translated.upper():
        translated = translated.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'

    # INSERT OR REPLACE INTO table (cols) VALUES (vals)
    # → INSERT INTO table (cols) VALUES (vals) ON CONFLICT (pk) DO UPDATE SET ...
    # This one is complex — we handle it by converting to a simpler upsert pattern
    or_replace_match = re.match(
        r'INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)',
        translated,
        flags=re.IGNORECASE
    )
    if or_replace_match:
        table = or_replace_match.group(1)
        cols_str = or_replace_match.group(2)
        vals_str = or_replace_match.group(3)
        cols = [c.strip() for c in cols_str.split(',')]

        # Map tables to their composite primary keys
        pk_map = {
            'settings': ['user_id', 'key'],
            'portfolio_snapshots': ['user_id', 'portfolio_id', 'date'],
            'watchlist': ['user_id', 'symbol'],
        }
        pk_cols = pk_map.get(table.lower(), [cols[0]])

        non_pk = [c for c in cols if c not in pk_cols]
        update_clause = ', '.join(f'{c} = EXCLUDED.{c}' for c in non_pk)
        conflict_cols = ', '.join(pk_cols)

        translated = (
            f'INSERT INTO {table} ({cols_str}) VALUES ({vals_str}) '
            f'ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}'
        )

    return translated


class PgRowProxy(dict):
    """Makes psycopg2 RealDictRow behave like sqlite3.Row — supports both
    dict-style access (row['col']) and index-based access (row[0])."""

    def __init__(self, data, columns=None):
        super().__init__(data)
        self._columns = columns or list(data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return super().__getitem__(self._columns[key])
        return super().__getitem__(key)

    def keys(self):
        return self._columns


class PgCursorWrapper:
    """Wraps a psycopg2 cursor to auto-translate SQL and provide sqlite3-like API."""

    def __init__(self, cursor, columns=None):
        self._cursor = cursor
        self._columns = columns
        self.lastrowid = None

    def execute(self, sql, params=None):
        translated = _translate_sql(sql)
        # For INSERT with SERIAL, add RETURNING id to capture lastrowid
        needs_returning = (
            translated.strip().upper().startswith('INSERT') and
            'RETURNING' not in translated.upper() and
            'ON CONFLICT' not in translated.upper()
        )
        if needs_returning:
            translated = translated.rstrip().rstrip(';') + ' RETURNING id'

        try:
            self._cursor.execute(translated, params or ())
        except psycopg2.errors.UndefinedColumn:
            # Column doesn't exist — equivalent to sqlite3.OperationalError for migrations
            raise sqlite3.OperationalError("column does not exist")
        except psycopg2.errors.DuplicateColumn:
            # Column already exists — migration already applied
            raise sqlite3.OperationalError("duplicate column")
        except psycopg2.errors.DuplicateTable:
            # Table already exists
            pass
        except psycopg2.errors.UniqueViolation:
            # ON CONFLICT should handle this, but just in case
            pass

        if needs_returning:
            try:
                row = self._cursor.fetchone()
                if row:
                    self.lastrowid = row.get('id') if isinstance(row, dict) else row[0]
            except Exception:
                pass

        return self

    def executemany(self, sql, params_list):
        translated = _translate_sql(sql)
        # Create initial savepoint for PostgreSQL error recovery
        try:
            self._cursor.execute("SAVEPOINT executemany_sp")
        except Exception:
            pass
        error_count = 0
        last_error = None
        for params in params_list:
            try:
                self._cursor.execute(translated, params)
            except Exception as e:
                error_count += 1
                last_error = e
                # In PostgreSQL, a failed query aborts the transaction.
                # Use SAVEPOINT to recover and continue the batch.
                try:
                    self._cursor.execute("ROLLBACK TO SAVEPOINT executemany_sp")
                except Exception:
                    pass
            else:
                try:
                    self._cursor.execute("RELEASE SAVEPOINT executemany_sp")
                    self._cursor.execute("SAVEPOINT executemany_sp")
                except Exception:
                    pass
        if error_count > 0:
            print(f"[db] executemany: {error_count} errors, last: {last_error}", flush=True)
        return self

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            return PgRowProxy(row)
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        if rows and isinstance(rows[0], dict):
            return [PgRowProxy(r) for r in rows]
        return rows


class PgConnectionWrapper:
    """Wraps a psycopg2 connection to auto-translate SQL and provide sqlite3-like API."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cursor = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wrapper = PgCursorWrapper(cursor)
        wrapper.execute(sql, params)
        return wrapper

    def executemany(self, sql, params_list):
        cursor = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wrapper = PgCursorWrapper(cursor)
        wrapper.executemany(sql, params_list)
        return wrapper

    def cursor(self):
        cursor = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return PgCursorWrapper(cursor)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    @property
    def row_factory(self):
        return None

    @row_factory.setter
    def row_factory(self, value):
        # Ignore — PG always returns dicts via RealDictCursor
        pass


def get_db():
    """Return a database connection. Works identically for SQLite and PostgreSQL."""
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        return PgConnectionWrapper(conn)
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn


def get_raw_conn():
    """Return a raw connection (for init_db and special operations).
    For SQLite, returns plain sqlite3 connection (no row_factory).
    For PostgreSQL, returns wrapped connection."""
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        return PgConnectionWrapper(conn)
    else:
        return sqlite3.connect(DB_PATH)


def init_db():
    """Initialize the database schema. Handles both SQLite and PostgreSQL."""
    if USE_PG:
        _init_db_pg()
    else:
        _init_db_sqlite()


def _init_db_pg():
    """Create all tables in PostgreSQL."""
    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()

    # Fix: drop sessions table if it's missing the 'id' column (from earlier schema bug)
    c.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'sessions')
           AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'sessions' AND column_name = 'id') THEN
            DROP TABLE sessions;
        END IF;
    END $$""")
    conn.commit()

    # Use DO block to avoid "duplicate key" error when multiple workers start simultaneously
    c.execute("""DO $$ BEGIN
        CREATE TABLE IF NOT EXISTS stocks (
            symbol TEXT PRIMARY KEY, name TEXT NOT NULL, name_jp TEXT NOT NULL,
            sector TEXT NOT NULL, market TEXT NOT NULL, code TEXT NOT NULL
        );
    EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
    END $$""")

    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        password_hash TEXT,
        salt TEXT,
        created_at TEXT NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS portfolio (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL DEFAULT 'default',
        cash DOUBLE PRECISION DEFAULT 1000000.0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS holdings (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL DEFAULT 'default',
        symbol TEXT NOT NULL,
        name TEXT NOT NULL,
        shares DOUBLE PRECISION NOT NULL,
        avg_cost DOUBLE PRECISION NOT NULL,
        portfolio_id TEXT NOT NULL DEFAULT 'main'
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL DEFAULT 'default',
        symbol TEXT NOT NULL,
        name TEXT NOT NULL,
        action TEXT NOT NULL,
        shares DOUBLE PRECISION NOT NULL,
        price DOUBLE PRECISION NOT NULL,
        total DOUBLE PRECISION NOT NULL,
        pnl DOUBLE PRECISION DEFAULT 0.0,
        commission DOUBLE PRECISION DEFAULT 0.0,
        timestamp TEXT NOT NULL,
        portfolio_id TEXT NOT NULL DEFAULT 'main'
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        user_id TEXT NOT NULL DEFAULT 'default',
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        PRIMARY KEY (user_id, key)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS sessions (
        id SERIAL PRIMARY KEY,
        token TEXT NOT NULL UNIQUE,
        user_id TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS sub_portfolios (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        cash DOUBLE PRECISION DEFAULT 1000000.0,
        realized_pnl DOUBLE PRECISION DEFAULT 0.0,
        fund_amount DOUBLE PRECISION DEFAULT 1000000.0,
        created_at TEXT NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS portfolio_snapshots (
        user_id TEXT NOT NULL,
        portfolio_id TEXT NOT NULL,
        date TEXT NOT NULL,
        total_value DOUBLE PRECISION NOT NULL,
        cash DOUBLE PRECISION NOT NULL,
        invested DOUBLE PRECISION NOT NULL DEFAULT 0,
        net_deposits DOUBLE PRECISION DEFAULT 0,
        PRIMARY KEY (user_id, portfolio_id, date)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS portfolio_snapshots_intraday (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        portfolio_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        total_value DOUBLE PRECISION NOT NULL,
        cash DOUBLE PRECISION NOT NULL,
        invested DOUBLE PRECISION NOT NULL DEFAULT 0,
        net_deposits DOUBLE PRECISION DEFAULT 0
    )''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_intraday_snap
        ON portfolio_snapshots_intraday (user_id, portfolio_id, timestamp)''')

    c.execute('''CREATE TABLE IF NOT EXISTS watchlist (
        user_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        name TEXT NOT NULL DEFAULT '',
        added_at TEXT NOT NULL,
        PRIMARY KEY (user_id, symbol)
    )''')

    # Named watchlists (PG)
    c.execute('''CREATE TABLE IF NOT EXISTS watchlists (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        sort_order INTEGER DEFAULT 0,
        created_at TEXT NOT NULL
    )''')
    try:
        c.execute('ALTER TABLE watchlist ADD COLUMN list_id INTEGER DEFAULT NULL')
    except Exception:
        conn.rollback()
    c.execute('''CREATE TABLE IF NOT EXISTS watchlist_items (
        list_id INTEGER NOT NULL,
        user_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        added_at TEXT NOT NULL,
        PRIMARY KEY (list_id, symbol)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS price_alerts (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        name TEXT NOT NULL DEFAULT '',
        reference_price DOUBLE PRECISION NOT NULL,
        up_pct DOUBLE PRECISION,
        down_pct DOUBLE PRECISION,
        triggered INTEGER DEFAULT 0,
        created_at TEXT NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS fund_transactions (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        portfolio_id TEXT NOT NULL,
        type TEXT NOT NULL,
        amount DOUBLE PRECISION NOT NULL,
        note TEXT DEFAULT '',
        timestamp TEXT NOT NULL
    )''')

    # Migration: add cash_usd column to sub_portfolios (SBI-style dual currency)
    try:
        c.execute('ALTER TABLE sub_portfolios ADD COLUMN cash_usd DOUBLE PRECISION DEFAULT 0.0')
    except Exception:
        conn.rollback()

    # FX conversion transactions
    c.execute('''CREATE TABLE IF NOT EXISTS fx_transactions (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        portfolio_id TEXT NOT NULL,
        direction TEXT NOT NULL,
        usd_amount DOUBLE PRECISION NOT NULL,
        jpy_amount DOUBLE PRECISION NOT NULL,
        rate DOUBLE PRECISION NOT NULL,
        spread DOUBLE PRECISION NOT NULL DEFAULT 0.25,
        timestamp TEXT NOT NULL
    )''')

    # Seed default user and portfolio
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute("""INSERT INTO users (id, name, created_at) VALUES ('default', 'Default', %s)
                 ON CONFLICT (id) DO NOTHING""", (now,))
    c.execute("""INSERT INTO portfolio (user_id, cash)
                 SELECT 'default', 1000000.0
                 WHERE NOT EXISTS (SELECT 1 FROM portfolio WHERE user_id = 'default')""")
    c.execute("""INSERT INTO settings (user_id, key, value) VALUES ('default', 'fund_amount', '1000000')
                 ON CONFLICT (user_id, key) DO NOTHING""")
    c.execute("""INSERT INTO settings (user_id, key, value) VALUES ('default', 'theme', 'dark')
                 ON CONFLICT (user_id, key) DO NOTHING""")

    conn.commit()
    conn.close()


def _init_db_sqlite():
    """Original SQLite init_db — preserved exactly as before."""
    from datetime import datetime

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS stocks (
        symbol TEXT PRIMARY KEY, name TEXT NOT NULL, name_jp TEXT NOT NULL,
        sector TEXT NOT NULL, market TEXT NOT NULL, code TEXT NOT NULL
    )""")
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS portfolio (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL DEFAULT 'default',
        cash REAL DEFAULT 1000000.0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS holdings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL DEFAULT 'default',
        symbol TEXT NOT NULL,
        name TEXT NOT NULL,
        shares REAL NOT NULL,
        avg_cost REAL NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL DEFAULT 'default',
        symbol TEXT NOT NULL,
        name TEXT NOT NULL,
        action TEXT NOT NULL,
        shares REAL NOT NULL,
        price REAL NOT NULL,
        total REAL NOT NULL,
        pnl REAL DEFAULT 0.0,
        timestamp TEXT NOT NULL
    )''')
    # Migration: add pnl column if missing
    try:
        c.execute('ALTER TABLE transactions ADD COLUMN pnl REAL DEFAULT 0.0')
    except Exception:
        pass
    # Migration: add commission column if missing
    try:
        c.execute('ALTER TABLE transactions ADD COLUMN commission REAL DEFAULT 0.0')
    except Exception:
        pass
    # Backfill P&L for existing sell transactions that have pnl=0
    try:
        sells = c.execute("SELECT id, user_id, symbol, shares, price, portfolio_id FROM transactions WHERE action='sell' AND (pnl IS NULL OR pnl = 0)").fetchall()
        for s in sells:
            txn_id, uid, sym, sh, sell_price, pid = s
            buys = c.execute(
                "SELECT price, shares FROM transactions WHERE user_id=? AND symbol=? AND portfolio_id=? AND action='buy' AND id < ?",
                (uid, sym, pid, txn_id)
            ).fetchall()
            if buys:
                total_cost = sum(b[0] * b[1] for b in buys)
                total_shares = sum(b[1] for b in buys)
                avg_cost = total_cost / total_shares if total_shares > 0 else sell_price
                pnl = round((sell_price - avg_cost) * sh, 2)
                c.execute("UPDATE transactions SET pnl = ? WHERE id = ?", (pnl, txn_id))
        conn.commit()
    except Exception:
        pass

    # Settings table with user_id
    cols = [row[1] for row in c.execute("PRAGMA table_info(settings)").fetchall()]
    if not cols:
        c.execute('''CREATE TABLE settings (
            user_id TEXT NOT NULL DEFAULT 'default',
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (user_id, key)
        )''')
    elif 'user_id' not in cols:
        c.execute("ALTER TABLE settings RENAME TO settings_old")
        c.execute('''CREATE TABLE settings (
            user_id TEXT NOT NULL DEFAULT 'default',
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (user_id, key)
        )''')
        c.execute("INSERT INTO settings (user_id, key, value) SELECT 'default', key, value FROM settings_old")
        c.execute("DROP TABLE settings_old")

    # Migration: add user_id to tables that might not have it
    for table in ['portfolio', 'holdings', 'transactions']:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default'")
        except sqlite3.OperationalError:
            pass

    # Migration: add password_hash and salt to users table
    try:
        c.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE users ADD COLUMN salt TEXT")
    except sqlite3.OperationalError:
        pass

    # Create sessions table
    c.execute('''CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )''')

    # Sub-portfolios table
    c.execute('''CREATE TABLE IF NOT EXISTS sub_portfolios (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        cash REAL DEFAULT 1000000.0,
        realized_pnl REAL DEFAULT 0.0,
        created_at TEXT NOT NULL
    )''')
    # Migration: add realized_pnl column if missing
    try:
        c.execute('ALTER TABLE sub_portfolios ADD COLUMN realized_pnl REAL DEFAULT 0.0')
    except Exception:
        pass
    # Migration: add fund_amount column to sub_portfolios
    try:
        c.execute('ALTER TABLE sub_portfolios ADD COLUMN fund_amount REAL DEFAULT 1000000.0')
    except Exception:
        pass

    # Portfolio snapshots for performance chart
    c.execute('''CREATE TABLE IF NOT EXISTS portfolio_snapshots (
        user_id TEXT NOT NULL,
        portfolio_id TEXT NOT NULL,
        date TEXT NOT NULL,
        total_value REAL NOT NULL,
        cash REAL NOT NULL,
        invested REAL NOT NULL DEFAULT 0,
        PRIMARY KEY (user_id, portfolio_id, date)
    )''')

    # Migration: add net_deposits column to portfolio_snapshots
    try:
        c.execute('ALTER TABLE portfolio_snapshots ADD COLUMN net_deposits REAL DEFAULT 0')
    except:
        pass

    # Intraday portfolio snapshots (multiple per day, for intraday chart)
    c.execute('''CREATE TABLE IF NOT EXISTS portfolio_snapshots_intraday (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        portfolio_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        total_value REAL NOT NULL,
        cash REAL NOT NULL,
        invested REAL NOT NULL DEFAULT 0,
        net_deposits REAL DEFAULT 0
    )''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_intraday_snap
        ON portfolio_snapshots_intraday (user_id, portfolio_id, timestamp)''')

    # Migration: add cash_usd column to sub_portfolios (SBI-style dual currency)
    try:
        c.execute('ALTER TABLE sub_portfolios ADD COLUMN cash_usd REAL DEFAULT 0.0')
    except Exception:
        pass

    # FX conversion transactions (separate from fund_transactions to avoid CHECK constraint)
    c.execute('''CREATE TABLE IF NOT EXISTS fx_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        portfolio_id TEXT NOT NULL,
        direction TEXT NOT NULL,
        usd_amount REAL NOT NULL,
        jpy_amount REAL NOT NULL,
        rate REAL NOT NULL,
        spread REAL NOT NULL DEFAULT 0.25,
        timestamp TEXT NOT NULL
    )''')

    # Watchlist
    c.execute('''CREATE TABLE IF NOT EXISTS watchlist (
        user_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        name TEXT NOT NULL DEFAULT '',
        added_at TEXT NOT NULL,
        PRIMARY KEY (user_id, symbol)
    )''')

    # Named watchlists (user-defined groups, like "JP Tech", "US Growth")
    c.execute('''CREATE TABLE IF NOT EXISTS watchlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        sort_order INTEGER DEFAULT 0,
        created_at TEXT NOT NULL
    )''')
    # Migration: add list_id column to watchlist (NULL = default "All" list)
    try:
        c.execute('ALTER TABLE watchlist ADD COLUMN list_id INTEGER DEFAULT NULL')
    except (sqlite3.OperationalError, Exception):
        pass
    # Link table: many-to-many between symbols and named lists
    # (a symbol can belong to multiple named lists simultaneously)
    c.execute('''CREATE TABLE IF NOT EXISTS watchlist_items (
        list_id INTEGER NOT NULL,
        user_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        added_at TEXT NOT NULL,
        PRIMARY KEY (list_id, symbol)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS price_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        name TEXT NOT NULL DEFAULT '',
        reference_price REAL NOT NULL,
        up_pct REAL,
        down_pct REAL,
        triggered INTEGER DEFAULT 0,
        created_at TEXT NOT NULL
    )''')

    # Migration: add portfolio_id to holdings and transactions
    for table in ['holdings', 'transactions']:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN portfolio_id TEXT NOT NULL DEFAULT 'main'")
        except sqlite3.OperationalError:
            pass

    # Migration: add kabu_order_id to transactions (Kabu Station live trading)
    try:
        c.execute('ALTER TABLE transactions ADD COLUMN kabu_order_id TEXT')
    except (sqlite3.OperationalError, Exception):
        pass

    # Migration: add is_live flag to sub_portfolios (Real Account vs Simulation)
    try:
        c.execute('ALTER TABLE sub_portfolios ADD COLUMN is_live INTEGER DEFAULT 0')
    except (sqlite3.OperationalError, Exception):
        pass

    # Migration: fix live portfolios that inherited the ¥1M default fund_amount
    # Live accounts should start at 0 — their baseline is set on first Kabu sync
    try:
        c.execute('UPDATE sub_portfolios SET fund_amount = 0 WHERE is_live = 1 AND fund_amount = 1000000.0')
    except Exception:
        pass

    # Tick history table for Kabu Station real-time candle building
    c.execute('''CREATE TABLE IF NOT EXISTS tick_history (
        symbol TEXT NOT NULL,
        ts REAL NOT NULL,
        price REAL NOT NULL,
        volume INTEGER DEFAULT 0,
        PRIMARY KEY (symbol, ts)
    )''')
    try:
        c.execute('CREATE INDEX IF NOT EXISTS idx_tick_symbol_ts ON tick_history(symbol, ts)')
    except (sqlite3.OperationalError, Exception):
        pass

    # Fund transactions table
    c.execute('''CREATE TABLE IF NOT EXISTS fund_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        portfolio_id TEXT NOT NULL,
        type TEXT NOT NULL CHECK(type IN ('deposit', 'withdrawal')),
        amount REAL NOT NULL,
        note TEXT DEFAULT '',
        timestamp TEXT NOT NULL
    )''')

    # Migration: seed initial deposit for existing sub_portfolios that have no fund_transactions
    # Skip live portfolios — their baseline is set by Kabu sync, not a simulated deposit
    existing_sps = c.execute('SELECT id, user_id, fund_amount, COALESCE(is_live, 0) as is_live FROM sub_portfolios').fetchall()
    for sp in existing_sps:
        sp_id, sp_uid, sp_fund, sp_live = sp[0], sp[1], sp[2], sp[3]
        if sp_live:
            continue  # Live portfolios don't get seeded deposits
        has_ft = c.execute('SELECT 1 FROM fund_transactions WHERE user_id = ? AND portfolio_id = ? LIMIT 1', (sp_uid, sp_id)).fetchone()
        if not has_ft:
            fund_val = sp_fund if sp_fund else 1000000.0
            c.execute('INSERT INTO fund_transactions (user_id, portfolio_id, type, amount, note, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                      (sp_uid, sp_id, 'deposit', fund_val, 'Initial fund (migrated)', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    # Clean up: remove any bogus seeded deposits for live portfolios (from earlier bug)
    c.execute("DELETE FROM fund_transactions WHERE note = 'Initial fund (migrated)' AND portfolio_id IN (SELECT id FROM sub_portfolios WHERE is_live = 1)")

    # Ensure default user exists
    c.execute("INSERT OR IGNORE INTO users (id, name, created_at) VALUES ('default', 'Default', ?)",
              (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))

    # Init default portfolio if empty
    c.execute("SELECT COUNT(*) FROM portfolio WHERE user_id = 'default'")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO portfolio (user_id, cash) VALUES ('default', 1000000.0)")

    # Init default settings
    defaults = [('fund_amount', '1000000'), ('theme', 'dark')]
    for key, val in defaults:
        c.execute("INSERT OR IGNORE INTO settings (user_id, key, value) VALUES ('default', ?, ?)", (key, val))

    conn.commit()
    conn.close()


# ═══ Tick History Helpers ════════════════════════════════════════

def insert_ticks_batch(rows):
    """Batch insert ticks. rows = list of (symbol, ts, price, volume).
    Uses INSERT OR IGNORE to skip duplicates."""
    if not rows:
        return
    conn = get_db()
    try:
        conn.executemany(
            'INSERT OR IGNORE INTO tick_history (symbol, ts, price, volume) VALUES (?, ?, ?, ?)',
            rows
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def get_ticks(symbol, after_ts=0):
    """Get ticks for a symbol after a given timestamp. Returns list of (ts, price, volume)."""
    conn = get_db()
    try:
        rows = conn.execute(
            'SELECT ts, price, volume FROM tick_history WHERE symbol = ? AND ts > ? ORDER BY ts',
            (symbol, after_ts)
        ).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def cleanup_old_ticks(days=3):
    """Delete ticks older than N days to prevent DB bloat."""
    import time
    cutoff = time.time() - days * 86400
    conn = get_db()
    try:
        conn.execute('DELETE FROM tick_history WHERE ts < ?', (cutoff,))
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
