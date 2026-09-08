import os, time, sqlite3, threading, json
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string, redirect
import requests

APP = Flask(__name__)

DB = os.environ.get('EDO_DB', 'edo_market_alerts.db')
TWELVE_KEY = os.environ.get('TWELVE_DATA_API_KEY', '')
PUSHOVER_APP_TOKEN = os.environ.get('PUSHOVER_APP_TOKEN', '')
PUSHOVER_USER_KEY = os.environ.get('PUSHOVER_USER_KEY', '')
CHECK_SECONDS = int(os.environ.get('CHECK_SECONDS', '900'))

# -------------------------------------------------
# TWELVE DATA API PROTECTION
# -------------------------------------------------
# Twelve Data account limit seen by Edo: 8 credits/minute.
# EdoSignal deliberately stays below that with a conservative maximum
# of 5 REAL Twelve Data calls in any rolling 60-second window.
#
# IMPORTANT:
# The API limiter no longer uses SQLite. On Railway, Gunicorn workers
# could block each other on the SQLite database long enough for Gunicorn
# to kill a worker. The limiter now uses a tiny file + Linux file lock.
# This keeps the API protection shared between workers on the same service
# without holding the main EdoSignal database open.
TWELVE_CALL_LIMIT = int(os.environ.get('TWELVE_CALL_LIMIT', '45'))
TWELVE_CALL_WINDOW = 60.0

_API_LIMIT_FILE = os.environ.get(
    "EDO_API_LIMIT_FILE",
    os.path.abspath(DB) + ".api_limit.json"
)
_API_LOCK_FILE = _API_LIMIT_FILE + ".lock"
_LOCAL_API_LIMIT_LOCK = threading.Lock()

try:
    import fcntl
except ImportError:
    fcntl = None

# Shared in-process caches.
_OHLC_CACHE = {}
_PRICE_CACHE = {}
_CACHE_LOCK = threading.Lock()

# Manual Trend/Signal screens get priority over background scans.
_MANUAL_PRIORITY_UNTIL = 0.0
_MANUAL_PRIORITY_LOCK = threading.Lock()


def give_manual_api_priority(seconds=120):
    global _MANUAL_PRIORITY_UNTIL
    with _MANUAL_PRIORITY_LOCK:
        _MANUAL_PRIORITY_UNTIL = max(
            _MANUAL_PRIORITY_UNTIL,
            time.time() + float(seconds)
        )


def manual_api_priority_active():
    with _MANUAL_PRIORITY_LOCK:
        return time.time() < _MANUAL_PRIORITY_UNTIL


def _load_api_timestamps():
    try:
        with open(_API_LIMIT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return []
        return [float(x) for x in data]
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError, OSError):
        return []


def _save_api_timestamps(values):
    folder = os.path.dirname(_API_LIMIT_FILE)
    if folder:
        os.makedirs(folder, exist_ok=True)

    temp_file = _API_LIMIT_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(values, f)
        f.flush()
        os.fsync(f.fileno())

    os.replace(temp_file, _API_LIMIT_FILE)



class TwelveDataCoolingDown(Exception):
    """Raised when EdoSignal should not wait inside a web worker for API credit."""
    pass


def wait_for_twelve_credit(max_wait=2.0):
    """
    Shared rolling Twelve Data limiter.

    The important change is that EdoSignal will NOT sit inside a Gunicorn
    web request waiting 30-60 seconds for the next API slot. If the next
    slot is too far away it fails quickly, so Railway can keep serving
    the app instead of showing Server Error / 502.
    """
    started = time.time()

    while True:
        now = time.time()
        wait_seconds = 0.25

        with _LOCAL_API_LIMIT_LOCK:
            lock_handle = None
            try:
                folder = os.path.dirname(_API_LOCK_FILE)
                if folder:
                    os.makedirs(folder, exist_ok=True)

                lock_handle = open(_API_LOCK_FILE, "a+", encoding="utf-8")

                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)

                timestamps = _load_api_timestamps()
                cutoff = now - TWELVE_CALL_WINDOW
                timestamps = sorted(ts for ts in timestamps if ts >= cutoff)

                if len(timestamps) < TWELVE_CALL_LIMIT:
                    timestamps.append(now)
                    _save_api_timestamps(timestamps)
                    return True

                oldest = timestamps[0]
                wait_seconds = max(
                    0.25,
                    TWELVE_CALL_WINDOW - (now - oldest) + 0.25
                )

            finally:
                if lock_handle is not None:
                    try:
                        if fcntl is not None:
                            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                    finally:
                        lock_handle.close()

        elapsed = time.time() - started
        remaining = float(max_wait) - elapsed

        if remaining <= 0 or wait_seconds > remaining:
            raise TwelveDataCoolingDown(
                "Twelve Data limit is cooling down. Please try again shortly."
            )

        time.sleep(min(wait_seconds, remaining))


def twelve_get_json(endpoint, params, timeout=15):
    """All Twelve Data requests pass through the shared fail-fast limiter."""
    wait_for_twelve_credit(max_wait=2.0)
    r = requests.get(endpoint, params=params, timeout=timeout)
    return r.json()


def ohlc_cache_seconds(interval):
    # Signals use fully CLOSED candles, so these cache periods are safe and
    # prevent repeat downloads when the same page is opened several times.
    return {
        "1h": 120,
        "2h": 180,
        "4h": 240,
        "8h": 300,
        "12h": 360,
        "1day": 600,
        "1week": 900,
        "1month": 1800,
    }.get(interval, 180)


_HOME_PAGE_CACHE = {
    "saved_at": 0.0,
    "markets": [],
    "favorites": [],
    "trend_statuses": {},
    "trend_snapshots": {},
}
_HOME_PAGE_CACHE_LOCK = threading.Lock()


COLORS = {
    'FOREX': '#2980ff',
    'CRYPTO': '#9b59ff',
    'CFD': '#ff9f2f'
}

HTML = r'''
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Edo Market Alerts</title>

<style>
body{
    font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;
    background:#07111f;
    color:#eef6ff;
    margin:0;
}
.wrap{max-width:760px;margin:auto;padding:18px}
.card{
    background:#0d1b2a;
    border-radius:18px;
    padding:16px;
    margin:12px 0;
}
.row{display:flex;gap:8px;flex-wrap:wrap}
input,select,button{
    font-size:16px;
    border:0;
    border-radius:12px;
    padding:12px;
}
input,select{
    background:#13263b;
    color:#fff;
    flex:1;
}
button{
    background:#1fd1a5;
    font-weight:700;
    cursor:pointer;
}
.market{
    display:flex;
    justify-content:space-between;
    align-items:center;
    gap:8px;
    padding:12px;
    border-radius:14px;
    margin:8px 0;
    background:#12263b;
}
.pill{
    padding:5px 9px;
    border-radius:999px;
    font-size:12px;
    font-weight:800;
}
.small{color:#8ca7bf;font-size:13px}
.danger{background:#ff5e73;color:#fff}
.secondary{background:#23394f;color:#fff}
.favorite{background:#f2c94c;color:#07111f}
.saved-actions{
    display:flex;
    gap:6px;
    flex-wrap:nowrap;
    align-items:center;
    justify-content:flex-end;
}
.saved-actions button{
    padding:10px 11px;
    font-size:14px;
    white-space:nowrap;
}
.saved-market{
    flex-wrap:nowrap;
}
.saved-market > div:first-child{
    min-width:0;
}
.saved-market b{
    font-size:16px;
}
@media(max-width:600px){
    .saved-market{
        display:block;
        padding:12px;
    }
    .saved-market > div:first-child{
        width:100%;
        margin-bottom:10px;
        display:flex;
        align-items:center;
        gap:7px;
    }
    .saved-actions{
        width:100%;
        display:grid;
        grid-template-columns:0.85fr 1.15fr 1.35fr 1fr;
        gap:6px;
    }
    .saved-actions a{
        min-width:0;
    }
    .saved-actions button{
        width:100%;
        padding:9px 3px;
        font-size:11px;
        white-space:nowrap;
    }
    .saved-market .pill{
        font-size:10px;
        padding:4px 7px;
    }
    .saved-market b{
        font-size:17px;
        white-space:nowrap;
    }

    /* Non-FOREX rows only have USE, TREND and Delete. */
    .three-actions{
        grid-template-columns:0.85fr 1.25fr 1fr;
    }
    .forex-actions{
        grid-template-columns:0.8fr 1.15fr 1.35fr 1fr;
    }
}
h1{font-size:27px;margin-bottom:3px}
h2{font-size:18px}
.price{font-variant-numeric:tabular-nums;font-weight:700}
.status{font-size:12px;font-weight:800}
.fulltrend{font-size:12px;font-weight:900;margin-top:4px}

.alerttrend{
    margin-top:10px;
    padding:9px 10px;
    border:1px solid #294761;
    border-radius:12px;
    background:#0d2134;
}
.alerttrend-title{
    font-size:10px;
    color:#8ca7bf;
    font-weight:800;
    margin-bottom:7px;
}
.alerttrend-grid{
    display:grid;
    grid-template-columns:repeat(5,1fr);
    gap:6px;
    text-align:center;
}
.alerttrend-tf{
    font-size:10px;
    color:#b5c7d8;
    font-weight:800;
}
.alerttrend-state{
    font-size:10px;
    font-weight:900;
    margin-top:2px;
}
.alerttrend-bull{color:#35e28a}
.alerttrend-bear{color:#ff5f73}
.alerttrend-mixed{color:#f2c94c}
@media(max-width:700px){
    .alerttrend-grid{grid-template-columns:repeat(5,1fr);gap:3px}
    .alerttrend{padding:8px 6px}
    .alerttrend-tf,.alerttrend-state{font-size:9px}
}
.fullbull{color:#35e28a}
.fullbear{color:#ff6b7d}
.trendbtn{background:#5dade2;color:#07111f}
.livebtn{background:#f2c94c;color:#07111f}
.trend-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin-top:12px}
.trend-box{background:#12263b;border-radius:12px;padding:10px;text-align:center}
.trend-tf{font-size:12px;color:#8ca7bf;font-weight:800}
.trend-state{margin-top:5px;font-weight:800}
.trend-bull{color:#35e28a}
.trend-bear{color:#ff6b7d}
.trend-mixed{color:#f2c94c}
.trend-summary{font-size:22px;font-weight:900;margin-top:14px}
@media(max-width:600px){
    .trend-grid{grid-template-columns:1fr}
}
a{text-decoration:none}
</style>
</head>

<body>
<div class="wrap">

<h1>📈 EDO MARKET ALERTS</h1>
<div class="small">Forex • Crypto • CFD • Cloud price alarms</div>

<div class="card">
<h2>➕ Create Price Alert</h2>

<form method="post" action="/add">

<div class="row">
<input
    id="symbol"
    name="symbol"
    placeholder="USD/CAD or BTCUSD"
    value="{{ selected_symbol }}"
    required>

<select id="group" name="group">
<option {% if selected_group=='FOREX' %}selected{% endif %}>FOREX</option>
<option {% if selected_group=='CRYPTO' %}selected{% endif %}>CRYPTO</option>
<option {% if selected_group=='CFD' %}selected{% endif %}>CFD</option>
</select>
</div>

<div class="row" style="margin-top:8px">
<input name="target" type="number" step="any" placeholder="Target price" required>

<select name="direction">
<option value="ABOVE">ABOVE</option>
<option value="BELOW">BELOW</option>
</select>
<input name="note" type="text" placeholder="Note: WINNING, DANGER, Take Profit...">
<button>ARM</button>
</div>

</form>

<form method="post" action="/favorite/add" style="margin-top:10px">
<input type="hidden" id="fav_symbol" name="symbol">
<input type="hidden" id="fav_group" name="group">

<button class="favorite"
onclick="
document.getElementById('fav_symbol').value=document.getElementById('symbol').value;
document.getElementById('fav_group').value=document.getElementById('group').value;
">
⭐ SAVE PAIR
</button>
</form>

</div>


<div class="card">
<h2>⭐ Saved Pairs</h2>

{% if favorites %}

{% for f in favorites %}

<div class="market saved-market">

<div>
<span class="pill"
style="background:{{ colors[f['grp']] }}22;color:{{ colors[f['grp']] }}">
{{f['grp']}}
</span>

<b>{{f['symbol']}}</b>
</div>

<div class="saved-actions forex-actions">
<a href="/favorite/use/{{f['id']}}">
<button>USE</button>
</a>

<a href="/trend/{{f['id']}}">
<button class="trendbtn">📊 TREND</button>
</a>

<a href="/signal/{{f['id']}}">
<button class="livebtn">⚡ SIGNAL</button>
</a>

<a href="/favorite/delete/{{f['id']}}">
<button class="danger">Delete</button>
</a>
</div>

</div>

{% endfor %}

{% else %}

<div class="small">
No saved pairs yet. Enter a market above and press ⭐ SAVE PAIR.
</div>

{% endif %}

</div>


<div class="card">
<h2>🚨 Active Alerts</h2>

{% if markets %}

{% for m in markets %}

<div class="market">

<div>

<span class="pill"
style="background:{{ colors[m['grp']] }}22;color:{{ colors[m['grp']] }}">
{{m['grp']}}
</span>

<b>{{m['symbol']}}</b>

<div class="small">
Target {{m['direction']}} {{m['target']}}
</div>
{% if m['note'] %}
<div class="small">
📝 {{m['note']}}
</div>
{% endif %}

{% set snap = trend_snapshots.get(m['symbol']) %}
{% if m['triggered'] and snap %}
<div class="alerttrend">
    <div class="alerttrend-title">TREND • Weekly reference only</div>
    <div class="alerttrend-grid">
        {% for tf in ['Weekly','12H','8H','4H','1H'] %}
        {% set st = snap.get(tf, '') %}
        <div>
            <div class="alerttrend-tf">{{ 'W' if tf == 'Weekly' else tf }}</div>
            <div class="alerttrend-state
                {{ 'alerttrend-bull' if st == 'Bullish'
                   else 'alerttrend-bear' if st == 'Bearish'
                   else 'alerttrend-mixed' }}">
                {{ st if st else '—' }}
            </div>
        </div>
        {% endfor %}
    </div>
</div>
{% endif %}
</div>

<div style="text-align:right">

<div class="price">
{{m['last_price'] if m['last_price'] is not none else '—'}}
</div>

<div class="status">
{{'✅ TRIGGERED' if m['triggered'] else '🟢 ARMED'}}
</div>

{% set ts = trend_statuses.get(m['symbol']) %}
{% if m['triggered'] and ts == 'FULL BULLISH' %}
<div class="fulltrend fullbull">🟢 FULL BULLISH</div>
{% elif m['triggered'] and ts == 'FULL BEARISH' %}
<div class="fulltrend fullbear">🔴 FULL BEARISH</div>
{% endif %}

<div>
<a href="/reset/{{m['id']}}">
<button class="secondary">Reset</button>
</a>

<a href="/delete/{{m['id']}}">
<button class="danger">Delete</button>
</a>
</div>

</div>

</div>

{% endfor %}

{% else %}

<div class="small">No active alerts.</div>

{% endif %}

</div>


<div class="card">
<h2>🔔 Notification test</h2>

<a href="/test">
<button>Send test to iPhone</button>
</a>

<div class="small" style="margin-top:8px">
Use Pushover on your iPhone. Enable Pushover in Withings notifications for ScanWatch alerts.
</div>

</div>

</div>
</body>
</html>
'''


TREND_HTML = r"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ symbol }} Trend - Edo Market Alerts</title>
<style>
body{
    font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;
    background:#07111f;
    color:#eef6ff;
    margin:0;
}
.wrap{max-width:760px;margin:auto;padding:18px}
.card{
    background:#0d1b2a;
    border-radius:18px;
    padding:16px;
    margin:12px 0;
}
button{
    font-size:16px;
    border:0;
    border-radius:12px;
    padding:12px;
    background:#1fd1a5;
    font-weight:700;
    cursor:pointer;
}
a{text-decoration:none}
.small{color:#8ca7bf;font-size:13px}
.trend-grid{
    display:grid;
    grid-template-columns:repeat(6,1fr);
    gap:8px;
    margin-top:14px;
}
.trend-box{
    background:#12263b;
    border-radius:14px;
    padding:12px 8px;
    text-align:center;
}
.trend-tf{font-size:12px;color:#8ca7bf;font-weight:800}
.trend-state{margin-top:6px;font-weight:900}
.bull{color:#35e28a}
.bear{color:#ff6b7d}
.mixed{color:#f2c94c}
.summary{font-size:23px;font-weight:900;margin-top:16px}
.error{color:#ff8a96;font-weight:700}
@media(max-width:600px){
    .trend-grid{grid-template-columns:1fr}
}
</style>
</head>
<body>
<div class="wrap">
    <h1>📊 {{ symbol }} TREND</h1>
    <div class="small">{{ grp }} • Multi-timeframe trend scan</div>

    <div class="card">
    {% if error %}
        <div class="error">{{ error }}</div>
    {% else %}
        <div class="trend-grid">
        {% for item in results %}
            <div class="trend-box">
                <div class="trend-tf">{{ item['label'] }}</div>
                <div class="trend-state {{ item['css'] }}">
                    {% if not item.get('reference_only') %}{{ item['icon'] }} {% endif %}{{ item['state'] }}
                </div>
            </div>
        {% endfor %}
        </div>

        <div class="summary {{ summary_css }}">
            {{ summary_icon }} {{ summary }}
        </div>

        {% if detail %}
        <div class="small" style="margin-top:8px">{{ detail }}</div>
        {% endif %}

        <div class="small" style="margin-top:12px">
            Trend is calculated directly from candlestick colour: green candle = bullish, red candle = bearish
            and short-term momentum. Monthly is shown for your own reference but is excluded
            from the FULL BULLISH / FULL BEARISH signal. It is an analysis aid, not a guarantee
            of future price movement.
        </div>
    {% endif %}
    </div>

    <a href="/"><button>← Back to Market Alerts</button></a>
</div>
</body>
</html>
"""



SIGNAL_HTML = r"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ symbol }} Pattern Signal - Edo Market Alerts</title>
<style>
body{
    font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;
    background:#07111f;
    color:#eef6ff;
    margin:0;
}
.wrap{max-width:760px;margin:auto;padding:18px}
.card{
    background:#0d1b2a;
    border-radius:18px;
    padding:16px;
    margin:12px 0;
}
button{
    font-size:15px;
    border:0;
    border-radius:12px;
    padding:11px 13px;
    background:#1fd1a5;
    font-weight:800;
    cursor:pointer;
}
a{text-decoration:none}
.small{color:#8ca7bf;font-size:13px}
.signal{font-size:28px;font-weight:900;margin-top:10px}
.buy{color:#35e28a}
.sell{color:#ff6b7d}
.wait{color:#f2c94c}
.neutral{color:#8ca7bf}
.error{color:#ff8a96;font-weight:800}
.row{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.tfrow{display:flex;gap:7px;flex-wrap:wrap;margin:12px 0}
.tfbtn{background:#23394f;color:#eef6ff}
.tfactive{background:#1fd1a5;color:#07111f}
.pattern{
    background:#12263b;
    border-radius:14px;
    padding:13px;
    margin-top:10px;
}
.pattern-title{font-size:17px;font-weight:900}
.pattern-detail{font-size:13px;color:#a9bfd2;margin-top:5px;line-height:1.4}
.level{font-size:14px;font-weight:800;margin-top:7px}
.section-title{
    font-size:16px;
    font-weight:900;
    letter-spacing:.3px;
    margin-top:16px;
}
.newest-title{color:#f2c94c}
.previous-title{color:#8ca7bf}
.newest-pattern{
    border:2px solid #f2c94c;
    box-shadow:0 0 0 1px rgba(242,201,76,.08) inset;
}
.newest-badge{
    display:inline-block;
    background:#f2c94c;
    color:#07111f;
    padding:5px 9px;
    border-radius:999px;
    font-size:12px;
    font-weight:900;
    margin-bottom:8px;
}
.pricebox{
    display:flex;
    justify-content:space-between;
    gap:10px;
    background:#12263b;
    border-radius:14px;
    padding:12px;
    margin-top:10px;
}
.pricebig{font-size:22px;font-weight:900}
.badge{
    display:inline-block;
    padding:5px 9px;
    border-radius:999px;
    background:#23394f;
    font-size:12px;
    font-weight:900;
}
</style>
</head>
<body>
<div class="wrap">
    <h1>⚡ {{ symbol }} EDO SETUP SIGNAL</h1>
    <div class="small">{{ group }} • Your price-action method • Trend Pullback: 4H / 8H / 1D / 1W • Other patterns: 8H / 1D / 1W • Closed candles only • Manual trade decision</div>

    <div class="tfrow">
        {% for tf in timeframes %}
        <a href="/signal/{{ fav_id }}?tf={{ tf['value'] }}">
            <button class="{{ 'tfactive' if tf['value'] == selected_tf else 'tfbtn' }}">
                {{ tf['label'] }}
            </button>
        </a>
        {% endfor %}
    </div>

    <div class="card">
    {% if error %}
        <div class="error">{{ error }}</div>
    {% else %}
        <div class="pricebox">
            <div>
                <div class="small">Latest close</div>
                <div class="pricebig">{{ price }}</div>
            </div>
            <div style="text-align:right">
                <div class="small">Timeframe</div>
                <div class="badge">{{ selected_label }}</div>
            </div>
        </div>

        <div class="signal {{ signal_css }}">{{ signal_icon }} {{ signal }}</div>
        <div class="small" style="margin-top:6px">{{ summary }}</div>

        {% if patterns %}
            <div class="section-title newest-title">⚡ NEWEST TRIGGER</div>

            {% set newest = patterns[0] %}
            <div class="pattern newest-pattern">
                <div class="newest-badge">MOST RECENT VALID PATTERN</div>
                <div class="pattern-title {{ newest['css'] }}">
                    {{ newest['icon'] }} {{ newest['name'] }}
                </div>
                <div class="pattern-detail">{{ newest['detail'] }}</div>
                {% if newest['level_text'] %}
                <div class="level">{{ newest['level_text'] }}</div>
                {% endif %}
            </div>

            {% if patterns|length > 1 %}
                <div class="section-title previous-title">📚 PREVIOUS SETUPS</div>

                {% for p in patterns[1:] %}
                <div class="pattern">
                    <div class="pattern-title {{ p['css'] }}">{{ p['icon'] }} {{ p['name'] }}</div>
                    <div class="pattern-detail">{{ p['detail'] }}</div>
                    {% if p['level_text'] %}
                    <div class="level">{{ p['level_text'] }}</div>
                    {% endif %}
                </div>
                {% endfor %}
            {% endif %}
        {% else %}
            <div class="pattern">
                <div class="pattern-title neutral">No matching setup yet</div>
                <div class="pattern-detail">
                    No recent candle sequence matches your bounce/retest, 2+ candle trend-pullback,
                    or range-reversal confirmation rules on this timeframe.
                </div>
            </div>
        {% endif %}

        <div class="small" style="margin-top:12px">
            Updated: {{ updated }}. “Newest Trigger” means the most recent valid completed
            pattern found on this selected timeframe. Older matches are kept below as
            Previous Setups. The scanner uses closed candles only and does not place trades.
        </div>
    {% endif %}
    </div>

    <div class="row">
        <a href="/signal/{{ fav_id }}?tf={{ selected_tf }}&refresh=1"><button>↻ Scan Again</button></a>
        <a href="/"><button>← Back to Market Alerts</button></a>
    </div>
</div>
</body>
</html>
"""


def db_conn():
    c = sqlite3.connect(
        DB,
        timeout=5,
        check_same_thread=False
    )
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=3000")
    return c


def init_db():
    with db_conn() as c:
        # WAL allows the home page to READ saved data while background
        # threads perform short WRITES. This greatly reduces lock errors.
        try:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.OperationalError as e:
            print("SQLite WAL setup warning:", e)


        c.execute('''
        CREATE TABLE IF NOT EXISTS alerts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            grp TEXT,
            direction TEXT,
            target REAL,
            triggered INTEGER DEFAULT 0,
            last_price REAL,
            created TEXT,
            note TEXT
        )
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS favorites(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            grp TEXT NOT NULL,
            UNIQUE(symbol,grp)
        )
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS trend_status(
            symbol TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT '',
            updated TEXT
        )
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS trend_snapshot(
            symbol TEXT PRIMARY KEY,
            weekly TEXT,
            h12 TEXT,
            h8 TEXT,
            h4 TEXT,
            h1 TEXT,
            updated TEXT
        )
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS pattern_notifications(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            pattern_name TEXT NOT NULL,
            direction TEXT NOT NULL,
            confirmation_date TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            UNIQUE(symbol, interval, pattern_name, direction, confirmation_date)
        )
        ''')
        c.execute("""
        CREATE TABLE IF NOT EXISTS pattern_monitor_state(
            grp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            last_closed_date TEXT NOT NULL DEFAULT '',
            updated TEXT,
            PRIMARY KEY(grp, symbol, interval)
        )
        """)

        try:
            c.execute("ALTER TABLE alerts ADD COLUMN note TEXT")
        except sqlite3.OperationalError:
            pass
        c.commit()


def reset_old_signal_history():
    """
    Start a fresh EdoSignal signal-test period.

    Clears only EdoSignal's stored trend/pattern notification history.
    Saved pairs and normal price alerts are kept.
    Existing notifications already inside the Pushover app cannot be deleted
    by this script.
    """
    with db_conn() as c:
        c.execute("DELETE FROM trend_status")
        c.execute("DELETE FROM pattern_notifications")
        c.commit()


def send_push(title, msg):

    if not PUSHOVER_APP_TOKEN or not PUSHOVER_USER_KEY:
        print('Pushover not configured:', title, msg)
        return

    try:
        requests.post(
            'https://api.pushover.net/1/messages.json',
            data={
                'token': PUSHOVER_APP_TOKEN,
                'user': PUSHOVER_USER_KEY,
                'title': title,
                'message': msg,
                'sound': 'cashregister'
            },
            timeout=10
        )

    except Exception as e:
        print('push error', e)


def latest_price(symbol, grp=None):
    if not TWELVE_KEY:
        return None

    cache_key = (grp or "", symbol.upper().strip())
    now = time.time()

    with _CACHE_LOCK:
        cached = _PRICE_CACHE.get(cache_key)
        if cached and now - cached["saved_at"] < 30:
            return cached["price"]

    try:
        j = twelve_get_json(
            'https://api.twelvedata.com/price',
            {
                'symbol': twelve_symbol(symbol, grp),
                'apikey': TWELVE_KEY
            },
            timeout=10
        )

        if j.get("status") == "error":
            print("price API error", symbol, j.get("message", "Unknown error"))
            return None

        if 'price' not in j:
            return None

        price = float(j['price'])
        with _CACHE_LOCK:
            _PRICE_CACHE[cache_key] = {"saved_at": now, "price": price}
        return price

    except TwelveDataCoolingDown:
        return None
    except Exception as e:
        print('price error', symbol, e)
        return None



PATTERN_SIGNAL_CACHE = {}
PATTERN_SIGNAL_CACHE_SECONDS = 60

# -------------------------------------------------
# CLEAN START / FROM-NOW BASELINES
# -------------------------------------------------
# Legacy in-memory baseline containers retained for compatibility.
# Persistent notification state is now stored in SQLite.
TREND_BASELINED = set()
PATTERN_BASELINE_CLOSED = {}

# Edo's higher-timeframe setup scanner.
# Supported saved-market groups: FOREX, CRYPTO, CFD.
# The scanner does NOT place trades. It only finds setups for manual review.
PATTERN_TIMEFRAMES = [
    {"label": "4H", "value": "4h"},
    {"label": "8H", "value": "8h"},
    {"label": "1D", "value": "1day"},
    {"label": "1W", "value": "1week"},
]

# Only Trend Pullback is active below 8H.
CORE_PATTERN_INTERVALS = {"8h", "1day", "1week"}


def get_ohlc(symbol, interval, outputsize=140, grp=None):
    """Download OHLC candles from Twelve Data, oldest -> newest, with shared caching."""
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    cache_key = (grp or "", symbol.upper().strip(), interval)
    now = time.time()
    ttl = ohlc_cache_seconds(interval)

    with _CACHE_LOCK:
        cached = _OHLC_CACHE.get(cache_key)
        if cached and now - cached["saved_at"] < ttl and len(cached["candles"]) >= outputsize:
            return cached["candles"][-outputsize:], None

    try:
        j = twelve_get_json(
            "https://api.twelvedata.com/time_series",
            {
                "symbol": twelve_symbol(symbol, grp),
                "interval": interval,
                "outputsize": outputsize,
                "apikey": TWELVE_KEY,
                "format": "JSON",
            },
            timeout=15
        )

        if j.get("status") == "error":
            return None, j.get("message", "Twelve Data returned an error.")

        values = j.get("values") or []
        candles = []
        for row in reversed(values):
            try:
                candles.append({
                    "datetime": row.get("datetime", ""),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                })
            except (KeyError, TypeError, ValueError):
                pass

        if len(candles) < 40:
            return None, f"Not enough {interval} candle history returned."

        with _CACHE_LOCK:
            old = _OHLC_CACHE.get(cache_key)
            if not old or len(candles) >= len(old["candles"]):
                _OHLC_CACHE[cache_key] = {"saved_at": now, "candles": candles}

        return candles, None

    except TwelveDataCoolingDown:
        return None, "API cooling down — please try again in a few seconds."
    except Exception as e:
        print("setup scanner data error", symbol, interval, e)
        return None, "Could not download setup data."


def swing_points(candles, kind="low", left=2, right=2):
    """Return clear local swing highs/lows, excluding edge candles."""
    points = []
    key = "low" if kind == "low" else "high"

    for i in range(left, len(candles) - right):
        value = candles[i][key]
        before = [candles[j][key] for j in range(i-left, i)]
        after = [candles[j][key] for j in range(i+1, i+right+1)]

        if kind == "low":
            if value <= min(before) and value <= min(after):
                points.append((i, value))
        else:
            if value >= max(before) and value >= max(after):
                points.append((i, value))

    return points


def candle_colour(c):
    if c["close"] > c["open"]:
        return "green"
    if c["close"] < c["open"]:
        return "red"
    return "flat"


def avg_range(candles, end=None, length=20):
    if end is None:
        end = len(candles)
    part = candles[max(0, end-length):end]
    if not part:
        return 0.0
    return sum(max(0.0, c["high"] - c["low"]) for c in part) / len(part)


def avg_body(candles, end=None, length=20):
    if end is None:
        end = len(candles)
    part = candles[max(0, end-length):end]
    if not part:
        return 0.0
    return sum(abs(c["close"] - c["open"]) for c in part) / len(part)


def confirmation_at(candles, i):
    """
    Edo confirmation:
      bullish = green candle closes >50% back through previous red body
      bearish = red candle closes >50% back through previous green body
    """
    if i <= 0 or i >= len(candles):
        return None

    prev = candles[i-1]
    curr = candles[i]
    prev_colour = candle_colour(prev)
    curr_colour = candle_colour(curr)
    body = abs(prev["close"] - prev["open"])

    if body <= 0:
        return None

    midpoint = (prev["open"] + prev["close"]) / 2.0

    if prev_colour == "red" and curr_colour == "green" and curr["close"] > midpoint:
        penetration = ((curr["close"] - prev["close"]) / body) * 100.0
        if penetration >= 50.0:
            return {
                "direction": "bullish",
                "index": i,
                "penetration": penetration,
                "date": curr.get("datetime", ""),
                "close": curr["close"],
                "previous_midpoint": midpoint,
            }

    if prev_colour == "green" and curr_colour == "red" and curr["close"] < midpoint:
        penetration = ((prev["close"] - curr["close"]) / body) * 100.0
        if penetration >= 50.0:
            return {
                "direction": "bearish",
                "index": i,
                "penetration": penetration,
                "date": curr.get("datetime", ""),
                "close": curr["close"],
                "previous_midpoint": midpoint,
            }

    return None


def recent_confirmations(candles, lookback=6):
    found = []
    first = max(1, len(candles) - lookback)

    for i in range(first, len(candles)):
        c = confirmation_at(candles, i)
        if c:
            found.append(c)

    # Newest first.
    return list(reversed(found))


def local_structure_trend(candles, end_index):
    """
    Simple price-structure direction before a pullback.
    Uses recent close progress plus swing structure. No EMA/RSI.
    """
    if end_index < 12:
        return "mixed"

    start = max(0, end_index - 28)
    part = candles[start:end_index]
    if len(part) < 10:
        return "mixed"

    first_close = sum(c["close"] for c in part[:4]) / 4.0
    last_close = sum(c["close"] for c in part[-4:]) / 4.0
    move = (last_close - first_close) / first_close if first_close else 0.0

    highs = swing_points(part, "high")
    lows = swing_points(part, "low")

    hh = len(highs) >= 2 and highs[-1][1] > highs[-2][1]
    hl = len(lows) >= 2 and lows[-1][1] > lows[-2][1]
    lh = len(highs) >= 2 and highs[-1][1] < highs[-2][1]
    ll = len(lows) >= 2 and lows[-1][1] < lows[-2][1]

    if (hh and hl) or move > 0.004:
        return "bullish"
    if (lh and ll) or move < -0.004:
        return "bearish"
    return "mixed"


def previous_target(candles, direction, before_index, search_back=60):
    """
    BUY target = previous significant swing high.
    SELL target = previous significant swing low.
    """
    start = max(0, before_index - search_back)
    part = candles[start:before_index+1]
    if len(part) < 5:
        return None

    if direction == "bullish":
        pts = swing_points(part, "high")
        if pts:
            candidates = [p[1] for p in pts]
            above = [v for v in candidates if v > candles[before_index]["close"]]
            if above:
                return min(above)
            return max(candidates)
    else:
        pts = swing_points(part, "low")
        if pts:
            candidates = [p[1] for p in pts]
            below = [v for v in candidates if v < candles[before_index]["close"]]
            if below:
                return max(below)
            return min(candidates)

    return None


def detect_bounce_retest(candles, conf):
    """
    Edo Bounce / Retest Zone rule.

    A valid setup needs an ESTABLISHED support/resistance area first:

      RESISTANCE / possible SELL
        1) Price previously reacts from a wick-defined resistance area.
        2) Price moves clearly away from that area.
        3) Later, price returns and RETESTS the same resistance zone.
        4) A bearish/red candle must then FULLY CLOSE back away from/below the zone.
        5) The confirmation candle must also satisfy Edo's existing >=50%
           previous-candle BODY close confirmation rule.

      SUPPORT / possible BUY
        1) Price previously reacts from a wick-defined support area.
        2) Price moves clearly away from that area.
        3) Later, price returns and RETESTS the same support zone.
        4) A bullish/green candle must then FULLY CLOSE back away from/above the zone.
        5) The confirmation candle must also satisfy Edo's existing >=50%
           previous-candle BODY close confirmation rule.

    A colour change in the middle of nowhere is NOT a Bounce / Retest setup.
    The previously established level, move-away, return/retest and closed
    confirmation are all required.
    """
    i = conf["index"]
    direction = conf["direction"]

    # Need enough history to establish a genuine earlier level.
    if i < 16:
        return None

    ar = avg_range(candles, i + 1, 20)
    if ar <= 0:
        return None

    # Recent retest area must be immediately before/around the confirmation.
    touch_start = max(3, i - 4)
    touch_end = i + 1
    touch_slice = candles[touch_start:touch_end]
    if not touch_slice:
        return None

    if direction == "bullish":
        # Support retest: recent lowest wick defines second touch.
        rel_touch_i = min(range(len(touch_slice)), key=lambda k: touch_slice[k]["low"])
        retest_price = touch_slice[rel_touch_i]["low"]
        retest_index = touch_start + rel_touch_i
        swings = swing_points(candles[:touch_start], "low")
        level_source = "lower wick"

        # Confirmation must close back ABOVE the support-zone centre.
        confirm_close = candles[i]["close"]

    else:
        # Resistance retest: recent highest wick defines second touch.
        rel_touch_i = max(range(len(touch_slice)), key=lambda k: touch_slice[k]["high"])
        retest_price = touch_slice[rel_touch_i]["high"]
        retest_index = touch_start + rel_touch_i
        swings = swing_points(candles[:touch_start], "high")
        level_source = "upper wick"

        # Confirmation must close back BELOW the resistance-zone centre.
        confirm_close = candles[i]["close"]

    # Yellow-area concept: use an adaptive zone rather than one exact price.
    # The zone is tight enough to represent the same level but wide enough
    # to allow normal wick variation.
    zone_tolerance = max(ar * 0.55, abs(retest_price) * 0.0020)

    candidates = []

    for old_i, old_price in swings:
        separation = touch_start - old_i

        # Must be a genuine later retest, not just adjacent noise.
        if separation < 8 or separation > 120:
            continue

        # First touch and second touch must be inside the same zone.
        if abs(old_price - retest_price) > zone_tolerance:
            continue

        between = candles[old_i + 1:touch_start]
        if not between:
            continue

        if direction == "bullish":
            # After first support touch, price must have moved materially UP
            # before later returning to retest that support area.
            moved_away = max(c["high"] for c in between) - max(old_price, retest_price)
        else:
            # After first resistance touch, price must have moved materially DOWN
            # before later returning to retest that resistance area.
            moved_away = min(old_price, retest_price) - min(c["low"] for c in between)

        # Require a real departure from the level before the retest.
        if moved_away < ar * 2.0:
            continue

        zone_centre = (old_price + retest_price) / 2.0

        # The confirmation close must be AWAY from the zone in the expected direction.
        if direction == "bullish":
            if confirm_close <= zone_centre:
                continue
        else:
            if confirm_close >= zone_centre:
                continue

        closeness = 1.0 - min(
            1.0,
            abs(old_price - retest_price) / zone_tolerance
        )

        candidates.append(
            (old_i, old_price, separation, closeness, moved_away, zone_centre)
        )

    if not candidates:
        return None

    # Prefer the cleanest matching established zone.
    old_i, old_price, separation, closeness, moved_away, zone_centre = max(
        candidates,
        key=lambda x: (x[3], x[2])
    )

    target = previous_target(candles, direction, i)

    score = 6.0
    score += min(3.0, separation / 15.0)
    score += closeness * 2.0
    score += min(2.0, max(0.0, conf["penetration"] - 50.0) / 25.0)

    return {
        "name": "BOUNCE / RETEST SETUP",
        "direction": direction,
        "confirmed": True,
        "score": score,
        "confirmation_date": conf["date"],
        "confirmation_close": conf["close"],
        "penetration": conf["penetration"],
        "level": zone_centre,
        "old_level": old_price,
        "retest_price": retest_price,
        "retest_date": candles[retest_index].get("datetime", ""),
        "level_source": level_source,
        "old_date": candles[old_i].get("datetime", ""),
        "separation": separation,
        "weak_retest": False,
        "target": target,
    }


def count_same_colour_before(candles, i, colour):
    count = 0
    j = i - 1

    while j >= 0 and candle_colour(candles[j]) == colour:
        count += 1
        j -= 1

    return count, j + 1


def detect_trend_pullback(candles, conf):
    """
    Edo Trend Pullback rule.

    BEARISH trend / possible SELL:
      1) Minimum 2 consecutive bullish candles pull upward.
      2) Next candle is bearish.
      3) That bearish candle must be FULLY CLOSED.
      4) Its close must reach at least 50% DOWN through the BODY
         of the immediately previous bullish candle.
      5) If it closes less than 50% through that previous body,
         the setup is INVALID and must NOT trigger.

    BULLISH trend / possible BUY:
      1) Minimum 2 consecutive bearish candles pull downward.
      2) Next candle is bullish.
      3) That bullish candle must be FULLY CLOSED.
      4) Its close must reach at least 50% UP through the BODY
         of the immediately previous bearish candle.
      5) If it closes less than 50% through that previous body,
         the setup is INVALID and must NOT trigger.

    Wicks do not count toward the 50% calculation; candle BODY only.
    No forming candle may trigger a signal.
    """
    i = conf["index"]
    if i < 2:
        return None

    direction = conf["direction"]
    confirm_candle = candles[i]
    prev = candles[i - 1]

    prev_open = float(prev["open"])
    prev_close = float(prev["close"])
    confirm_close = float(confirm_candle["close"])

    prev_body_high = max(prev_open, prev_close)
    prev_body_low = min(prev_open, prev_close)
    prev_body_size = prev_body_high - prev_body_low

    if prev_body_size <= 0:
        return None

    if direction == "bearish":
        # SELL setup: 2+ bullish pullback candles, then bearish confirmation.
        run_colour = "green"
        required_trend = "bearish"
        run_count, run_start = count_same_colour_before(candles, i, run_colour)

        if run_count < 2:
            return None

        # 50% point measured downward through previous bullish body.
        fifty_level = prev_body_high - (prev_body_size * 0.50)

        # Bearish confirmation must close at or below the halfway point.
        if confirm_close > fifty_level:
            return None

        penetration = ((prev_body_high - confirm_close) / prev_body_size) * 100.0

    elif direction == "bullish":
        # BUY setup: 2+ bearish pullback candles, then bullish confirmation.
        run_colour = "red"
        required_trend = "bullish"
        run_count, run_start = count_same_colour_before(candles, i, run_colour)

        if run_count < 2:
            return None

        # 50% point measured upward through previous bearish body.
        fifty_level = prev_body_low + (prev_body_size * 0.50)

        # Bullish confirmation must close at or above the halfway point.
        if confirm_close < fifty_level:
            return None

        penetration = ((confirm_close - prev_body_low) / prev_body_size) * 100.0

    else:
        return None

    trend = local_structure_trend(candles, run_start)

    # Trend Pullback must agree with the established trend.
    if trend != required_trend:
        return None

    score = 6.0 + min(3.0, (run_count - 2) * 0.75)
    score += min(2.0, max(0.0, penetration - 50.0) / 25.0)

    target = previous_target(candles, direction, run_start)

    return {
        "name": "TREND PULLBACK SETUP",
        "direction": direction,
        "confirmed": True,
        "score": score,
        "confirmation_date": conf["date"],
        "confirmation_close": confirm_close,
        "penetration": penetration,
        "run_count": run_count,
        "run_colour": run_colour,
        "trend": trend,
        "context": "trend",
        "target": target,
        "fifty_percent_level": fifty_level,
    }


def describe_setup(p):
    bullish = p["direction"] == "bullish"
    icon = "🟢" if bullish else "🔴"
    css = "buy" if bullish else "sell"
    direction_word = "Bullish" if bullish else "Bearish"

    if p["name"] == "BOUNCE / RETEST SETUP":
        weak_text = " Weakness was also detected in the retest candles." if p["weak_retest"] else ""
        detail = (
            f"{direction_word} Bounce/Retest confirmation on {p['confirmation_date']}. "
            f"Price first established a wick-defined support/resistance zone, moved clearly away, "
            f"then returned to RETEST the same zone after {p['separation']} candles. "
            f"The opposite-colour confirmation candle fully closed away from the zone and "
            f"{p['penetration']:.0f}% through the previous candle BODY."
        )

        level_text = (
            f"Established retest zone: {p['level']:.5f} • "
            f"Earlier wick level: {p['old_level']:.5f} on {p['old_date']} • "
            f"Second {p['level_source']} test: {p['retest_price']:.5f} on {p['retest_date']} • "
            f"Confirmation close: {p['confirmation_close']:.5f}"
        )

    elif p["name"] == "TREND PULLBACK SETUP":
        detail = (
            f"{direction_word} trend-pullback confirmation on {p['confirmation_date']}. "
            f"{p['run_count']} {p['run_colour']} CLOSED candles pulled against the larger "
            f"{p['trend']} price structure, then the opposite-colour confirmation candle "
            f"closed {p['penetration']:.0f}% through the BODY of the immediately previous "
            f"pullback candle. Minimum required: 50%."
        )

        level_text = f"Confirmation close: {p['confirmation_close']:.5f}"

    else:
        detail = (
            f"{direction_word} range-reversal confirmation on {p['confirmation_date']}. "
            f"{p['run_count']} {p['run_colour']} CLOSED candles moved in one direction "
            f"while local structure was mixed/range-bound, then the confirmation candle "
            f"closed {p['penetration']:.0f}% back through the previous candle body."
        )

        level_text = f"Confirmation close: {p['confirmation_close']:.5f}"

    if p.get("target") is not None:
        target_word = "previous structural high" if bullish else "previous structural low"
        level_text += f" • Review target ({target_word}): {p['target']:.5f}"

    return {
        "name": p["name"],
        "detail": detail,
        "level_text": level_text,
        "icon": icon,
        "css": css,
        "direction": p["direction"],
        "confirmed": True,
        "score": p.get("score", 0),
    }


def build_pattern_signal(symbol, interval, grp="FOREX", force_refresh=False):
    cache_key = f"{grp}|{symbol}|{interval}"
    now = time.time()
    cached = PATTERN_SIGNAL_CACHE.get(cache_key)

    if cached and not force_refresh and now - cached["saved_at"] < PATTERN_SIGNAL_CACHE_SECONDS:
        return cached["data"], None

    candles, error = get_ohlc(symbol, interval, outputsize=140, grp=grp)

    if error:
        if "credits" in error.lower() or "limit" in error.lower():
            return None, "Twelve Data is temporarily busy. EdoSignal is protecting your API limit. Wait a moment and press Scan Again."
        return None, error

    # Ignore the newest API candle because it may still be forming.
    # Pattern setups are confirmed from fully CLOSED candles only.
    closed_candles = candles[:-1]

    if len(closed_candles) < 40:
        return None, f"Not enough fully closed {interval} candle history returned."

    found = []
    confirmations = recent_confirmations(closed_candles, lookback=7)

    for conf in confirmations:
        retest = detect_bounce_retest(closed_candles, conf)
        if retest:
            found.append(retest)

        pullback = detect_trend_pullback(closed_candles, conf)
        if pullback:
            found.append(pullback)

    # Avoid duplicate descriptions of the same setup/direction/confirmation.
    unique = {}
    for p in found:
        key = (p["name"], p["direction"], p["confirmation_date"])
        if key not in unique or p.get("score", 0) > unique[key].get("score", 0):
            unique[key] = p

    found = list(unique.values())

    # Edo rule: on 4H, ONLY Trend Pullback is active.
    # Bounce/Retest and Range Reversal stay 8H + 1D + 1W.
    if interval not in CORE_PATTERN_INTERVALS:
        found = [
            p for p in found
            if p.get("name") == "TREND PULLBACK SETUP"
        ]

    found.sort(
        key=lambda p: (
            p.get("confirmation_date", ""),
            p.get("score", 0)
        ),
        reverse=True
    )

    bullish = [p for p in found if p["direction"] == "bullish"]
    bearish = [p for p in found if p["direction"] == "bearish"]

    if bullish and not bearish:
        signal = "BULLISH SETUP DETECTED"
        icon, css = "🟢", "buy"
        summary = (
            "Your candle-close rules found a bullish setup. Review the chart yourself "
            "before deciding whether to trade."
        )
    elif bearish and not bullish:
        signal = "BEARISH SETUP DETECTED"
        icon, css = "🔴", "sell"
        summary = (
            "Your candle-close rules found a bearish setup. Review the chart yourself "
            "before deciding whether to trade."
        )
    elif bullish and bearish:
        # If both exist, favour a clearly stronger/recent setup only when the score
        # difference is meaningful; otherwise show mixed.
        best_bull = max(bullish, key=lambda p: p.get("score", 0))
        best_bear = max(bearish, key=lambda p: p.get("score", 0))
        diff = best_bull.get("score", 0) - best_bear.get("score", 0)

        if diff >= 2.0:
            signal = "BULLISH SETUP DETECTED"
            icon, css = "🟢", "buy"
            summary = "Bullish evidence is stronger, but a bearish setup also exists. Review the chart."
        elif diff <= -2.0:
            signal = "BEARISH SETUP DETECTED"
            icon, css = "🔴", "sell"
            summary = "Bearish evidence is stronger, but a bullish setup also exists. Review the chart."
        else:
            signal = "MIXED SETUPS"
            icon, css = "🟡", "wait"
            summary = "Bullish and bearish setup evidence are both present. Review the chart and wait for clarity."
    else:
        signal = "NO EDO SETUP YET"
        icon, css = "⚪", "neutral"
        summary = (
            "No recent setup matches your retest, 2+ candle trend-pullback, or range-reversal confirmation rules "
            "on this timeframe."
        )

    data = {
        "price": candles[-1]["close"],
        "signal": signal,
        "signal_icon": icon,
        "signal_css": css,
        "summary": summary,
        "patterns": [describe_setup(p) for p in found[:4]],
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    PATTERN_SIGNAL_CACHE[cache_key] = {"saved_at": now, "data": data}
    return data, None



def notify_new_pattern_setups(symbol, interval, patterns, latest_closed_date, grp="FOREX"):
    """
    Persist the last processed CLOSED candle in SQLite so restarts/redeploys
    do not suppress the next genuine signal. Historical candles are not replayed.
    """
    if not latest_closed_date:
        return

    with db_conn() as c:
        row = c.execute(
            """
            SELECT last_closed_date
            FROM pattern_monitor_state
            WHERE grp=? AND symbol=? AND interval=?
            """,
            (grp, symbol, interval)
        ).fetchone()

        previous_closed = row["last_closed_date"] if row else None

        # First-ever observation after installing this version: create one
        # persistent baseline so historical setups are not replayed.
        if previous_closed is None:
            c.execute(
                """
                INSERT INTO pattern_monitor_state(
                    grp, symbol, interval, last_closed_date, updated
                ) VALUES(?,?,?,?,?)
                """,
                (grp, symbol, interval, latest_closed_date, datetime.utcnow().isoformat())
            )
            c.commit()
            return

        if latest_closed_date == previous_closed:
            return

        # A genuinely new fully closed candle has appeared. Save the state
        # before sending so a restart cannot cause the same candle to replay.
        c.execute(
            """
            UPDATE pattern_monitor_state
            SET last_closed_date=?, updated=?
            WHERE grp=? AND symbol=? AND interval=?
            """,
            (latest_closed_date, datetime.utcnow().isoformat(), grp, symbol, interval)
        )
        c.commit()

    if not patterns:
        return

    tf_labels = {x["value"]: x["label"] for x in PATTERN_TIMEFRAMES}
    tf_label = tf_labels.get(interval, interval)

    for p in patterns:
        confirmation_date = p.get("confirmation_date", "")
        if not confirmation_date or confirmation_date != latest_closed_date:
            continue

        try:
            with db_conn() as c:
                c.execute(
                    """
                    INSERT INTO pattern_notifications(
                        symbol, interval, pattern_name, direction,
                        confirmation_date, sent_at
                    ) VALUES(?,?,?,?,?,?)
                    """,
                    (
                        symbol, interval, p["name"], p["direction"],
                        confirmation_date, datetime.utcnow().isoformat()
                    )
                )
                c.commit()
        except sqlite3.IntegrityError:
            continue

        bullish = p["direction"] == "bullish"
        icon = "🟢" if bullish else "🔴"
        direction_word = "BULLISH" if bullish else "BEARISH"

        send_push(
            f"{icon} {symbol} [{grp}] — {direction_word} EDO SETUP",
            (
                f"{p['name']} confirmed on the NEWEST CLOSED {tf_label} candle "
                f"({confirmation_date}). {direction_word} possibility. "
                f"Review the chart before trading."
            )
        )


def collect_closed_pattern_setups(symbol, interval, grp="FOREX"):
    """
    Collect raw Bounce/Retest, Trend Pullback, and Range Reversal setups from closed candles only.

    Returns:
      setups, latest_closed_date, error
    """
    candles, error = get_ohlc(symbol, interval, outputsize=140, grp=grp)
    if error:
        return None, None, error
    if not candles or len(candles) < 41:
        return None, None, f"Not enough fully closed {interval} candle history returned."

    # The newest API candle may still be forming, so exclude it.
    closed_candles = candles[:-1]
    latest_closed_date = closed_candles[-1].get("datetime", "")
    found = []

    for conf in recent_confirmations(closed_candles, lookback=7):
        retest = detect_bounce_retest(closed_candles, conf)
        if retest:
            found.append(retest)

        pullback = detect_trend_pullback(closed_candles, conf)
        if pullback:
            found.append(pullback)

    unique = {}
    for p in found:
        key = (p["name"], p["direction"], p["confirmation_date"])
        if key not in unique or p.get("score", 0) > unique[key].get("score", 0):
            unique[key] = p

    setups = list(unique.values())

    # Edo rule: on 4H, ONLY Trend Pullback may notify.
    if interval not in CORE_PATTERN_INTERVALS:
        setups = [
            p for p in setups
            if p.get("name") == "TREND PULLBACK SETUP"
        ]

    return setups, latest_closed_date, None


def pattern_signal_monitor():
    """
    Background pattern scanner for all saved FOREX, CRYPTO, and CFD markets.
    Checks one symbol/timeframe combination every five minutes.
    """
    time.sleep(150)
    index = 0

    while True:
        try:
            if manual_api_priority_active():
                time.sleep(15)
                continue

            with db_conn() as c:
                rows = c.execute(
                    "SELECT symbol, grp FROM favorites WHERE grp IN ('FOREX','CRYPTO','CFD') ORDER BY grp,symbol"
                ).fetchall()

            jobs = []
            for row in rows:
                for tf in PATTERN_TIMEFRAMES:
                    jobs.append((row["symbol"], row["grp"], tf["value"]))

            if jobs:
                if index >= len(jobs):
                    index = 0

                symbol, grp, interval = jobs[index]
                index = (index + 1) % len(jobs)

                setups, latest_closed_date, error = collect_closed_pattern_setups(
                    symbol, interval, grp
                )

                if error:
                    print("pattern monitor error", symbol, interval, error)
                else:
                    notify_new_pattern_setups(
                        symbol,
                        interval,
                        setups,
                        latest_closed_date,
                        grp
                    )

        except Exception as e:
            print("pattern signal monitor error", e)

        time.sleep(300)


def get_daily_candles_for_alignment(symbol, outputsize=1800, grp=None):
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    try:
        j = twelve_get_json(
            "https://api.twelvedata.com/time_series",
            {
                "symbol": twelve_symbol(symbol, grp),
                "interval": "1day",
                "outputsize": outputsize,
                "apikey": TWELVE_KEY,
                "format": "JSON",
            },
            timeout=20
        )

        if j.get("status") == "error":
            return None, j.get("message", "Twelve Data returned an error.")

        values = j.get("values") or []
        rows = []

        for row in reversed(values):
            try:
                dt = datetime.fromisoformat(row["datetime"])
                rows.append((dt, float(row["close"])))
            except Exception:
                pass

        if len(rows) < 300:
            return None, "Not enough daily history for higher-timeframe alignment."

        return rows, None

    except Exception as e:
        print("daily alignment error", symbol, e)
        return None, "Could not download daily alignment data."


def resample_closes(rows, mode):
    buckets = {}

    for dt, close in rows:
        if mode == "week":
            year, week, _ = dt.isocalendar()
            key = (year, week)
        else:
            key = (dt.year, dt.month)

        buckets[key] = close

    return list(buckets.values())


def save_trend_snapshot(symbol, states):
    """Persist latest closed-candle states for Active Alerts display."""
    with db_conn() as c:
        c.execute(
            """
            INSERT INTO trend_snapshot(symbol,weekly,h12,h8,h4,h1,updated)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
                weekly=excluded.weekly,
                h12=excluded.h12,
                h8=excluded.h8,
                h4=excluded.h4,
                h1=excluded.h1,
                updated=excluded.updated
            """,
            (
                symbol,
                states.get("Weekly", ""),
                states.get("12H", ""),
                states.get("8H", ""),
                states.get("4H", ""),
                states.get("1H", ""),
                datetime.utcnow().isoformat(),
            )
        )
        c.commit()


def build_full_alignment(symbol, grp=None):
    """
    Edo direct-candlestick alignment signal.

    TRIGGER timeframes:
      12H + 8H + 4H + 1H

    Weekly:
      display/reference only. It NEVER affects FULL BULLISH / FULL BEARISH.

    All decisions use the last fully CLOSED candle.
    """
    intervals = {
        "12H": "12h",
        "8H": "8h",
        "4H": "4h",
        "1H": "1h",
    }

    signal_states = {}

    for label, interval in intervals.items():
        candles, err = get_candles(symbol, interval, outputsize=3, grp=grp)
        if err:
            return "", err

        closed = last_closed_candle(candles)
        if closed is None:
            return "", f"Not enough completed {label} candle data."

        signal_states[label] = analyse_candle(closed)

    # Weekly is reference-only. If Weekly data fails, the real trigger
    # still works from 12H + 8H + 4H + 1H.
    weekly_state = ""
    weekly_candles, weekly_err = get_candles(symbol, "1week", outputsize=3, grp=grp)
    if not weekly_err:
        weekly_closed = last_closed_candle(weekly_candles)
        if weekly_closed is not None:
            weekly_state = analyse_candle(weekly_closed)

    save_trend_snapshot(
        symbol,
        {"Weekly": weekly_state, **signal_states}
    )

    if all(v == "Bullish" for v in signal_states.values()):
        return "FULL BULLISH", None

    if all(v == "Bearish" for v in signal_states.values()):
        return "FULL BEARISH", None

    return "", None


def save_trend_status(symbol, status):
    """Save trend status and return the previous saved status."""
    with db_conn() as c:
        row = c.execute(
            "SELECT status FROM trend_status WHERE symbol=?",
            (symbol,)
        ).fetchone()
        previous = row["status"] if row else None

        c.execute(
            """
            INSERT INTO trend_status(symbol,status,updated)
            VALUES(?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
                status=excluded.status,
                updated=excluded.updated
            """,
            (symbol, status, datetime.utcnow().isoformat())
        )
        c.commit()

    return previous


def active_trend_monitor():
    # Start two minutes after boot, then keep TRIGGERED alert trend data fresh.
    time.sleep(120)
    index = 0

    while True:
        try:
            if manual_api_priority_active():
                time.sleep(15)
                continue

            with db_conn() as c:
                rows = c.execute(
                    "SELECT DISTINCT symbol, grp FROM alerts WHERE triggered=1 ORDER BY symbol"
                ).fetchall()

            markets = [(r["symbol"], r["grp"]) for r in rows]

            if markets:
                if index >= len(markets):
                    index = 0

                symbol, grp = markets[index]
                index = (index + 1) % len(markets)

                status, error = build_full_alignment(symbol, grp)

                if error:
                    print("active trend error", symbol, error)
                else:
                    # trend_status is persistent in SQLite, so restart/redeploy
                    # does not erase the previously processed trend state.
                    previous = save_trend_status(symbol, status)

                    if status in ("FULL BULLISH", "FULL BEARISH") and status != previous:
                        icon = "🟢" if status == "FULL BULLISH" else "🔴"
                        direction = "bullish" if status == "FULL BULLISH" else "bearish"
                        send_push(
                            f"{icon} {symbol} — {status}",
                            f"All 4 last CLOSED signal candles are {direction}: 12H, 8H, 4H, 1H. "
                            f"Weekly is display-only. CFD markets may use an ETF proxy for trend data."
                        )

        except Exception as e:
            print("active trend monitor error", e)

        time.sleep(300)


TREND_INTERVALS = [
    ("1M", "1month"),   # DISPLAY ONLY - excluded from alignment signal
    ("1W", "1week"),
    ("1D", "1day"),
    ("8H", "8h"),
    ("4H", "4h"),
    ("1H", "1h"),
]


def twelve_symbol(symbol, grp=None):
    """
    Convert the symbols Edo normally types into Twelve Data format.

    FOREX:
        EURUSD  -> EUR/USD
        EUR/USD -> EUR/USD

    CRYPTO:
        BTCUSD -> BTC/USD
        ETHUSD -> ETH/USD
        SOLUSD -> SOL/USD

    CFD / INDEX broker aliases:
        US500 / SP500 -> SPY
        NAS100 / US100 -> QQQ
        US30 / DJ30 -> DIA
        GER40 / DE40 -> DAX
        UK100 -> FTSE
        FRA40 -> FCHI
        JPN225 -> N225
        EU50 -> STOXX50E

    The CFD mapping converts common broker names to the underlying index
    symbol used by market-data providers. Availability can still depend on
    the user's Twelve Data plan.
    """
    s = symbol.upper().strip().replace(" ", "")

    # Keep already formatted currency pairs unchanged.
    if "/" in s:
        return s

    # Common broker CFD/index aliases.
    cfd_aliases = {
        # Use liquid US-listed ETF proxies for the US indices because
        # they are much more reliable on Twelve Data Basic than direct CFD/index feeds.
        "US500": "SPY",
        "SP500": "SPY",
        "S&P500": "SPY",
        "SPX500": "SPY",

        "US30": "DIA",
        "DJ30": "DIA",
        "DOW30": "DIA",

        # Nasdaq-100 proxy.
        "NAS100": "QQQ",
        "NASDAQ100": "QQQ",
        "US100": "QQQ",
        "USTEC": "QQQ",

        # Keep international aliases for later testing; availability may vary by plan.
        "GER40": "DAX",
        "DE40": "DAX",
        "GER30": "DAX",
        "UK100": "FTSE",
        "FTSE100": "FTSE",
        "FRA40": "FCHI",
        "FR40": "FCHI",
        "JPN225": "N225",
        "JP225": "N225",
        "EU50": "STOXX50E",
        "EUSTX50": "STOXX50E",
    }

    if grp == "CFD" and s in cfd_aliases:
        return cfd_aliases[s]

    # If group is not supplied, still recognise the common CFD aliases.
    if s in cfd_aliases:
        return cfd_aliases[s]

    # Crypto:
    # Keep the app simple: enter compact USD pairs such as BTCUSD, ETHUSD,
    # SOLUSD, XRPUSD. The app converts them to Twelve Data slash format.
    if grp == "CRYPTO":
        for quote in ("AUD", "EUR", "GBP", "USD", "BTC", "ETH"):
            if s.endswith(quote) and len(s) > len(quote):
                return s[:-len(quote)] + "/" + quote

    # Compact six-letter forex pairs such as EURUSD or AUDJPY.
    if grp == "FOREX" and len(s) == 6 and s.isalpha():
        return s[:3] + "/" + s[3:]

    # Group-less fallback for common compact USD crypto symbols.
    if grp is None and s.endswith("USD") and len(s) > 3:
        return s[:-3] + "/USD"

    return s


def _aggregate_4h_to_12h(candles_4h):
    """
    Build synthetic 12H candles from Twelve Data 4H candles.

    Twelve Data does not provide a native 12h interval on this plan.
    We combine 3 consecutive 4H candles into one 12H candle, aligned to
    00:00-12:00 and 12:00-24:00 using the timestamps returned by Twelve Data.

    The newest synthetic 12H candle may still be forming, which is fine because
    last_closed_candle() always ignores the newest candle for signal decisions.
    """
    buckets = {}

    for c in candles_4h:
        dt_text = c.get("datetime", "")
        try:
            dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        bucket_hour = 0 if dt.hour < 12 else 12
        bucket_key = dt.replace(hour=bucket_hour, minute=0, second=0, microsecond=0)

        if bucket_key not in buckets:
            buckets[bucket_key] = []
        buckets[bucket_key].append(c)

    synthetic = []

    for bucket_key in sorted(buckets):
        group = sorted(buckets[bucket_key], key=lambda x: x["datetime"])

        # A complete 12H candle contains exactly three 4H candles.
        # Keep an incomplete newest bucket too, so last_closed_candle()
        # can safely skip it if it is still forming.
        if len(group) < 1:
            continue

        synthetic.append({
            "datetime": bucket_key.strftime("%Y-%m-%d %H:%M:%S"),
            "open": group[0]["open"],
            "high": max(x["high"] for x in group),
            "low": min(x["low"] for x in group),
            "close": group[-1]["close"],
            "_parts": len(group),
        })

    return synthetic


def get_candles(symbol, interval, outputsize=60, grp=None):
    """
    Return OHLC candles oldest -> newest using the same protected shared cache
    as the setup scanner.

    12H is synthetic because Twelve Data does not support a native 12h interval:
    it is built from 3 x 4H candles.

    The newest API/synthetic candle may still be forming. Signal logic still uses
    last_closed_candle(), so live candles never trigger a signal.
    """
    if interval == "12h":
        # Fetch enough 4H candles to construct the requested 12H history.
        source_size = max(60, outputsize * 3 + 9)
        candles_4h, error = get_ohlc(
            symbol,
            "4h",
            outputsize=source_size,
            grp=grp
        )
        if error:
            return None, error

        candles_12h = _aggregate_4h_to_12h(candles_4h)

        # We need at least two synthetic candles because last_closed_candle()
        # deliberately ignores the newest one.
        if len(candles_12h) < 2:
            return None, "Not enough 4H candle history to build completed 12H candles."

        return candles_12h[-outputsize:], None

    candles, error = get_ohlc(symbol, interval, outputsize=max(40, outputsize), grp=grp)
    if error:
        return None, error
    return candles[-outputsize:], None


def last_closed_candle(candles):
    """
    Return the last fully completed candle.

    Twelve Data can include the currently forming candle as the newest item.
    Because candles are stored oldest -> newest, candles[-1] may still move.
    We deliberately use candles[-2] so EdoSignal cannot trigger from a live candle.
    """
    if not candles or len(candles) < 2:
        return None
    return candles[-2]


def analyse_candle(candle):
    """
    Direct candlestick direction only.

    Green candle: close > open  -> Bullish
    Red candle:   close < open  -> Bearish
    Doji/flat:    close == open -> Mixed
    """
    if candle["close"] > candle["open"]:
        return "Bullish"
    if candle["close"] < candle["open"]:
        return "Bearish"
    return "Mixed"


def analyse_closes(candles):
    """
    Kept under the old function name so the rest of the app stays simple.
    It now analyses ONLY the last fully closed candlestick colour -- no SMA20/SMA50.
    """
    closed = last_closed_candle(candles)
    if closed is None:
        return "Mixed"
    return analyse_candle(closed)

def build_trend_scan(symbol, grp=None):
    results = []

    state_info = {
        "Bullish": ("🟢", "bull"),
        "Bearish": ("🔴", "bear"),
        "Mixed": ("🟡", "mixed"),
    }

    # Weekly is DISPLAY ONLY for reference.
    # It does NOT affect FULL BULLISH / FULL BEARISH or Pushover triggering.
    reference_intervals = [
        ("Weekly", "1week"),
    ]

    # These FOUR timeframes alone control the Full Trend signal.
    signal_intervals = [
        ("12H", "12h"),
        ("8H", "8h"),
        ("4H", "4h"),
        ("1H", "1h"),
    ]

    states = {}

    # Weekly first: coloured text only, no red/green dot.
    for label, interval in reference_intervals:
        candles, error = get_candles(symbol, interval, outputsize=3, grp=grp)

        if error:
            return None, error

        closed = last_closed_candle(candles)
        if closed is None:
            return None, f"Not enough completed {label} candle data."

        state = analyse_candle(closed)
        _, css = state_info[state]

        results.append({
            "label": label,
            "interval": interval,
            "state": state,
            "icon": "",
            "css": css,
            "reference_only": True,
        })

    # Existing signal rows stay unchanged.
    for label, interval in signal_intervals:
        candles, error = get_candles(symbol, interval, outputsize=3, grp=grp)

        if error:
            return None, error

        closed = last_closed_candle(candles)
        if closed is None:
            return None, f"Not enough completed {label} candle data."

        state = analyse_candle(closed)
        states[label] = state
        icon, css = state_info[state]

        results.append({
            "label": label,
            "interval": interval,
            "state": state,
            "icon": icon,
            "css": css,
            "reference_only": False,
        })

    signal_labels = ("12H", "8H", "4H", "1H")
    weights = {"12H": 4, "8H": 3, "4H": 2, "1H": 1}
    score = 0

    for label in signal_labels:
        if states[label] == "Bullish":
            score += weights[label]
        elif states[label] == "Bearish":
            score -= weights[label]

    full_bull = all(states[x] == "Bullish" for x in signal_labels)
    full_bear = all(states[x] == "Bearish" for x in signal_labels)

    if full_bull:
        summary = "FULL BULLISH"
        icon, css = "🟢", "bull"
        detail = "Last CLOSED candles on 12H, 8H, 4H and 1H are all GREEN. Bullish possibility."
    elif full_bear:
        summary = "FULL BEARISH"
        icon, css = "🔴", "bear"
        detail = "Last CLOSED candles on 12H, 8H, 4H and 1H are all RED. Bearish possibility."
    elif states["12H"] == "Bullish" and any(
        states[x] == "Bearish" for x in ("8H", "4H", "1H")
    ):
        summary = "BULLISH — LOWER-TIMEFRAME PULLBACK"
        icon, css = "🟡", "mixed"
        detail = "12H is bullish, but one or more lower signal timeframes are pulling back."
    elif states["12H"] == "Bearish" and any(
        states[x] == "Bullish" for x in ("8H", "4H", "1H")
    ):
        summary = "BEARISH — LOWER-TIMEFRAME BOUNCE"
        icon, css = "🟡", "mixed"
        detail = "12H is bearish, but one or more lower signal timeframes are bouncing."
    elif score >= 6:
        summary = "BULLISH"
        icon, css = "🟢", "bull"
        detail = "12H, 8H, 4H and 1H lean bullish."
    elif score <= -6:
        summary = "BEARISH"
        icon, css = "🔴", "bear"
        detail = "12H, 8H, 4H and 1H lean bearish."
    else:
        summary = "MIXED / WAIT"
        icon, css = "🟡", "mixed"
        detail = "12H, 8H, 4H and 1H are not aligned strongly enough."

    return {
        "results": results,
        "summary": summary,
        "summary_icon": icon,
        "summary_css": css,
        "detail": detail,
        "score": score,
    }, None


def monitor():

    while True:

        try:
            if manual_api_priority_active():
                time.sleep(15)
                continue

            with db_conn() as c:

                rows = c.execute(
                    'SELECT * FROM alerts WHERE triggered=0'
                ).fetchall()
                price_cache = {}
                for a in rows:

                    if a['symbol'] not in price_cache:
                        price_cache[a['symbol']] = latest_price(a['symbol'], a['grp'])
                    p = price_cache[a['symbol']]

                    if p is None:
                        continue

                    c.execute(
                        'UPDATE alerts SET last_price=? WHERE id=?',
                        (p, a['id'])
                    )

                    if not a['triggered']:

                        hit = (
                            a['direction'] == 'ABOVE'
                            and p >= a['target']
                        ) or (
                            a['direction'] == 'BELOW'
                            and p <= a['target']
                        )

                        if hit:

                            c.execute(
                                'UPDATE alerts SET triggered=1 WHERE id=?',
                                (a['id'],)
                            )
                            c.commit()

                            # PRE-ENTRY TREND CHECK:
                            # Refresh immediately when the price alert triggers.
                            # build_full_alignment() uses only fully CLOSED candles.
                            fresh_status = ""
                            trend_error = None

                            try:
                                fresh_status, trend_error = build_full_alignment(
                                    a['symbol'], a['grp']
                                )

                                if not trend_error:
                                    save_trend_status(a['symbol'], fresh_status)

                            except Exception as trend_exc:
                                trend_error = str(trend_exc)
                                print(
                                    "trigger-time trend refresh error",
                                    a['symbol'],
                                    trend_exc
                                )

                            if fresh_status == "FULL BULLISH":
                                trend_line = "Trend now: 🟢 FULL BULLISH"
                            elif fresh_status == "FULL BEARISH":
                                trend_line = "Trend now: 🔴 FULL BEARISH"
                            elif trend_error:
                                trend_line = "Trend now: refresh temporarily unavailable"
                            else:
                                trend_line = "Trend now: 🟡 MIXED"

                            send_push(
                                f"🚨 {a['symbol']} PRICE ALERT",
                                f"{a['symbol']} is {p}\n"
                                f"Target: {a['direction']} {a['target']}\n"
                                f"{trend_line}\n"
                                f"12H + 8H + 4H + 1H use CLOSED candles only.\n"
                                f"Weekly is reference only.\n"
                                f"Note: {a['note'] or '-'}"
                            )

                c.commit()

        except Exception as e:
            print('monitor error', e)

        time.sleep(CHECK_SECONDS)


@APP.route('/')
def home():
    """
    Fast home screen:
    - database/cache only
    - no Twelve Data request
    - no waiting for API credit
    """
    selected_symbol = request.args.get('symbol', '')
    selected_group = request.args.get('group', 'FOREX')

    try:
        with db_conn() as c:
            markets = c.execute(
                'SELECT * FROM alerts ORDER BY grp,symbol'
            ).fetchall()

            favorites = c.execute(
                'SELECT * FROM favorites ORDER BY grp,symbol'
            ).fetchall()

            trend_rows = c.execute(
                'SELECT symbol,status FROM trend_status'
            ).fetchall()

            trend_statuses = {
                r['symbol']: r['status']
                for r in trend_rows
            }

            snapshot_rows = c.execute(
                'SELECT symbol,weekly,h12,h8,h4,h1,updated FROM trend_snapshot'
            ).fetchall()

            trend_snapshots = {
                r['symbol']: {
                    'Weekly': r['weekly'] or '',
                    '12H': r['h12'] or '',
                    '8H': r['h8'] or '',
                    '4H': r['h4'] or '',
                    '1H': r['h1'] or '',
                    'updated': r['updated'] or ''
                }
                for r in snapshot_rows
            }

        with _HOME_PAGE_CACHE_LOCK:
            _HOME_PAGE_CACHE["saved_at"] = time.time()
            _HOME_PAGE_CACHE["markets"] = markets
            _HOME_PAGE_CACHE["favorites"] = favorites
            _HOME_PAGE_CACHE["trend_statuses"] = trend_statuses
            _HOME_PAGE_CACHE["trend_snapshots"] = trend_snapshots

    except sqlite3.OperationalError as e:
        print("home SQLite busy; serving cached screen:", e)

        with _HOME_PAGE_CACHE_LOCK:
            markets = _HOME_PAGE_CACHE["markets"]
            favorites = _HOME_PAGE_CACHE["favorites"]
            trend_statuses = dict(_HOME_PAGE_CACHE["trend_statuses"])
            trend_snapshots = dict(_HOME_PAGE_CACHE["trend_snapshots"])

    return render_template_string(
        HTML,
        markets=markets,
        favorites=favorites,
        colors=COLORS,
        selected_symbol=selected_symbol,
        selected_group=selected_group,
        trend_statuses=trend_statuses,
        trend_snapshots=trend_snapshots
    )


@APP.post('/add')
def add():

    symbol = request.form['symbol'].upper().strip()
    grp = request.form['group']
    direction = request.form['direction']
    target = float(request.form['target'])
    note = request.form.get('note', '').strip()
    with db_conn() as c:

        c.execute(
            '''
            INSERT INTO alerts(
symbol,grp,direction,target,created,note
            )
            VALUES(?,?,?,?,?,?)
            ''',
            (
                symbol,
                grp,
                direction,
                target,
datetime.utcnow().isoformat(),
note
            )
        )

        c.commit()

    return redirect('/')


@APP.post('/favorite/add')
def favorite_add():

    symbol = request.form.get('symbol', '').upper().strip()
    grp = request.form.get('group', 'FOREX')

    if symbol:

        with db_conn() as c:

            c.execute(
                'INSERT OR IGNORE INTO favorites(symbol,grp) VALUES(?,?)',
                (symbol, grp)
            )

            c.commit()

    return redirect('/')


@APP.route('/favorite/use/<int:i>')
def favorite_use(i):

    with db_conn() as c:

        f = c.execute(
            'SELECT * FROM favorites WHERE id=?',
            (i,)
        ).fetchone()

    if not f:
        return redirect('/')

    return redirect(
        '/?symbol=' +
        requests.utils.quote(f['symbol']) +
        '&group=' +
        requests.utils.quote(f['grp'])
    )




@APP.route('/signal/<int:i>')
def signal(i):
    # Manual screen request gets priority over background API scans.
    give_manual_api_priority(90)

    with db_conn() as c:
        f = c.execute(
            'SELECT * FROM favorites WHERE id=?',
            (i,)
        ).fetchone()

    if not f or f['grp'] not in ('FOREX', 'CRYPTO', 'CFD'):
        return redirect('/')

    allowed = {x["value"]: x["label"] for x in PATTERN_TIMEFRAMES}
    selected_tf = request.args.get("tf", "8h")

    if selected_tf not in allowed:
        selected_tf = "8h"

    force_refresh = request.args.get('refresh') == '1'
    data, error = build_pattern_signal(
        f['symbol'],
        selected_tf,
        grp=f['grp'],
        force_refresh=force_refresh
    )

    if not error:
        setups, latest_closed_date, setup_error = collect_closed_pattern_setups(
            f['symbol'],
            selected_tf,
            f['grp']
        )
        if setup_error:
            print("manual pattern notification error", f['symbol'], selected_tf, setup_error)
        else:
            notify_new_pattern_setups(
                f['symbol'],
                selected_tf,
                setups,
                latest_closed_date,
                f['grp']
            )


    if error:
        return render_template_string(
            SIGNAL_HTML,
            symbol=f['symbol'],
            group=f['grp'],
            fav_id=f['id'],
            timeframes=PATTERN_TIMEFRAMES,
            selected_tf=selected_tf,
            selected_label=allowed[selected_tf],
            price='—',
            signal='',
            signal_icon='',
            signal_css='neutral',
            summary='',
            patterns=[],
            updated='—',
            error=error
        )

    return render_template_string(
        SIGNAL_HTML,
        symbol=f['symbol'],
        group=f['grp'],
        fav_id=f['id'],
        timeframes=PATTERN_TIMEFRAMES,
        selected_tf=selected_tf,
        selected_label=allowed[selected_tf],
        price=f"{data['price']:.5f}",
        signal=data['signal'],
        signal_icon=data['signal_icon'],
        signal_css=data['signal_css'],
        summary=data['summary'],
        patterns=data['patterns'],
        updated=data['updated'],
        error=''
    )


@APP.route('/trend/<int:i>')
def trend(i):
    # Manual screen request gets priority over background API scans.
    give_manual_api_priority(90)

    with db_conn() as c:
        f = c.execute(
            'SELECT * FROM favorites WHERE id=?',
            (i,)
        ).fetchone()

    if not f:
        return redirect('/')

    scan, error = build_trend_scan(f['symbol'], f['grp'])

    if error:
        return render_template_string(
            TREND_HTML,
            symbol=f['symbol'],
            grp=f['grp'],
            results=[],
            summary='',
            summary_icon='',
            summary_css='mixed',
            detail='',
            error=error
        )

    return render_template_string(
        TREND_HTML,
        symbol=f['symbol'],
        grp=f['grp'],
        results=scan['results'],
        summary=scan['summary'],
        summary_icon=scan['summary_icon'],
        summary_css=scan['summary_css'],
        detail=scan['detail'],
        error=''
    )


@APP.route('/favorite/delete/<int:i>')
def favorite_delete(i):

    with db_conn() as c:

        c.execute(
            'DELETE FROM favorites WHERE id=?',
            (i,)
        )

        c.commit()

    return redirect('/')


@APP.route('/reset/<int:i>')
def reset(i):

    with db_conn() as c:

        c.execute(
            'UPDATE alerts SET triggered=0 WHERE id=?',
            (i,)
        )

        c.commit()

    return redirect('/')


@APP.route('/delete/<int:i>')
def delete(i):

    with db_conn() as c:

        c.execute(
            'DELETE FROM alerts WHERE id=?',
            (i,)
        )

        c.commit()

    return redirect('/')


@APP.route('/test')
def test():

    send_push(
        '🔔 EDO MARKET ALERT TEST',
        'Your cloud market alerts can reach this iPhone while the screen is locked.'
    )

    return redirect('/')



@APP.errorhandler(500)
def internal_error(error):
    print("HTTP 500:", error)
    return """
    <!doctype html>
    <html>
    <head>
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <title>EdoSignal temporary error</title>
      <style>
        body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#07111f;color:#eef6ff;margin:0}
        .wrap{max-width:700px;margin:auto;padding:24px}
        .card{background:#0d1b2a;border-radius:18px;padding:20px;margin-top:30px}
        .err{color:#ff8a96;font-weight:800}
        a{color:#1fd1a5}
      </style>
    </head>
    <body><div class="wrap"><div class="card">
      <h2>⚠️ EdoSignal temporary server error</h2>
      <div class="err">The request could not complete just now.</div>
      <p>Please wait a few seconds and try again. The app will no longer show a blank Internal Server Error page.</p>
      <a href="/">← Back to Market Alerts</a>
    </div></div></body></html>
    """, 500


@APP.route('/health')
def health():
    return jsonify(ok=True)


init_db()
# Keep trend/pattern processing state across restart so new valid signals are not missed.
threading.Thread(
    target=monitor,
    daemon=True
).start()

threading.Thread(
    target=active_trend_monitor,
    daemon=True
).start()

threading.Thread(
    target=pattern_signal_monitor,
    daemon=True
).start()


if __name__ == '__main__':
    APP.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', '8080'))
    )
