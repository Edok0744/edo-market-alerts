import os, time, sqlite3, threading
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string, redirect
import requests

APP = Flask(__name__)

DB = os.environ.get('EDO_DB', 'edo_market_alerts.db')
TWELVE_KEY = os.environ.get('TWELVE_DATA_API_KEY', '')
PUSHOVER_APP_TOKEN = os.environ.get('PUSHOVER_APP_TOKEN', '')
PUSHOVER_USER_KEY = os.environ.get('PUSHOVER_USER_KEY', '')
CHECK_SECONDS = int(os.environ.get('CHECK_SECONDS', '900'))

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

<div class="saved-actions {{ 'forex-actions' if f['grp']=='FOREX' else 'three-actions' }}">
<a href="/favorite/use/{{f['id']}}">
<button>USE</button>
</a>

<a href="/trend/{{f['id']}}">
<button class="trendbtn">📊 TREND</button>
</a>

{% if f['grp'] == 'FOREX' %}
<a href="/signal/{{f['id']}}">
<button class="livebtn">⚡ SIGNAL</button>
</a>
{% endif %}

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
</div>

<div style="text-align:right">

<div class="price">
{{m['last_price'] if m['last_price'] is not none else '—'}}
</div>

<div class="status">
{{'✅ TRIGGERED' if m['triggered'] else '🟢 ARMED'}}
</div>

{% set ts = trend_statuses.get(m['symbol']) %}
{% if ts == 'FULL BULLISH' %}
<div class="fulltrend fullbull">🟢 FULL BULLISH</div>
{% elif ts == 'FULL BEARISH' %}
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
                    {{ item['icon'] }} {{ item['state'] }}
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
    <div class="small">Your price-action method • 8H and higher • Wicks define levels, body closes confirm • Manual trade decision</div>

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
            {% for p in patterns %}
            <div class="pattern">
                <div class="pattern-title {{ p['css'] }}">{{ p['icon'] }} {{ p['name'] }}</div>
                <div class="pattern-detail">{{ p['detail'] }}</div>
                {% if p['level_text'] %}
                <div class="level">{{ p['level_text'] }}</div>
                {% endif %}
            </div>
            {% endfor %}
        {% else %}
            <div class="pattern">
                <div class="pattern-title neutral">No matching setup yet</div>
                <div class="pattern-detail">
                    No recent candle sequence matches your bounce/retest or trend-pullback
                    confirmation rules on this timeframe.
                </div>
            </div>
        {% endif %}

        <div class="small" style="margin-top:12px">
            Updated: {{ updated }}. The scanner finds setups from your candle-close rules.
            It does not place trades. You decide whether to pull the trigger.
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
    c = sqlite3.connect(DB, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db_conn() as c:

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
        try:
            c.execute("ALTER TABLE alerts ADD COLUMN note TEXT")
        except sqlite3.OperationalError:
            pass
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

    try:

        r = requests.get(
            'https://api.twelvedata.com/price',
            params={
                'symbol': twelve_symbol(symbol, grp),
                'apikey': TWELVE_KEY
            },
            timeout=10
        )

        j = r.json()

        return float(j['price']) if 'price' in j else None

    except Exception as e:
        print('price error', symbol, e)
        return None




PATTERN_SIGNAL_CACHE = {}
PATTERN_SIGNAL_CACHE_SECONDS = 60

# Edo's higher-timeframe setup scanner.
# The scanner does NOT place trades. It only finds setups for manual review.
PATTERN_TIMEFRAMES = [
    {"label": "8H", "value": "8h"},
    {"label": "1D", "value": "1day"},
    {"label": "1W", "value": "1week"},
    {"label": "1M", "value": "1month"},
]


def get_ohlc(symbol, interval, outputsize=140, grp=None):
    """Download OHLC candles from Twelve Data, oldest -> newest."""
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    try:
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": twelve_symbol(symbol, grp),
                "interval": interval,
                "outputsize": outputsize,
                "apikey": TWELVE_KEY,
                "format": "JSON",
            },
            timeout=15
        )
        j = r.json()

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

        return candles, None

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
    Edo level/retest setup:
      wick LOW/HIGH is a valid structural support/resistance point
      meaningful candle separation
      return to roughly the same wick-defined zone
      >50% candle BODY-close confirmation
    """
    i = conf["index"]
    direction = conf["direction"]
    if i < 12:
        return None

    ar = avg_range(candles, i+1, 20)
    if ar <= 0:
        return None

    # The support/resistance point is allowed to come from the WICK.
    # The second bounce/retest may occur a few candles before the confirmation.
    touch_start = max(2, i - 4)
    touch_end = i + 1
    touch_slice = candles[touch_start:touch_end]

    if direction == "bullish":
        # Lowest lower-wick in the recent retest area = second support test.
        rel_touch_i = min(range(len(touch_slice)), key=lambda k: touch_slice[k]["low"])
        retest_price = touch_slice[rel_touch_i]["low"]
        retest_index = touch_start + rel_touch_i
        swings = swing_points(candles[:touch_start], "low")
        level_source = "lower wick"
    else:
        # Highest upper-wick in the recent retest area = second resistance test.
        rel_touch_i = max(range(len(touch_slice)), key=lambda k: touch_slice[k]["high"])
        retest_price = touch_slice[rel_touch_i]["high"]
        retest_index = touch_start + rel_touch_i
        swings = swing_points(candles[:touch_start], "high")
        level_source = "upper wick"

    # Adaptive support/resistance zone. Keeps the rule useful across JPY and
    # normal 1.x forex prices.
    zone_tolerance = max(ar * 0.65, abs(retest_price) * 0.0025)

    candidates = []
    for old_i, old_price in swings:
        separation = touch_start - old_i
        if separation < 8 or separation > 90:
            continue
        if abs(old_price - retest_price) > zone_tolerance:
            continue

        between = candles[old_i+1:touch_start]
        if not between:
            continue

        if direction == "bullish":
            moved_away = max(c["high"] for c in between) - min(old_price, retest_price)
        else:
            moved_away = max(old_price, retest_price) - min(c["low"] for c in between)

        # Price must have genuinely left the zone before returning.
        if moved_away < ar * 2.0:
            continue

        closeness = 1.0 - min(1.0, abs(old_price - retest_price) / zone_tolerance)
        candidates.append((old_i, old_price, separation, closeness, moved_away))

    if not candidates:
        return None

    # Prefer a clean recent structural retest with good level similarity.
    old_i, old_price, separation, closeness, moved_away = max(
        candidates,
        key=lambda x: (x[3], x[2])
    )

    body_reference = avg_body(candles, touch_start, 20)
    recent_bodies = [
        abs(c["close"] - c["open"])
        for c in candles[max(old_i+1, i-4):i]
    ]
    weak_retest = (
        body_reference > 0
        and recent_bodies
        and (sum(recent_bodies) / len(recent_bodies)) < body_reference * 0.85
    )

    target = previous_target(candles, direction, i)
    score = 5.0 + min(3.0, separation / 12.0) + closeness * 2.0
    score += min(2.0, max(0.0, conf["penetration"] - 50.0) / 25.0)
    if weak_retest:
        score += 1.0

    return {
        "name": "BOUNCE / RETEST SETUP",
        "direction": direction,
        "confirmed": True,
        "score": score,
        "confirmation_date": conf["date"],
        "confirmation_close": conf["close"],
        "penetration": conf["penetration"],
        "level": (old_price + retest_price) / 2.0,
        "old_level": old_price,
        "retest_price": retest_price,
        "retest_date": candles[retest_index].get("datetime", ""),
        "level_source": level_source,
        "old_date": candles[old_i].get("datetime", ""),
        "separation": separation,
        "weak_retest": weak_retest,
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
    Edo trend-pullback setup:
      established bigger price direction
      3+ same-colour candles pulling against it
      opposite candle closes >50% through the previous candle
    """
    i = conf["index"]
    direction = conf["direction"]

    if direction == "bullish":
        run_colour = "red"
        required_trend = "bullish"
    else:
        run_colour = "green"
        required_trend = "bearish"

    run_count, run_start = count_same_colour_before(candles, i, run_colour)

    if run_count < 3:
        return None

    trend = local_structure_trend(candles, run_start)
    if trend != required_trend:
        return None

    target = previous_target(candles, direction, run_start)
    score = 6.0 + min(3.0, (run_count - 3) * 0.75)
    score += min(2.0, max(0.0, conf["penetration"] - 50.0) / 25.0)

    return {
        "name": "TREND PULLBACK SETUP",
        "direction": direction,
        "confirmed": True,
        "score": score,
        "confirmation_date": conf["date"],
        "confirmation_close": conf["close"],
        "penetration": conf["penetration"],
        "run_count": run_count,
        "run_colour": run_colour,
        "trend": trend,
        "target": target,
    }


def describe_setup(p):
    bullish = p["direction"] == "bullish"
    icon = "🟢" if bullish else "🔴"
    css = "buy" if bullish else "sell"
    direction_word = "Bullish" if bullish else "Bearish"

    if p["name"] == "BOUNCE / RETEST SETUP":
        weak_text = " Weakness was also detected in the retest candles." if p["weak_retest"] else ""
        detail = (
            f"{direction_word} candle-close confirmation on {p['confirmation_date']}. "
            f"The confirmation candle closed {p['penetration']:.0f}% back through the previous "
            f"opposite-colour candle BODY. The structural support/resistance is allowed to be "
            f"formed by the candle WICK. Price revisited that area after "
            f"{p['separation']} candles.{weak_text}"
        )

        level_text = (
            f"Wick-defined retest zone: {p['level']:.5f} • "
            f"Earlier wick level: {p['old_level']:.5f} on {p['old_date']} • "
            f"Second {p['level_source']} test: {p['retest_price']:.5f} on {p['retest_date']} • "
            f"Confirmation close: {p['confirmation_close']:.5f}"
        )

    else:
        detail = (
            f"{direction_word} trend-pullback confirmation on {p['confirmation_date']}. "
            f"{p['run_count']} {p['run_colour']} candles pulled against the larger "
            f"{p['trend']} price structure, then the confirmation candle closed "
            f"{p['penetration']:.0f}% back through the previous candle body."
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


def build_pattern_signal(symbol, interval, force_refresh=False):
    cache_key = f"{symbol}|{interval}"
    now = time.time()
    cached = PATTERN_SIGNAL_CACHE.get(cache_key)

    if cached and not force_refresh and now - cached["saved_at"] < PATTERN_SIGNAL_CACHE_SECONDS:
        return cached["data"], None

    candles, error = get_ohlc(symbol, interval, outputsize=140)

    if error:
        if "credits" in error.lower() or "limit" in error.lower():
            return None, "API busy. Wait about 60 seconds, then press Scan Again."
        return None, error

    found = []
    confirmations = recent_confirmations(candles, lookback=7)

    for conf in confirmations:
        retest = detect_bounce_retest(candles, conf)
        if retest:
            found.append(retest)

        pullback = detect_trend_pullback(candles, conf)
        if pullback:
            found.append(pullback)

    # Avoid duplicate descriptions of the same setup/direction/confirmation.
    unique = {}
    for p in found:
        key = (p["name"], p["direction"], p["confirmation_date"])
        if key not in unique or p.get("score", 0) > unique[key].get("score", 0):
            unique[key] = p

    found = list(unique.values())
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
            "No recent setup matches your retest or 3+ candle pullback confirmation rules "
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


def get_daily_candles_for_alignment(symbol, outputsize=1800, grp=None):
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    try:
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": twelve_symbol(symbol, grp),
                "interval": "1day",
                "outputsize": outputsize,
                "apikey": TWELVE_KEY,
                "format": "JSON",
            },
            timeout=20
        )
        j = r.json()

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


def build_full_alignment(symbol, grp=None):
    """
    Edo direct-candlestick alignment signal.

    Signal timeframes:
      1W + 1D + 8H + 4H + 1H

    Monthly is display-only and does not trigger a signal.

    FULL BULLISH:
      the last FULLY CLOSED candle on ALL five signal timeframes is green.

    FULL BEARISH:
      the last FULLY CLOSED candle on ALL five signal timeframes is red.
    """
    intervals = {
        "1W": "1week",
        "1D": "1day",
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
    # Start two minutes after boot, then refresh one active symbol every five minutes.
    time.sleep(120)
    index = 0

    while True:
        try:
            with db_conn() as c:
                rows = c.execute(
                    "SELECT DISTINCT symbol, grp FROM alerts WHERE triggered=0 ORDER BY symbol"
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
                    previous = save_trend_status(symbol, status)

                    # Send one Pushover notification only when the pair ENTERS
                    # a fully aligned bullish or bearish state. Repeated checks
                    # in the same state do not send duplicate notifications.
                    if status in ("FULL BULLISH", "FULL BEARISH") and status != previous:
                        icon = "🟢" if status == "FULL BULLISH" else "🔴"
                        direction = "bullish" if status == "FULL BULLISH" else "bearish"
                        send_push(
                            f"{icon} {symbol} — {status}",
                            f"All 5 last CLOSED signal candles are {direction}: 1W, 1D, 8H, 4H, 1H. "
                            f"Monthly is display-only. CFD markets may use an ETF proxy for trend data."
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


def get_candles(symbol, interval, outputsize=60, grp=None):
    """
    Download OHLC candles from Twelve Data.

    Candles are returned oldest -> newest.

    IMPORTANT:
    The newest candle returned by the API may still be forming.
    EdoSignal therefore NEVER uses that live candle for a signal.

    The signal uses the previous candle, which is treated as the last
    fully closed/completed candle:
      close > open  = Bullish / green
      close < open  = Bearish / red
      close == open = Mixed / doji
    """
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    try:
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": twelve_symbol(symbol, grp),
                "interval": interval,
                "outputsize": outputsize,
                "apikey": TWELVE_KEY,
                "format": "JSON",
            },
            timeout=15
        )
        j = r.json()

        if j.get("status") == "error":
            return None, j.get("message", "Twelve Data returned an error.")

        values = j.get("values") or []
        candles = []

        # Twelve Data returns newest first. Reverse so candles are oldest -> newest.
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

        if not candles:
            return None, f"No usable {interval} candles returned."

        return candles, None

    except Exception as e:
        print("trend data error", symbol, interval, e)
        return None, "Could not download trend data."

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

    for label, interval in TREND_INTERVALS:
        candles, error = get_candles(symbol, interval, outputsize=3, grp=grp)

        if error:
            return None, error

        closed = last_closed_candle(candles)
        if closed is None:
            return None, f"Not enough completed {label} candle data."

        state = analyse_candle(closed)
        icon, css = state_info[state]

        results.append({
            "label": label,
            "interval": interval,
            "state": state,
            "icon": icon,
            "css": css,
        })

    states = {item["label"]: item["state"] for item in results}

    # IMPORTANT:
    # 1M is shown on screen so Edo can inspect it manually,
    # but it does NOT affect the signal or score.
    signal_labels = ("1W", "1D", "8H", "4H", "1H")
    weights = {"1W": 5, "1D": 4, "8H": 3, "4H": 2, "1H": 1}
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
        detail = "Last CLOSED candles on Weekly, Daily, 8H, 4H and 1H are all GREEN. Bullish possibility. Monthly is display-only."
    elif full_bear:
        summary = "FULL BEARISH"
        icon, css = "🔴", "bear"
        detail = "Last CLOSED candles on Weekly, Daily, 8H, 4H and 1H are all RED. Bearish possibility. Monthly is display-only."
    elif states["1W"] == "Bullish" and states["1D"] == "Bullish" and any(
        states[x] == "Bearish" for x in ("8H", "4H", "1H")
    ):
        summary = "BULLISH — LOWER-TIMEFRAME PULLBACK"
        icon, css = "🟡", "mixed"
        detail = "Weekly and Daily are bullish, but a lower timeframe is pulling back."
    elif states["1W"] == "Bearish" and states["1D"] == "Bearish" and any(
        states[x] == "Bullish" for x in ("8H", "4H", "1H")
    ):
        summary = "BEARISH — LOWER-TIMEFRAME BOUNCE"
        icon, css = "🟡", "mixed"
        detail = "Weekly and Daily are bearish, but a lower timeframe is bouncing."
    elif score >= 7:
        summary = "BULLISH"
        icon, css = "🟢", "bull"
        detail = "Weekly through 1H lean bullish. Monthly is not included in this score."
    elif score <= -7:
        summary = "BEARISH"
        icon, css = "🔴", "bear"
        detail = "Weekly through 1H lean bearish. Monthly is not included in this score."
    else:
        summary = "MIXED / WAIT"
        icon, css = "🟡", "mixed"
        detail = "Weekly through 1H are not aligned strongly enough."

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

                            send_push(
                                f"🚨 {a['symbol']} PRICE ALERT",
                                f"{a['symbol']} is {p}\n"
                                f"Target: {a['direction']} {a['target']}\n"
                                f"Note: {a['note'] or '-'}"
                            )

                c.commit()

        except Exception as e:
            print('monitor error', e)

        time.sleep(CHECK_SECONDS)


@APP.route('/')
def home():

    selected_symbol = request.args.get('symbol', '')
    selected_group = request.args.get('group', 'FOREX')

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

    return render_template_string(
        HTML,
        markets=markets,
        favorites=favorites,
        colors=COLORS,
        selected_symbol=selected_symbol,
        selected_group=selected_group,
        trend_statuses=trend_statuses
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

    with db_conn() as c:
        f = c.execute(
            'SELECT * FROM favorites WHERE id=?',
            (i,)
        ).fetchone()

    if not f or f['grp'] != 'FOREX':
        return redirect('/')

    allowed = {x["value"]: x["label"] for x in PATTERN_TIMEFRAMES}
    selected_tf = request.args.get("tf", "8h")

    if selected_tf not in allowed:
        selected_tf = "8h"

    force_refresh = request.args.get('refresh') == '1'
    data, error = build_pattern_signal(
        f['symbol'],
        selected_tf,
        force_refresh=force_refresh
    )

    if error:
        return render_template_string(
            SIGNAL_HTML,
            symbol=f['symbol'],
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


@APP.route('/health')
def health():
    return jsonify(ok=True)


init_db()
threading.Thread(
    target=monitor,
    daemon=True
).start()

threading.Thread(
    target=active_trend_monitor,
    daemon=True
).start()


if __name__ == '__main__':
    APP.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', '8080'))
    )
