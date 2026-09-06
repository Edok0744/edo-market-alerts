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
.trend-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-top:12px}
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
    placeholder="USD/CAD or BTC/USD"
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
    grid-template-columns:repeat(5,1fr);
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
            Trend is calculated from recent candle closes, 20/50-period moving averages
            and short-term momentum. It is an analysis aid, not a guarantee of future price movement.
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
    <h1>⚡ {{ symbol }} PATTERN SIGNAL</h1>
    <div class="small">Price-action scan • 4H and higher • No EMA/RSI signal</div>

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
                <div class="pattern-title neutral">No clear price-action pattern yet</div>
                <div class="pattern-detail">
                    The selected timeframe does not currently show a clean double top/bottom,
                    support/resistance rejection, or trendline break under the scanner rules.
                </div>
            </div>
        {% endif %}

        <div class="small" style="margin-top:12px">
            Updated: {{ updated }}. Pattern signals are based only on candle price structure.
            They are not guaranteed trade entries.
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


def latest_price(symbol):

    if not TWELVE_KEY:
        return None

    try:

        r = requests.get(
            'https://api.twelvedata.com/price',
            params={
                'symbol': symbol,
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

PATTERN_TIMEFRAMES = [
    {"label": "4H", "value": "4h"},
    {"label": "1D", "value": "1day"},
    {"label": "1W", "value": "1week"},
    {"label": "1M", "value": "1month"},
]


def get_ohlc(symbol, interval, outputsize=120):
    """Download OHLC candles from Twelve Data, oldest -> newest."""
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    try:
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": twelve_symbol(symbol),
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

        if len(candles) < 35:
            return None, f"Not enough {interval} candle history returned."

        return candles, None

    except Exception as e:
        print("pattern data error", symbol, interval, e)
        return None, "Could not download pattern data."


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


def pct_diff(a, b):
    mid = (abs(a) + abs(b)) / 2.0
    if mid == 0:
        return 0
    return abs(a - b) / mid


def detect_double_bottom(candles):
    lows = swing_points(candles, "low")
    best = None

    # Use recent 70 candles and require meaningful space between the two lows.
    for x in range(len(lows)):
        i1, p1 = lows[x]
        if i1 < max(0, len(candles) - 75):
            continue

        for y in range(x + 1, len(lows)):
            i2, p2 = lows[y]
            gap = i2 - i1

            if gap < 5 or gap > 35:
                continue
            if pct_diff(p1, p2) > 0.006:
                continue

            middle_high = max(c["high"] for c in candles[i1:i2+1])
            base = (p1 + p2) / 2.0
            bounce = (middle_high - base) / base

            if bounce < 0.006:
                continue

            latest_close = candles[-1]["close"]
            confirmed = latest_close > middle_high
            recent_enough = i2 >= len(candles) - 18

            if not recent_enough:
                continue

            quality = bounce - pct_diff(p1, p2)
            candidate = {
                "name": "Double Bottom",
                "direction": "bullish",
                "confirmed": confirmed,
                "level": middle_high,
                "p1": p1,
                "p2": p2,
                "gap": gap,
                "quality": quality,
            }

            if best is None or candidate["quality"] > best["quality"]:
                best = candidate

    return best


def detect_double_top(candles):
    highs = swing_points(candles, "high")
    best = None

    for x in range(len(highs)):
        i1, p1 = highs[x]
        if i1 < max(0, len(candles) - 75):
            continue

        for y in range(x + 1, len(highs)):
            i2, p2 = highs[y]
            gap = i2 - i1

            if gap < 5 or gap > 35:
                continue
            if pct_diff(p1, p2) > 0.006:
                continue

            middle_low = min(c["low"] for c in candles[i1:i2+1])
            top = (p1 + p2) / 2.0
            drop = (top - middle_low) / top

            if drop < 0.006:
                continue

            latest_close = candles[-1]["close"]
            confirmed = latest_close < middle_low
            recent_enough = i2 >= len(candles) - 18

            if not recent_enough:
                continue

            quality = drop - pct_diff(p1, p2)
            candidate = {
                "name": "Double Top",
                "direction": "bearish",
                "confirmed": confirmed,
                "level": middle_low,
                "p1": p1,
                "p2": p2,
                "gap": gap,
                "quality": quality,
            }

            if best is None or candidate["quality"] > best["quality"]:
                best = candidate

    return best


def detect_rejection(candles):
    """Detect a recent rejection from a repeatedly tested horizontal level."""
    recent = candles[-45:]
    latest = recent[-1]
    avg_range = sum(c["high"] - c["low"] for c in recent[-20:]) / 20.0

    if avg_range <= 0:
        return None

    # Support rejection: recent lows cluster near a level and latest candle rejects upward.
    lows = sorted(c["low"] for c in recent[:-1])[:8]
    support = sum(lows[:4]) / 4.0
    support_touches = sum(1 for c in recent[:-1] if abs(c["low"] - support) <= avg_range * 0.35)

    lower_wick = min(latest["open"], latest["close"]) - latest["low"]
    body = abs(latest["close"] - latest["open"])

    if (
        support_touches >= 2
        and abs(latest["low"] - support) <= avg_range * 0.45
        and lower_wick >= max(body * 1.2, avg_range * 0.25)
        and latest["close"] > latest["open"]
    ):
        return {
            "name": "Support Rejection",
            "direction": "bullish",
            "confirmed": True,
            "level": support,
        }

    # Resistance rejection.
    highs = sorted((c["high"] for c in recent[:-1]), reverse=True)[:8]
    resistance = sum(highs[:4]) / 4.0
    resistance_touches = sum(1 for c in recent[:-1] if abs(c["high"] - resistance) <= avg_range * 0.35)

    upper_wick = latest["high"] - max(latest["open"], latest["close"])

    if (
        resistance_touches >= 2
        and abs(latest["high"] - resistance) <= avg_range * 0.45
        and upper_wick >= max(body * 1.2, avg_range * 0.25)
        and latest["close"] < latest["open"]
    ):
        return {
            "name": "Resistance Rejection",
            "direction": "bearish",
            "confirmed": True,
            "level": resistance,
        }

    return None


def line_value(p1, p2, x):
    x1, y1 = p1
    x2, y2 = p2
    if x2 == x1:
        return y2
    slope = (y2 - y1) / (x2 - x1)
    return y1 + slope * (x - x1)


def detect_trendline_break(candles):
    """Find a clean break of a recent descending/ascending swing trendline."""
    highs = swing_points(candles, "high")
    lows = swing_points(candles, "low")

    # Descending resistance trendline -> bullish break.
    recent_highs = [p for p in highs if p[0] >= len(candles) - 60]
    if len(recent_highs) >= 2:
        p1, p2 = recent_highs[-2], recent_highs[-1]

        if p2[1] < p1[1] and p2[0] < len(candles) - 1:
            line_prev = line_value(p1, p2, len(candles) - 2)
            line_now = line_value(p1, p2, len(candles) - 1)

            prev_close = candles[-2]["close"]
            now_close = candles[-1]["close"]

            if prev_close <= line_prev and now_close > line_now:
                return {
                    "name": "Descending Trendline Break",
                    "direction": "bullish",
                    "confirmed": True,
                    "level": line_now,
                }

    # Ascending support trendline -> bearish break.
    recent_lows = [p for p in lows if p[0] >= len(candles) - 60]
    if len(recent_lows) >= 2:
        p1, p2 = recent_lows[-2], recent_lows[-1]

        if p2[1] > p1[1] and p2[0] < len(candles) - 1:
            line_prev = line_value(p1, p2, len(candles) - 2)
            line_now = line_value(p1, p2, len(candles) - 1)

            prev_close = candles[-2]["close"]
            now_close = candles[-1]["close"]

            if prev_close >= line_prev and now_close < line_now:
                return {
                    "name": "Ascending Trendline Break",
                    "direction": "bearish",
                    "confirmed": True,
                    "level": line_now,
                }

    return None


def describe_pattern(p):
    if p["name"] == "Double Bottom":
        status = "confirmed" if p["confirmed"] else "forming"
        detail = (
            f"Two similar lows are separated by {p['gap']} candles with a clear bounce between them. "
            f"The pattern is {status}."
        )
        level_text = f"Neckline / breakout level: {p['level']:.5f}"

    elif p["name"] == "Double Top":
        status = "confirmed" if p["confirmed"] else "forming"
        detail = (
            f"Two similar highs are separated by {p['gap']} candles with a clear drop between them. "
            f"The pattern is {status}."
        )
        level_text = f"Neckline / breakdown level: {p['level']:.5f}"

    elif p["name"] == "Support Rejection":
        detail = "Price tested a repeated support area and rejected upward on the latest candle."
        level_text = f"Support area: {p['level']:.5f}"

    elif p["name"] == "Resistance Rejection":
        detail = "Price tested a repeated resistance area and rejected downward on the latest candle."
        level_text = f"Resistance area: {p['level']:.5f}"

    elif p["name"] == "Descending Trendline Break":
        detail = "The latest candle closed above a descending swing-high trendline."
        level_text = f"Trendline break level: {p['level']:.5f}"

    else:
        detail = "The latest candle closed below an ascending swing-low trendline."
        level_text = f"Trendline break level: {p['level']:.5f}"

    bullish = p["direction"] == "bullish"

    return {
        "name": p["name"],
        "detail": detail,
        "level_text": level_text,
        "icon": "🟢" if bullish else "🔴",
        "css": "buy" if bullish else "sell",
        "direction": p["direction"],
        "confirmed": p.get("confirmed", False),
    }


def build_pattern_signal(symbol, interval, force_refresh=False):
    cache_key = f"{symbol}|{interval}"
    now = time.time()
    cached = PATTERN_SIGNAL_CACHE.get(cache_key)

    if cached and not force_refresh and now - cached["saved_at"] < PATTERN_SIGNAL_CACHE_SECONDS:
        return cached["data"], None

    candles, error = get_ohlc(symbol, interval, outputsize=120)

    if error:
        if "credits" in error.lower() or "limit" in error.lower():
            return None, "API busy. Wait about 60 seconds, then press Scan Again."
        return None, error

    found = []

    for detector in (
        detect_double_bottom,
        detect_double_top,
        detect_rejection,
        detect_trendline_break,
    ):
        p = detector(candles)
        if p:
            found.append(p)

    # Confirmed patterns are more important than forming patterns.
    found.sort(key=lambda p: (p.get("confirmed", False), p.get("quality", 0)), reverse=True)

    bullish_confirmed = sum(1 for p in found if p["direction"] == "bullish" and p.get("confirmed", False))
    bearish_confirmed = sum(1 for p in found if p["direction"] == "bearish" and p.get("confirmed", False))
    bullish_forming = sum(1 for p in found if p["direction"] == "bullish" and not p.get("confirmed", False))
    bearish_forming = sum(1 for p in found if p["direction"] == "bearish" and not p.get("confirmed", False))

    if bullish_confirmed > bearish_confirmed and bullish_confirmed > 0:
        signal = "BULLISH PATTERN SIGNAL"
        icon, css = "🟢", "buy"
        summary = "At least one bullish price-action pattern is confirmed on this timeframe."
    elif bearish_confirmed > bullish_confirmed and bearish_confirmed > 0:
        signal = "BEARISH PATTERN SIGNAL"
        icon, css = "🔴", "sell"
        summary = "At least one bearish price-action pattern is confirmed on this timeframe."
    elif bullish_confirmed and bearish_confirmed:
        signal = "MIXED PATTERNS"
        icon, css = "🟡", "wait"
        summary = "Bullish and bearish structures are both present. Better to wait for clearer direction."
    elif bullish_forming > bearish_forming and bullish_forming > 0:
        signal = "BULLISH SETUP FORMING"
        icon, css = "🟡", "wait"
        summary = "A bullish structure is forming but has not confirmed yet."
    elif bearish_forming > bullish_forming and bearish_forming > 0:
        signal = "BEARISH SETUP FORMING"
        icon, css = "🟡", "wait"
        summary = "A bearish structure is forming but has not confirmed yet."
    else:
        signal = "NO CLEAR SIGNAL"
        icon, css = "⚪", "neutral"
        summary = "No clean higher-timeframe price-action setup is confirmed right now."

    data = {
        "price": candles[-1]["close"],
        "signal": signal,
        "signal_icon": icon,
        "signal_css": css,
        "summary": summary,
        "patterns": [describe_pattern(p) for p in found[:4]],
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    PATTERN_SIGNAL_CACHE[cache_key] = {"saved_at": now, "data": data}
    return data, None



def get_daily_candles_for_alignment(symbol, outputsize=1800):
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    try:
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": twelve_symbol(symbol),
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


def build_full_alignment(symbol):
    # 1H + 4H + 1D API calls. 1W and 1M are derived locally from daily data.
    h1, err = get_candles(symbol, "1h", outputsize=70)
    if err:
        return "", err

    h4, err = get_candles(symbol, "4h", outputsize=70)
    if err:
        return "", err

    daily_rows, err = get_daily_candles_for_alignment(symbol, outputsize=1800)
    if err:
        return "", err

    d1 = [close for _, close in daily_rows]
    w1 = resample_closes(daily_rows, "week")
    m1 = resample_closes(daily_rows, "month")

    if len(w1) < 50 or len(m1) < 50:
        return "", "Not enough weekly/monthly history after resampling."

    states = {
        "1M": analyse_closes(m1),
        "1W": analyse_closes(w1),
        "1D": analyse_closes(d1),
        "4H": analyse_closes(h4),
        "1H": analyse_closes(h1),
    }

    if all(v == "Bullish" for v in states.values()):
        return "FULL BULLISH", None

    if all(v == "Bearish" for v in states.values()):
        return "FULL BEARISH", None

    return "", None


def save_trend_status(symbol, status):
    with db_conn() as c:
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


def active_trend_monitor():
    # Start two minutes after boot, then refresh one active symbol every five minutes.
    time.sleep(120)
    index = 0

    while True:
        try:
            with db_conn() as c:
                rows = c.execute(
                    "SELECT DISTINCT symbol FROM alerts WHERE triggered=0 ORDER BY symbol"
                ).fetchall()

            symbols = [r["symbol"] for r in rows]

            if symbols:
                if index >= len(symbols):
                    index = 0

                symbol = symbols[index]
                index = (index + 1) % len(symbols)

                status, error = build_full_alignment(symbol)

                if error:
                    print("active trend error", symbol, error)
                else:
                    save_trend_status(symbol, status)

        except Exception as e:
            print("active trend monitor error", e)

        time.sleep(300)


TREND_INTERVALS = [
    ("1M", "1month"),
    ("1W", "1week"),
    ("1D", "1day"),
    ("4H", "4h"),
    ("1H", "1h"),
]


def twelve_symbol(symbol):
    """Normalise common compact crypto symbols for Twelve Data."""
    s = symbol.upper().strip()
    compact_crypto = {
        "BTCUSDT": "BTC/USDT",
        "ETHUSDT": "ETH/USDT",
        "BTCUSD": "BTC/USD",
        "ETHUSD": "ETH/USD",
    }
    return compact_crypto.get(s, s)


def get_candles(symbol, interval, outputsize=60):
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    try:
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": twelve_symbol(symbol),
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
        closes = []

        # Twelve Data returns newest first. Reverse so closes are oldest -> newest.
        for row in reversed(values):
            try:
                closes.append(float(row["close"]))
            except (KeyError, TypeError, ValueError):
                pass

        if len(closes) < 50:
            return None, f"Not enough {interval} candle history returned."

        return closes, None

    except Exception as e:
        print("trend data error", symbol, interval, e)
        return None, "Could not download trend data."


def sma(values, period):
    return sum(values[-period:]) / period


def analyse_closes(closes):
    """
    Bullish:
      close > SMA20 > SMA50, SMA20 rising, and 3-bar momentum positive.
    Bearish:
      close < SMA20 < SMA50, SMA20 falling, and 3-bar momentum negative.
    Everything else is Mixed.
    """
    close = closes[-1]
    sma20 = sma(closes, 20)
    sma50 = sma(closes, 50)

    previous_sma20 = sum(closes[-21:-1]) / 20
    slope = sma20 - previous_sma20
    momentum = close - closes[-4]

    if close > sma20 > sma50 and slope > 0 and momentum > 0:
        return "Bullish"
    if close < sma20 < sma50 and slope < 0 and momentum < 0:
        return "Bearish"
    return "Mixed"


def build_trend_scan(symbol):
    results = []

    state_info = {
        "Bullish": ("🟢", "bull"),
        "Bearish": ("🔴", "bear"),
        "Mixed": ("🟡", "mixed"),
    }

    for label, interval in TREND_INTERVALS:
        closes, error = get_candles(symbol, interval)

        if error:
            return None, error

        state = analyse_closes(closes)
        icon, css = state_info[state]

        results.append({
            "label": label,
            "interval": interval,
            "state": state,
            "icon": icon,
            "css": css,
        })

    # Weight higher timeframes more heavily.
    weights = {"1M": 5, "1W": 4, "1D": 3, "4H": 2, "1H": 1}
    score = 0
    states = {}

    for item in results:
        states[item["label"]] = item["state"]
        if item["state"] == "Bullish":
            score += weights[item["label"]]
        elif item["state"] == "Bearish":
            score -= weights[item["label"]]

    higher_bull = all(states[x] == "Bullish" for x in ("1M", "1W", "1D"))
    higher_bear = all(states[x] == "Bearish" for x in ("1M", "1W", "1D"))

    if higher_bull and score >= 12:
        summary = "STRONG BULLISH"
        icon, css = "🟢", "bull"
        detail = "Major timeframes are aligned bullish."
    elif higher_bear and score <= -12:
        summary = "STRONG BEARISH"
        icon, css = "🔴", "bear"
        detail = "Major timeframes are aligned bearish."
    elif higher_bull and any(states[x] == "Bearish" for x in ("4H", "1H")):
        summary = "BULLISH — SHORT-TERM PULLBACK"
        icon, css = "🟡", "mixed"
        detail = "Monthly, weekly and daily are bullish, but a lower timeframe is pulling back."
    elif higher_bear and any(states[x] == "Bullish" for x in ("4H", "1H")):
        summary = "BEARISH — SHORT-TERM BOUNCE"
        icon, css = "🟡", "mixed"
        detail = "Monthly, weekly and daily are bearish, but a lower timeframe is bouncing."
    elif score >= 6:
        summary = "BULLISH"
        icon, css = "🟢", "bull"
        detail = "The weighted multi-timeframe trend leans bullish."
    elif score <= -6:
        summary = "BEARISH"
        icon, css = "🔴", "bear"
        detail = "The weighted multi-timeframe trend leans bearish."
    else:
        summary = "MIXED / WAIT"
        icon, css = "🟡", "mixed"
        detail = "The timeframes are not aligned strongly enough."

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
                        price_cache[a['symbol']] = latest_price(a['symbol'])
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
    selected_tf = request.args.get("tf", "4h")

    if selected_tf not in allowed:
        selected_tf = "4h"

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

    scan, error = build_trend_scan(f['symbol'])

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
