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
    gap:10px;
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
h1{font-size:27px;margin-bottom:3px}
h2{font-size:18px}
.price{font-variant-numeric:tabular-nums;font-weight:700}
.status{font-size:12px;font-weight:800}
.trendbtn{background:#5dade2;color:#07111f}
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

<div class="market">

<div>
<span class="pill"
style="background:{{ colors[f['grp']] }}22;color:{{ colors[f['grp']] }}">
{{f['grp']}}
</span>

<b>{{f['symbol']}}</b>
</div>

<div>
<a href="/favorite/use/{{f['id']}}">
<button>USE</button>
</a>

<a href="/trend/{{f['id']}}">
<button class="trendbtn">📊 TREND</button>
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
</div>

<div style="text-align:right">

<div class="price">
{{m['last_price'] if m['last_price'] is not none else '—'}}
</div>

<div class="status">
{{'✅ TRIGGERED' if m['triggered'] else '🟢 ARMED'}}
</div>

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

    return render_template_string(
        HTML,
        markets=markets,
        favorites=favorites,
        colors=COLORS,
        selected_symbol=selected_symbol,
        selected_group=selected_group
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


if __name__ == '__main__':
    APP.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', '8080'))
    )
