import os, time, sqlite3, threading, json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, render_template_string, redirect
import requests

APP = Flask(__name__)

# Used to prevent a Railway restart/redeploy from replaying an already-closed
# historical candle as a brand-new phone signal.
PROCESS_STARTED_UTC = datetime.now(timezone.utc)

DB = os.environ.get('EDO_DB', 'edo_market_alerts.db')
TWELVE_KEY = os.environ.get('TWELVE_DATA_API_KEY', '')
PUSHOVER_APP_TOKEN = os.environ.get('PUSHOVER_APP_TOKEN', '')
PUSHOVER_USER_KEY = os.environ.get('PUSHOVER_USER_KEY', '')
FOREX_FACTORY_CALENDAR_URL = os.environ.get(
    'FOREX_FACTORY_CALENDAR_URL',
    'https://nfs.faireconomy.media/ff_calendar_thisweek.json'
)
FOREX_FACTORY_CALENDAR_FALLBACK_URL = os.environ.get(
    'FOREX_FACTORY_CALENDAR_FALLBACK_URL',
    'https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json'
)
NEWS_REFRESH_SECONDS = int(os.environ.get('NEWS_REFRESH_SECONDS', '1800'))
NEWS_WARNING_MINUTES = int(os.environ.get('NEWS_WARNING_MINUTES', '5'))
NEWS_PUSH_SOUND = os.environ.get('NEWS_PUSH_SOUND', 'siren')
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
        "1min": 20,
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
.news-card{
    border:1px solid #294761;
}
.news-row{
    display:flex;
    justify-content:space-between;
    align-items:flex-start;
    gap:12px;
    padding:10px 0;
    border-bottom:1px solid #1c3449;
    min-height:58px;
}
.news-row:last-child{border-bottom:0}
.news-left{min-width:0}
.news-title{font-size:14px;font-weight:900}
.news-time{font-size:12px;color:#9eb5c9;margin-top:3px}
.news-count{
    font-size:12px;
    font-weight:900;
    white-space:nowrap;
    text-align:right;
    min-width:118px;
    width:118px;
    flex:0 0 118px;
    font-variant-numeric:tabular-nums;
}
.js-news-timeleft{
    display:inline-block;
    min-width:92px;
    text-align:right;
}
.news-red{color:#ff6b7d}
.news-amber{color:#f2c94c}
.news-normal{color:#8ca7bf}
.news-reaction{
    display:inline-block;
    margin-left:7px;
    padding:3px 7px;
    border-radius:8px;
    font-size:11px;
    font-weight:900;
    white-space:nowrap;
    vertical-align:1px;
    border:1px solid rgba(255,255,255,.13);
}
.news-reaction.reading{color:#9dccff;background:#183a5e}
.news-reaction.bullish{color:#72f0a0;background:#173e2a}
.news-reaction.bearish{color:#ff7d8d;background:#4a1d28}
.news-reaction.mixed{color:#f2c94c;background:#4a4020}
.news-outcomes{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px}
.news-outcome-label{font-size:11px;color:#9eb5c9;font-weight:800;margin-right:2px}
.news-pair-outcome-row{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:5px;font-size:12px}
.news-pair-symbol{min-width:72px;color:#e8f2ff;font-weight:900}

.news-chip{
    display:inline-block;
    padding:3px 7px;
    border-radius:999px;
    background:#1a3045;
    margin-right:6px;
    font-size:11px;
    font-weight:900;
}

.news-head{
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    margin-bottom:8px;
}
.news-refresh{
    border:1px solid #315674;
    background:#132b3f;
    color:#d8e8f5;
    border-radius:10px;
    padding:8px 11px;
    font-size:12px;
    font-weight:900;
    cursor:pointer;
}
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

.news-flash{
    color:#ff4d5a !important;
    animation:edoNewsFlash 1s infinite;
}
@keyframes edoNewsFlash{
    0%,100%{opacity:1}
    50%{opacity:.25}
}
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


<div class="card news-card">
<div class="news-head">
    <h2 style="margin:0">📰 High-Impact News</h2>
    <button class="news-refresh" id="news-refresh-btn" type="button">↻ Refresh</button>
</div>
<div id="news-refresh-status" class="small" style="display:none;margin-bottom:8px"></div>

{% if news_configured %}
    {% if news_items %}
        {% for n in news_items %}
        <div class="news-row">
            <div class="news-left">
                <div class="news-title">
                    <span class="news-chip">{{ n['currency'] }}</span>{{ n['event_name'] }}
                </div>
                {% if n.get('event_names') and n['event_names']|length > 1 %}
                <div class="news-time">{{ n['event_names'] | join(' • ') }}</div>
                {% endif %}
                <div class="news-outcomes js-news-outcome-group"
                     data-event-id="{{ n['event_id'] }}"
                     data-event-ms="{{ n['event_time_ms'] }}">
                    <span class="news-outcome-label">Pair outcomes:</span>
                    <div class="js-news-pair-outcomes" style="width:100%">
                        <div class="news-pair-outcome-row">
                            <strong>Waiting for 15M / 30M market reaction...</strong>
                        </div>
                    </div>
                </div>
                <div class="news-time">{{ n['perth_time'] }} Perth</div>
                {% if n.get('affected_pairs') %}
                <div class="news-time">
                    Affects: {{ n['affected_pairs'] | join(', ') }}
                </div>
                {% endif %}
            </div>
            <div class="news-count js-news-countdown {{ 'news-red' if n['level']=='red' else 'news-amber' if n['level']=='amber' else 'news-normal' }}"
                 data-event-ms="{{ n['event_time_ms'] }}">
                <span class="js-news-warning">
                    {% if n['level']=='red' %}⚠ HOLD / WAIT<br>{% elif n['level']=='amber' %}⚠ NEWS SOON<br>{% endif %}
                </span>
                <span class="js-news-timeleft">{{ n['countdown'] }}</span>
            </div>
        </div>
        {% endfor %}
        <div class="small" style="margin-top:8px">
            Information only. EdoSignal does not block your setup. Use the affected-pair line to decide whether to hold a new entry before major news.
        </div>
    {% else %}
        {% if news_feed_unavailable %}
        <div class="small" style="color:#f2c94c">
            ⚠ News feed temporarily unavailable. Do not assume there is no High-Impact news.
            Press Refresh again shortly.
        </div>
        {% else %}
        <div class="small">✅ No more High-Impact news today for your saved markets.</div>
        {% endif %}
    {% endif %}
{% else %}
    <div class="small">
        📰 Forex Factory calendar feed is enabled. Waiting for the next background refresh.
    </div>
{% endif %}
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

<script>
(function () {
    function formatLeft(msLeft) {
        if (msLeft <= 0) {
            const minsPast = Math.floor(Math.abs(msLeft) / 60000);
            if (minsPast < 1) return 'NOW';
            if (minsPast < 60) return minsPast + 'm ago';
            return Math.floor(minsPast / 60) + 'h ago';
        }

        const total = Math.floor(msLeft / 1000);
        const days = Math.floor(total / 86400);
        const hours = Math.floor((total % 86400) / 3600);
        const mins = Math.floor((total % 3600) / 60);
        const secs = total % 60;

        if (days > 0) return 'in ' + days + 'd ' + hours + 'h ' + mins + 'm';
        if (hours > 0) return 'in ' + hours + 'h ' + mins + 'm ' + secs + 's';
        return 'in ' + mins + 'm ' + secs + 's';
    }

    function updateNewsCountdowns() {
        const now = Date.now();
        const rows = document.querySelectorAll('.js-news-countdown');

        rows.forEach(function (row) {
            const eventMs = Number(row.dataset.eventMs || 0);
            if (!eventMs) return;

            const left = eventMs - now;
            const minsLeft = left / 60000;
            const timeNode = row.querySelector('.js-news-timeleft');
            const warningNode = row.querySelector('.js-news-warning');

            if (timeNode) timeNode.textContent = formatLeft(left);

            row.classList.remove('news-red', 'news-amber', 'news-normal', 'news-flash');

            if (minsLeft <= 30 && minsLeft >= 0) {
                row.classList.add('news-red', 'news-flash');
                if (warningNode) warningNode.innerHTML = '🔴 HOLD / WAIT<br>';
            } else if (minsLeft <= 60 && minsLeft > 30) {
                row.classList.add('news-red');
                if (warningNode) warningNode.innerHTML = '⚠ HOLD / WAIT<br>';
            } else if (minsLeft <= 240 && minsLeft > 60) {
                row.classList.add('news-amber');
                if (warningNode) warningNode.innerHTML = '⚠ NEWS SOON<br>';
            } else if (minsLeft < 0 && minsLeft >= -60) {
                row.classList.add('news-red');
                if (warningNode) warningNode.innerHTML = '⚠ NEWS RELEASED<br>';
            } else {
                row.classList.add('news-normal');
                if (warningNode) warningNode.innerHTML = '';
            }
        });
    }

    updateNewsCountdowns();
    setInterval(updateNewsCountdowns, 1000);


    function paintReactionBadge(node, status, stage, waitingText) {
        if (!node) return;
        node.classList.remove('reading', 'bullish', 'bearish', 'mixed');

        if (status === 'BULLISH') {
            node.classList.add('bullish');
            node.textContent = stage + ' ▲ BULLISH';
        } else if (status === 'BEARISH') {
            node.classList.add('bearish');
            node.textContent = stage + ' ▼ BEARISH';
        } else if (status === 'MIXED') {
            node.classList.add('mixed');
            node.textContent = stage + ' ↔ MIXED';
        } else {
            node.classList.add('reading');
            node.textContent = stage + ' ' + waitingText;
        }
    }

    function renderReactionGroup(groupNode, item) {
        const eventMs = Number(groupNode.dataset.eventMs || 0);
        const ageMin = eventMs ? ((Date.now() - eventMs) / 60000) : 0;
        const holder = groupNode.querySelector('.js-news-pair-outcomes');
        if (!holder) return;

        const pairs = Array.isArray(item.pairs) ? item.pairs : [];
        holder.innerHTML = '';

        if (!pairs.length) {
            const waitRow = document.createElement('div');
            waitRow.className = 'news-pair-outcome-row';
            waitRow.textContent = ageMin < 15
                ? 'Waiting for 15M / 30M market reaction...'
                : 'Checking affected saved pairs...';
            holder.appendChild(waitRow);
            return;
        }

        pairs.forEach(function(pair) {
            const row = document.createElement('div');
            row.className = 'news-pair-outcome-row';

            const symbol = document.createElement('span');
            symbol.className = 'news-pair-symbol';
            symbol.textContent = pair.symbol || '';

            const b15 = document.createElement('span');
            b15.className = 'news-reaction reading';
            paintReactionBadge(
                b15,
                pair.reaction_15 || '',
                '15M',
                ageMin < 15 ? 'READING' : 'CHECKING'
            );

            const b30 = document.createElement('span');
            b30.className = 'news-reaction reading';
            paintReactionBadge(
                b30,
                pair.reaction_30 || '',
                '30M',
                ageMin < 30 ? 'WAIT' : 'CHECKING'
            );

            row.appendChild(symbol);
            row.appendChild(b15);
            row.appendChild(b30);
            holder.appendChild(row);
        });
    }

    async function refreshNewsReactions() {
        try {
            const response = await fetch('/news-reactions?ts=' + Date.now(), {
                cache: 'no-store'
            });
            if (!response.ok) return;

            const payload = await response.json();

            document.querySelectorAll('.js-news-outcome-group').forEach(function(groupNode) {
                const eventId = groupNode.dataset.eventId || '';
                renderReactionGroup(groupNode, payload[eventId] || {});
            });
        } catch (e) {
            // Keep the last visible outcome if the tiny status request fails.
        }
    }

    refreshNewsReactions();
    setInterval(refreshNewsReactions, 30000);

    const refreshBtn = document.getElementById('news-refresh-btn');
    const refreshStatus = document.getElementById('news-refresh-status');

    if (refreshBtn) {
        refreshBtn.addEventListener('click', async function () {
            const oldText = refreshBtn.textContent;
            refreshBtn.disabled = true;
            refreshBtn.textContent = '↻ Updating...';

            if (refreshStatus) {
                refreshStatus.style.display = 'block';
                refreshStatus.textContent = 'Refreshing Forex Factory high-impact news...';
            }

            try {
                const response = await fetch('/refresh-news', {
                    method: 'POST',
                    headers: {'X-Requested-With': 'XMLHttpRequest'},
                    cache: 'no-store'
                });

                const result = await response.json();

                if (result.ok) {
                    if (refreshStatus) refreshStatus.textContent = '✓ News updated.';
                    setTimeout(function () {
                        window.location.replace('/?news=' + Date.now());
                    }, 300);
                } else {
                    if (refreshStatus) {
                        refreshStatus.textContent =
                            '⚠ Forex Factory did not return an update just now. Existing news remains displayed.';
                    }
                    refreshBtn.disabled = false;
                    refreshBtn.textContent = oldText;
                }
            } catch (err) {
                if (refreshStatus) {
                    refreshStatus.textContent =
                        '⚠ News refresh failed temporarily. Existing news remains displayed.';
                }
                refreshBtn.disabled = false;
                refreshBtn.textContent = oldText;
            }
        });
    }
})();
</script>
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

.weekly-spike{
    background:#21183b;
    border:2px solid #b56cff;
    border-radius:14px;
    padding:14px;
    margin-top:14px;
    box-shadow:0 0 0 1px rgba(181,108,255,.08) inset;
}
.weekly-spike-title{
    color:#d8a7ff;
    font-size:18px;
    font-weight:900;
}
.weekly-spike-detail{
    color:#d7c5e8;
    font-size:13px;
    line-height:1.45;
    margin-top:6px;
}
</style>
</head>
<body>
<div class="wrap">
    <h1>⚡ {{ symbol }} EDO SETUP SIGNAL</h1>
    <div class="small">{{ group }} • Your price-action method • Trend Pullback: 4H / 8H / 1D • S/R Gap-Retest: 8H / 1D / 1W • Closed candles only • Manual trade decision</div>

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
                <div class="small">Latest FULLY CLOSED candle</div>
                <div class="pricebig">{{ price }}</div>
                <div class="small">{{ latest_closed_date }} UTC</div>
                {% if market_source %}
                <div class="small">Data source: {{ market_source }}</div>
                {% endif %}
            </div>
            <div style="text-align:right">
                <div class="small">Timeframe</div>
                <div class="badge">{{ selected_label }}</div>
            </div>
        </div>

        <div class="signal {{ signal_css }}">{{ signal_icon }} {{ signal }}</div>
        <div class="small" style="margin-top:6px">{{ summary }}</div>

        {% if weekly_spike %}
        <div class="weekly-spike">
            <div class="weekly-spike-title">🟣 WEEKLY TREND SPIKE DETECTED</div>
            <div class="weekly-spike-detail">
                {{ weekly_spike['message'] }}
            </div>
        </div>
        {% endif %}

        {% if patterns %}
            <div class="section-title newest-title">⚡ NEWEST TRIGGER</div>

            {% set newest = patterns[0] %}
            <div class="pattern newest-pattern">
                <div class="newest-badge">
                    {% if newest.get('confirmation_date') == latest_closed_date %}
                        NEWEST CLOSED-CANDLE TRIGGER
                    {% else %}
                        MOST RECENT HISTORICAL TRIGGER
                    {% endif %}
                </div>
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
                    No recent candle sequence matches your 2+ candle trend-pullback confirmation rule on this timeframe.
                </div>
            </div>
        {% endif %}

        <div class="small" style="margin-top:12px">
            Updated: {{ updated }}. The headline refers ONLY to the newest fully closed candle.
            If no new setup triggered on that candle, the older valid pattern below is labelled
            historical. The scanner uses closed candles only and does not place trades.
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
        CREATE TABLE IF NOT EXISTS pair_daily_pushes(
            grp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            local_day TEXT NOT NULL,
            signal_name TEXT NOT NULL,
            direction TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            PRIMARY KEY(grp, symbol, local_day)
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS pattern_daily_pushes(
            grp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            signal_family TEXT NOT NULL,
            direction TEXT NOT NULL,
            local_day TEXT NOT NULL,
            pattern_name TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            PRIMARY KEY(grp, symbol, signal_family, direction, local_day)
        )
        """)

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

        c.execute("""
        CREATE TABLE IF NOT EXISTS weekly_spike_notifications(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            grp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            candle_date TEXT NOT NULL,
            side TEXT NOT NULL,
            wick_ratio REAL,
            sent_at TEXT NOT NULL,
            UNIQUE(grp, symbol, candle_date, side)
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS economic_news(
            event_id TEXT PRIMARY KEY,
            event_time_utc TEXT NOT NULL,
            country TEXT,
            currency TEXT,
            event_name TEXT NOT NULL,
            category TEXT,
            importance INTEGER NOT NULL DEFAULT 3,
            fetched_at TEXT NOT NULL
        )
        """)

        c.execute("""
        CREATE INDEX IF NOT EXISTS idx_economic_news_time
        ON economic_news(event_time_utc)
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS economic_news_pushes(
            event_id TEXT PRIMARY KEY,
            sent_at TEXT NOT NULL
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS economic_news_reactions(
            event_id TEXT PRIMARY KEY,
            reaction_15 TEXT,
            score_15 REAL,
            reaction_30 TEXT,
            score_30 REAL,
            updated TEXT
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS economic_news_pair_reactions(
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            grp TEXT NOT NULL,
            display_symbol TEXT NOT NULL,
            reaction_15 TEXT,
            score_15 REAL,
            reaction_30 TEXT,
            score_30 REAL,
            updated TEXT,
            PRIMARY KEY(event_id, symbol, grp)
        )
        """)

        # Keep older persistent Railway databases compatible with the
        # 15m / 30m news-reaction feature. CREATE TABLE IF NOT EXISTS does
        # not add columns to an already existing table, so migrate safely.
        for _col, _type in (
            ("reaction_15", "TEXT"),
            ("score_15", "REAL"),
            ("reaction_30", "TEXT"),
            ("score_30", "REAL"),
            ("updated", "TEXT"),
        ):
            try:
                c.execute(f"ALTER TABLE economic_news_reactions ADD COLUMN {_col} {_type}")
            except sqlite3.OperationalError:
                pass

        c.execute("""
        CREATE TABLE IF NOT EXISTS economic_news_status(
            id INTEGER PRIMARY KEY CHECK(id=1),
            last_attempt TEXT,
            last_success TEXT,
            last_error TEXT,
            source_url TEXT
        )
        """)
        c.execute("""
        INSERT OR IGNORE INTO economic_news_status(
            id,last_attempt,last_success,last_error,source_url
        )
        VALUES(1,'','','','')
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS weekly_spike_state(
            grp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            last_closed_date TEXT NOT NULL DEFAULT '',
            updated TEXT,
            PRIMARY KEY(grp, symbol)
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



# -------------------------------------------------
# HIGH-IMPACT ECONOMIC NEWS CALENDAR — FOREX FACTORY
# -------------------------------------------------
# INFORMATION ONLY. It never cancels, creates, or changes a trading setup.
# Edo decides whether to hold an entry around important scheduled news.
#
# Source:
#   Forex Factory weekly JSON calendar export
#
# No API key is required.
# The HOME page never downloads the feed directly. A background thread refreshes
# the feed and stores HIGH-impact events in SQLite so EdoSignal stays fast.

CFD_NEWS_CURRENCY = {
    "SP500": "USD", "US500": "USD", "SPX500": "USD", "GSPC": "USD",
    "DJ30": "USD", "US30": "USD", "DOW30": "USD", "DJI": "USD",
    "NAS100": "USD", "US100": "USD", "NASDAQ100": "USD", "USTEC": "USD", "NDX": "USD",
    "DAX": "EUR", "DE40": "EUR", "GER40": "EUR",
    "FTSE": "GBP", "UK100": "GBP",
    "N225": "JPY", "JPN225": "JPY",
    "STOXX50E": "EUR", "EU50": "EUR",
}


def normalize_pair_symbol(symbol):
    return "".join(ch for ch in str(symbol).upper() if ch.isalpha())


def currencies_for_market(symbol, grp):
    """
    Return currencies whose HIGH-impact news is relevant to this saved market.
    """
    s = normalize_pair_symbol(symbol)

    if grp == "FOREX":
        if len(s) >= 6:
            return {s[:3], s[3:6]}
        return set()

    if grp == "CFD":
        ccy = CFD_NEWS_CURRENCY.get(s)
        return {ccy} if ccy else set()

    # Major USD news can matter to crypto quoted against USD/USDT.
    if grp == "CRYPTO":
        if s.endswith("USD") or s.endswith("USDT"):
            return {"USD"}

    return set()


def relevant_news_currencies_from_favorites():
    currencies = set()
    try:
        with db_conn() as c:
            rows = c.execute(
                "SELECT symbol, grp FROM favorites "
                "WHERE grp IN ('FOREX','CRYPTO','CFD')"
            ).fetchall()
        for row in rows:
            currencies.update(currencies_for_market(row["symbol"], row["grp"]))
    except Exception as e:
        print("news favorites error", e)

    if not currencies:
        currencies = {"USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY", "NZD"}

    return currencies


def parse_ff_event_time(value):
    """
    Forex Factory JSON dates include an explicit UTC offset.
    Keep that offset, convert to UTC for storage, and later display in Perth.
    """
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return None
        return dt.astimezone(timezone.utc)
    except Exception:
        return None



def _save_news_feed_status(success, error="", source_url=""):
    """Persist Forex Factory feed health so HOME never confuses failure with no-news."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        with db_conn() as c:
            if success:
                c.execute(
                    """
                    UPDATE economic_news_status
                    SET last_attempt=?, last_success=?, last_error='', source_url=?
                    WHERE id=1
                    """,
                    (now, now, source_url)
                )
            else:
                c.execute(
                    """
                    UPDATE economic_news_status
                    SET last_attempt=?, last_error=?, source_url=?
                    WHERE id=1
                    """,
                    (now, str(error)[:500], source_url)
                )
            c.commit()
    except Exception as e:
        print("news feed status save error", e)


def _get_news_feed_status():
    try:
        with db_conn() as c:
            row = c.execute(
                """
                SELECT last_attempt,last_success,last_error,source_url
                FROM economic_news_status
                WHERE id=1
                """
            ).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}


def _download_forex_factory_calendar():
    """
    Fetch the weekly Forex Factory JSON with two public FairEconomy endpoints.

    Railway/cloud hosts can occasionally get a transient block or edge failure
    on one hostname, so EdoSignal tries the normal endpoint first and then the
    CDN hostname. A cache-busting query string and normal browser headers are
    used to avoid stale/error edge responses.
    """
    urls = []
    for url in (
        FOREX_FACTORY_CALENDAR_URL,
        FOREX_FACTORY_CALENDAR_FALLBACK_URL,
    ):
        if url and url not in urls:
            urls.append(url)

    errors = []

    for base_url in urls:
        try:
            separator = "&" if "?" in base_url else "?"
            url = f"{base_url}{separator}edo_ts={int(time.time())}"

            r = requests.get(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/140.0 Safari/537.36"
                    ),
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    "Connection": "close",
                },
                timeout=20,
                allow_redirects=True,
            )

            if r.status_code != 200:
                errors.append(f"{base_url}: HTTP {r.status_code}")
                continue

            body = (r.text or "").strip()
            if not body:
                errors.append(f"{base_url}: empty response")
                continue

            # Guard against Cloudflare/HTML pages being returned with HTTP 200.
            if body[0] not in "[{":
                errors.append(f"{base_url}: non-JSON response")
                continue

            data = r.json()
            if not isinstance(data, list) or not data:
                errors.append(f"{base_url}: JSON list empty/unusable")
                continue

            return data, base_url, None

        except Exception as e:
            errors.append(f"{base_url}: {type(e).__name__}: {e}")

    return None, "", " | ".join(errors) if errors else "No Forex Factory URL available."



def refresh_economic_news():
    """
    Refresh ALL supported HIGH-impact Forex Factory events into SQLite.

    Favorites are applied later when deciding what to display on the HOME page.
    This prevents a temporary Favorites lookup issue from wiping valid news.
    """
    supported_currencies = {"USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY", "NZD"}
    now_utc = datetime.now(timezone.utc)

    try:
        data, source_url, fetch_error = _download_forex_factory_calendar()

        if not data:
            _save_news_feed_status(False, fetch_error or "No usable event list.", source_url)
            return False, fetch_error or "Forex Factory calendar returned no usable event list."

        fetched = datetime.utcnow().isoformat()
        rows = []

        for item in data:
            impact = str(item.get("impact") or "").strip().lower()
            if impact != "high":
                continue

            currency = str(item.get("country") or "").strip().upper()
            if currency not in supported_currencies:
                continue

            event_name = str(item.get("title") or "High-impact economic event").strip()
            event_dt = parse_ff_event_time(item.get("date"))
            if event_dt is None:
                continue

            # Keep just-passed events briefly plus all upcoming events in this week.
            if event_dt < now_utc - timedelta(hours=2):
                continue

            event_id = f"{currency}|{event_name}|{event_dt.isoformat()}"

            rows.append((
                event_id,
                event_dt.isoformat(),
                "",
                currency,
                event_name,
                "Forex Factory High Impact",
                3,
                fetched,
            ))

        # Never erase a good cache because of a temporary empty/bad feed.
        if not rows:
            error = "Forex Factory returned no usable upcoming High-impact rows; existing cache kept."
            _save_news_feed_status(False, error, source_url)
            return False, error

        with db_conn() as c:
            c.execute("DELETE FROM economic_news")
            c.executemany(
                """
                INSERT OR REPLACE INTO economic_news(
                    event_id,event_time_utc,country,currency,event_name,
                    category,importance,fetched_at
                )
                VALUES(?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            c.commit()

        _save_news_feed_status(True, "", source_url)
        print(
            f"Forex Factory news cache refreshed: {len(rows)} High-impact event(s) "
            f"from {source_url}"
        )
        return True, None

    except Exception as e:
        print("Forex Factory economic news refresh error", e)
        _save_news_feed_status(False, str(e), "")
        return False, str(e)


def economic_news_monitor():
    """
    Background-only Forex Factory calendar refresh.
    This does not consume Twelve Data credits and never blocks the HOME page.
    """
    time.sleep(20)

    while True:
        try:
            ok, error = refresh_economic_news()
            if error:
                print("Forex Factory news refresh:", error)
        except Exception as e:
            print("economic news monitor error", e)

        time.sleep(max(300, NEWS_REFRESH_SECONDS))



def reserve_economic_news_push(event_id):
    """
    Persistently reserve this event so the 5-minute sound warning is sent once,
    even if Railway restarts or multiple monitor loops see the same event.
    """
    try:
        with db_conn() as c:
            c.execute(
                """
                INSERT INTO economic_news_pushes(event_id, sent_at)
                VALUES(?,?)
                """,
                (event_id, datetime.utcnow().isoformat())
            )
            c.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def economic_news_warning_monitor():
    """
    Check cached HIGH-impact Forex Factory events once per minute.

    Around 5 minutes before an event:
      - send THREE Pushover siren warnings in a short burst
      - include affected saved pairs/markets
      - do not alter or cancel any Edo trading signal

    This monitor reads SQLite only and uses NO Twelve Data credits.
    """
    time.sleep(35)

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            window_start = now_utc + timedelta(minutes=max(0, NEWS_WARNING_MINUTES - 1))
            window_end = now_utc + timedelta(minutes=NEWS_WARNING_MINUTES)

            with db_conn() as c:
                rows = c.execute(
                    """
                    SELECT event_id,event_time_utc,currency,event_name
                    FROM economic_news
                    WHERE event_time_utc >= ?
                      AND event_time_utc <= ?
                    ORDER BY event_time_utc ASC
                    """,
                    (window_start.isoformat(), window_end.isoformat())
                ).fetchall()

            perth = ZoneInfo("Australia/Perth")

            for row in rows:
                event_id = row["event_id"]

                if not reserve_economic_news_push(event_id):
                    continue

                event_dt = datetime.fromisoformat(row["event_time_utc"])
                if event_dt.tzinfo is None:
                    event_dt = event_dt.replace(tzinfo=timezone.utc)

                currency = str(row["currency"] or "").upper()
                affected = saved_markets_for_news_currency(currency)
                affected_text = ", ".join(affected) if affected else "No saved pair matched"

                title = f"📰 {currency} HIGH-IMPACT NEWS — 5 MIN"
                message = (
                    f"{row['event_name']}\\n"
                    f"Due {event_dt.astimezone(perth).strftime('%H:%M')} Perth\\n"
                    f"Affects: {affected_text}\\n"
                    f"⚠ Consider holding a new entry until the news has passed."
                )

                # Edo 5-minute news warning: THREE sirens in a row.
                # Three separate Pushover notifications are sent about
                # three seconds apart. Persistent event dedupe means this
                # burst happens only once for each economic event.
                delivered = False

                for siren_no in range(1, 4):
                    ok = send_push(
                        title,
                        message,
                        sound=NEWS_PUSH_SOUND
                    )
                    delivered = delivered or ok

                    if siren_no < 3:
                        time.sleep(3)

                # If none of the three notifications could be delivered,
                # release the reservation so a later monitor pass can retry.
                if not delivered:
                    try:
                        with db_conn() as c:
                            c.execute(
                                "DELETE FROM economic_news_pushes WHERE event_id=?",
                                (event_id,)
                            )
                            c.commit()
                    except Exception:
                        pass

        except Exception as e:
            print("economic news warning monitor error", e)

        time.sleep(60)



# -------------------------------------------------
# POST-NEWS MARKET REACTION — 15 MIN / 30 MIN
# -------------------------------------------------
# This is INFORMATION ONLY. It does not create or block a trading signal.
#
# EdoSignal judges how the affected CURRENCY actually moved after the release,
# rather than trying to label the economic number itself bullish or bearish.
#
# A small basket of FX crosses is used for each currency. Pair moves are
# direction-normalised so, for example:
#   EUR/USD up  -> EUR strength
#   USD/CAD up  -> CAD weakness (therefore inverted for CAD)
#
# The reading stays MIXED / LITTLE MOVE unless the basket has a clear majority
# and a meaningful median move. This is intentionally conservative.

NEWS_REACTION_BASKETS = {
    "EUR": [("EUR/USD",  1), ("EUR/GBP",  1), ("EUR/JPY",  1)],
    "USD": [("EUR/USD", -1), ("GBP/USD", -1), ("USD/JPY",  1)],
    "GBP": [("GBP/USD",  1), ("EUR/GBP", -1), ("GBP/JPY",  1)],
    "AUD": [("AUD/USD",  1), ("AUD/JPY",  1), ("EUR/AUD", -1)],
    "CAD": [("USD/CAD", -1), ("CAD/JPY",  1), ("EUR/CAD", -1)],
    "CHF": [("USD/CHF", -1), ("EUR/CHF", -1), ("CHF/JPY",  1)],
    "JPY": [("USD/JPY", -1), ("EUR/JPY", -1), ("GBP/JPY", -1)],
    "NZD": [("NZD/USD",  1), ("NZD/JPY",  1), ("EUR/NZD", -1)],
}

NEWS_REACTION_MIN_MOVE_15 = 0.0005   # legacy basket threshold
NEWS_REACTION_MIN_MOVE_30 = 0.0008   # legacy basket threshold

# Pair-specific news outcome threshold. The user wants the actual affected
# market direction, not a majority vote across unrelated USD/EUR crosses.
# Only a nearly flat move is labelled MIXED.
NEWS_PAIR_MIN_MOVE = 0.0001  # 0.01%


def _saved_news_markets(currency):
    """Return saved affected markets with raw symbol/group + display symbol."""
    currency = str(currency).upper().strip()
    out = []
    try:
        with db_conn() as c:
            rows = c.execute(
                "SELECT symbol, grp FROM favorites "
                "WHERE grp IN ('FOREX','CRYPTO','CFD') ORDER BY grp,symbol"
            ).fetchall()

        seen = set()
        for row in rows:
            symbol = str(row["symbol"])
            grp = str(row["grp"]).upper()
            if currency not in currencies_for_market(symbol, grp):
                continue

            display_symbol = symbol.upper()
            if grp == "FOREX":
                compact = normalize_pair_symbol(display_symbol)
                if len(compact) >= 6:
                    display_symbol = f"{compact[:3]}/{compact[3:6]}"

            key = (symbol.upper(), grp)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "symbol": symbol,
                "grp": grp,
                "display_symbol": display_symbol,
            })
    except Exception as e:
        print("saved news markets error", e)
    return out


def _news_pair_reaction(symbol, grp, event_dt, minutes_after):
    """Actual price direction of one affected saved market after the release."""
    candles, error = get_ohlc(symbol, "1min", outputsize=120, grp=grp)
    if not candles:
        return None, None

    before = _price_close_at_or_before(candles, event_dt)
    after = _price_close_at_or_before(
        candles, event_dt + timedelta(minutes=int(minutes_after))
    )
    if before is None or after is None or before == 0:
        return None, None

    move = (after - before) / before
    if move >= NEWS_PAIR_MIN_MOVE:
        return "BULLISH", move
    if move <= -NEWS_PAIR_MIN_MOVE:
        return "BEARISH", move
    return "MIXED", move


def _save_news_pair_reaction(event_id, market, minutes_after, reaction, score):
    col = "reaction_15" if int(minutes_after) == 15 else "reaction_30"
    score_col = "score_15" if int(minutes_after) == 15 else "score_30"
    with db_conn() as c:
        c.execute(
            f"""
            INSERT INTO economic_news_pair_reactions(
                event_id,symbol,grp,display_symbol,{col},{score_col},updated
            ) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(event_id,symbol,grp) DO UPDATE SET
                display_symbol=excluded.display_symbol,
                {col}=excluded.{col},
                {score_col}=excluded.{score_col},
                updated=excluded.updated
            """,
            (
                event_id, market["symbol"], market["grp"], market["display_symbol"],
                reaction, score, datetime.utcnow().isoformat()
            )
        )
        c.commit()


def _price_close_at_or_before(candles, target_utc):
    """Return latest 1-minute candle close whose candle is fully closed by target_utc."""
    best = None
    for c in candles or []:
        start = parse_candle_utc(c.get("datetime", ""))
        if start is None:
            continue
        end = start + timedelta(minutes=1)
        if end <= target_utc:
            best = float(c["close"])
        else:
            break
    return best


def _news_currency_reaction(currency, event_dt, minutes_after):
    """
    Calculate the actual currency reaction from the release to +15m or +30m.

    Returns:
      ("BULLISH" | "BEARISH" | "MIXED", median_signed_return)
    or (None, None) if there is not enough market data yet.
    """
    basket = NEWS_REACTION_BASKETS.get(str(currency).upper(), [])
    if not basket:
        return None, None

    baseline_target = event_dt
    target = event_dt + timedelta(minutes=int(minutes_after))

    signed_moves = []

    for pair, direction in basket:
        candles, error = get_ohlc(pair, "1min", outputsize=120, grp="FOREX")
        if not candles:
            continue

        before = _price_close_at_or_before(candles, baseline_target)
        after = _price_close_at_or_before(candles, target)

        if before is None or after is None or before == 0:
            continue

        raw_return = (after - before) / before
        signed_moves.append(raw_return * direction)

    # Require at least two usable crosses.
    if len(signed_moves) < 2:
        return None, None

    signed_moves.sort()
    n = len(signed_moves)
    if n % 2:
        median_move = signed_moves[n // 2]
    else:
        median_move = (signed_moves[n // 2 - 1] + signed_moves[n // 2]) / 2.0

    threshold = (
        NEWS_REACTION_MIN_MOVE_15
        if int(minutes_after) <= 15
        else NEWS_REACTION_MIN_MOVE_30
    )

    bullish_votes = sum(1 for x in signed_moves if x > 0)
    bearish_votes = sum(1 for x in signed_moves if x < 0)

    # Conservative majority requirement.
    majority_needed = 2 if len(signed_moves) >= 3 else 2

    if bullish_votes >= majority_needed and median_move >= threshold:
        return "BULLISH", median_move

    if bearish_votes >= majority_needed and median_move <= -threshold:
        return "BEARISH", median_move

    return "MIXED", median_move


def _save_news_reaction(event_id, minutes_after, reaction, score):
    column = "reaction_15" if int(minutes_after) == 15 else "reaction_30"
    score_column = "score_15" if int(minutes_after) == 15 else "score_30"

    with db_conn() as c:
        c.execute(
            f"""
            INSERT INTO economic_news_reactions(event_id,{column},{score_column},updated)
            VALUES(?,?,?,?)
            ON CONFLICT(event_id) DO UPDATE SET
                {column}=excluded.{column},
                {score_column}=excluded.{score_column},
                updated=excluded.updated
            """,
            (event_id, reaction, score, datetime.utcnow().isoformat())
        )
        c.commit()


def economic_news_reaction_monitor():
    """
    After each HIGH-impact event:
      +15 minutes -> first conservative reaction reading
      +30 minutes -> stronger second reading

    Events sharing the same currency and release time reuse one calculation,
    so a rate decision + statement released together do not waste API credits.
    """
    time.sleep(45)

    while True:
        try:
            now_utc = datetime.now(timezone.utc)

            with db_conn() as c:
                rows = c.execute(
                    """
                    SELECT n.event_id,n.event_time_utc,n.currency,
                           r.reaction_15,r.reaction_30
                    FROM economic_news n
                    LEFT JOIN economic_news_reactions r
                      ON r.event_id=n.event_id
                    WHERE n.event_time_utc <= ?
                      AND n.event_time_utc >= ?
                    ORDER BY n.event_time_utc ASC
                    """,
                    (
                        (now_utc - timedelta(minutes=15)).isoformat(),
                        (now_utc - timedelta(hours=2)).isoformat(),
                    )
                ).fetchall()

            # Group identical currency + release-time events.
            groups = {}
            for row in rows:
                key = (str(row["currency"]).upper(), row["event_time_utc"])
                groups.setdefault(key, []).append(row)

            for (currency, event_time_text), group_rows in groups.items():
                event_dt = datetime.fromisoformat(event_time_text)
                if event_dt.tzinfo is None:
                    event_dt = event_dt.replace(tzinfo=timezone.utc)

                age_minutes = (now_utc - event_dt).total_seconds() / 60.0

                need_15 = age_minutes >= 15 and any(not r["reaction_15"] for r in group_rows)
                need_30 = age_minutes >= 30 and any(not r["reaction_30"] for r in group_rows)

                # One calculation is reused for every same-time event.
                if need_15:
                    reaction, score = _news_currency_reaction(currency, event_dt, 15)
                    if reaction:
                        for row in group_rows:
                            if not row["reaction_15"]:
                                _save_news_reaction(row["event_id"], 15, reaction, score)

                if need_30:
                    reaction, score = _news_currency_reaction(currency, event_dt, 30)
                    if reaction:
                        for row in group_rows:
                            if not row["reaction_30"]:
                                _save_news_reaction(row["event_id"], 30, reaction, score)

                # Pair-specific outcomes: each affected SAVED market gets its own
                # actual 15m/30m price direction. This is what the home page shows.
                # Simultaneous events (e.g. four USD CPI releases) share the same
                # market calculation and the result is copied to each event id.
                markets = _saved_news_markets(currency)
                for market in markets:
                    with db_conn() as c:
                        existing = c.execute(
                            """
                            SELECT reaction_15,reaction_30
                            FROM economic_news_pair_reactions
                            WHERE event_id=? AND symbol=? AND grp=?
                            """,
                            (group_rows[0]["event_id"], market["symbol"], market["grp"])
                        ).fetchone()

                    pair_need_15 = age_minutes >= 15 and (not existing or not existing["reaction_15"])
                    pair_need_30 = age_minutes >= 30 and (not existing or not existing["reaction_30"])

                    if pair_need_15:
                        pr, ps = _news_pair_reaction(
                            market["symbol"], market["grp"], event_dt, 15
                        )
                        if pr:
                            for row in group_rows:
                                _save_news_pair_reaction(row["event_id"], market, 15, pr, ps)

                    if pair_need_30:
                        pr, ps = _news_pair_reaction(
                            market["symbol"], market["grp"], event_dt, 30
                        )
                        if pr:
                            for row in group_rows:
                                _save_news_pair_reaction(row["event_id"], market, 30, pr, ps)

        except Exception as e:
            print("economic news reaction monitor error", e)

        time.sleep(60)


def news_reaction_payload():
    """Small DB-only payload used by the home page for live reaction badges."""
    try:
        with db_conn() as c:
            rows = c.execute(
                """
                SELECT n.event_id,n.event_time_utc,n.currency,
                       r.reaction_15,r.score_15,r.reaction_30,r.score_30
                FROM economic_news n
                LEFT JOIN economic_news_reactions r
                  ON r.event_id=n.event_id
                WHERE n.event_time_utc >= ?
                ORDER BY n.event_time_utc ASC
                """,
                ((datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),)
            ).fetchall()

        result = {}
        for row in rows:
            r15 = row["reaction_15"]
            r30 = row["reaction_30"]

            status = "READING"
            stage = ""
            confirmed = False

            if r30:
                status = r30
                stage = "30M"
                confirmed = bool(r15 and r30 == r15 and r30 in ("BULLISH", "BEARISH"))
            elif r15:
                status = r15
                stage = "15M"

            result[row["event_id"]] = {
                "status": status,
                "stage": stage,
                "confirmed": confirmed,
                "reaction_15": r15 or "",
                "reaction_30": r30 or "",
                "score_15": row["score_15"],
                "score_30": row["score_30"],
                "pairs": [],
            }

        if result:
            placeholders = ",".join("?" for _ in result)
            with db_conn() as c:
                pair_rows = c.execute(
                    f"""
                    SELECT event_id,display_symbol,reaction_15,score_15,reaction_30,score_30
                    FROM economic_news_pair_reactions
                    WHERE event_id IN ({placeholders})
                    ORDER BY display_symbol
                    """,
                    tuple(result.keys())
                ).fetchall()

            for pr in pair_rows:
                if pr["event_id"] not in result:
                    continue
                result[pr["event_id"]]["pairs"].append({
                    "symbol": pr["display_symbol"],
                    "reaction_15": pr["reaction_15"] or "",
                    "reaction_30": pr["reaction_30"] or "",
                    "score_15": pr["score_15"],
                    "score_30": pr["score_30"],
                })

        return result

    except Exception as e:
        print("news reaction payload error", e)
        return {}


def news_warning_level(minutes_until):
    if minutes_until < -60:
        return "past"
    if minutes_until <= 60:
        return "red"
    if minutes_until <= 240:
        return "amber"
    return "normal"



def saved_markets_for_news_currency(currency, max_items=8):
    """
    Return saved markets affected by a given news currency.

    Examples:
      EUR -> EUR/CAD, EUR/USD, EUR/GBP ...
      USD -> GBP/USD, AUD/USD, NAS100, SP500, BTCUSD ...
    """
    currency = str(currency).upper().strip()
    affected = []

    try:
        with db_conn() as c:
            rows = c.execute(
                "SELECT symbol, grp FROM favorites "
                "WHERE grp IN ('FOREX','CRYPTO','CFD') "
                "ORDER BY grp,symbol"
            ).fetchall()

        for row in rows:
            symbol = row["symbol"]
            grp = row["grp"]
            if currency in currencies_for_market(symbol, grp):
                display_symbol = str(symbol).upper()
                if grp == "FOREX":
                    s = normalize_pair_symbol(display_symbol)
                    if len(s) >= 6:
                        display_symbol = f"{s[:3]}/{s[3:6]}"
                affected.append(display_symbol)

    except Exception as e:
        print("affected news markets error", e)

    # Deduplicate while preserving order.
    seen = set()
    unique = []
    for item in affected:
        if item not in seen:
            seen.add(item)
            unique.append(item)

    return unique[:max_items]



def cached_home_news(limit=6):
    """
    Read cached HIGH-impact Forex Factory events for the HOME page only.
    No external web/API call is made in the home request.
    """
    now_utc = datetime.now(timezone.utc)
    perth = ZoneInfo("Australia/Perth")

    try:
        with db_conn() as c:
            rows = c.execute(
                """
                SELECT n.event_id,n.event_time_utc,n.currency,n.event_name,n.importance,
                       r.reaction_15,r.reaction_30
                FROM economic_news n
                LEFT JOIN economic_news_reactions r
                  ON r.event_id=n.event_id
                WHERE n.event_time_utc >= ?
                ORDER BY n.event_time_utc ASC
                LIMIT ?
                """,
                (
                    (now_utc - timedelta(hours=2)).isoformat(),
                    max(40, int(limit) * 8),
                ),
            ).fetchall()
    except Exception as e:
        print("home news cache error", e)
        return []

    items = []

    # Combine simultaneous High-impact releases for the same currency into one
    # visible outcome block. Example: four USD CPI releases at 20:30 display as
    # one USD group, because the market reaction is the combined USD outcome.
    grouped = {}
    for row in rows:
        key = (str(row["currency"]).upper(), row["event_time_utc"])
        grouped.setdefault(key, []).append(row)

    for (_currency, _event_time), group_rows in grouped.items():
        try:
            row = group_rows[0]
            event_dt = datetime.fromisoformat(row["event_time_utc"])
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)

            minutes_until = int(
                (event_dt.astimezone(timezone.utc) - now_utc).total_seconds() / 60
            )

            if minutes_until < -1:
                mins_ago = abs(minutes_until)
                countdown = f"released {mins_ago} min ago"
            elif minutes_until < 0:
                countdown = "NOW / just released"
            elif minutes_until < 60:
                countdown = f"in {minutes_until} min"
            elif minutes_until < 24 * 60:
                h = minutes_until // 60
                m = minutes_until % 60
                countdown = f"in {h}h {m:02d}m"
            else:
                countdown = f"in {minutes_until // (24*60)} day(s)"

            affected_pairs = saved_markets_for_news_currency(row["currency"])
            event_names = [str(r["event_name"]) for r in group_rows]
            event_name = (
                event_names[0]
                if len(event_names) == 1
                else f"{len(event_names)} High-Impact events"
            )

            items.append({
                # Every same-time/currency event gets the same calculated
                # reaction, so using the first event id is sufficient for UI.
                "event_id": row["event_id"],
                "currency": row["currency"],
                "event_name": event_name,
                "event_names": event_names,
                "reaction_15": row["reaction_15"],
                "reaction_30": row["reaction_30"],
                "perth_time": event_dt.astimezone(perth).strftime("%a %d %b • %H:%M"),
                "countdown": countdown,
                "level": news_warning_level(minutes_until),
                "affected_pairs": affected_pairs,
                "affected_pairs_text": (
                    ", ".join(affected_pairs)
                    if affected_pairs
                    else "No saved-pair match"
                ),
                "event_time_ms": int(event_dt.timestamp() * 1000),
            })

        except Exception:
            continue

    return items[:int(limit)]



def send_push(title, msg, sound='cashregister'):

    if not PUSHOVER_APP_TOKEN or not PUSHOVER_USER_KEY:
        print('Pushover not configured:', title, msg)
        return False

    try:
        r = requests.post(
            'https://api.pushover.net/1/messages.json',
            data={
                'token': PUSHOVER_APP_TOKEN,
                'user': PUSHOVER_USER_KEY,
                'title': title,
                'message': msg,
                'sound': sound
            },
            timeout=10
        )
        return r.ok

    except Exception as e:
        print('push error', e)
        return False


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
        for resolved_symbol in twelve_symbol_candidates(symbol, grp):
            j = twelve_get_json(
                'https://api.twelvedata.com/price',
                {
                    'symbol': resolved_symbol,
                    'apikey': TWELVE_KEY
                },
                timeout=10
            )

            if j.get("status") == "error" or 'price' not in j:
                continue

            price = float(j['price'])

            with _CACHE_LOCK:
                _PRICE_CACHE[cache_key] = {
                    "saved_at": now,
                    "price": price,
                    "source_symbol": resolved_symbol
                }

            if grp == "CFD":
                print("CFD price source", symbol, "->", resolved_symbol)

            return price

        print("price API error", symbol, "No direct or fallback symbol returned a price.")
        return None

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

# Edo's exact signal timeframes.
NORMAL_PULLBACK_INTERVALS = {"4h", "8h", "1day"}
SR_GAP_RETEST_INTERVALS = {"8h", "1day", "1week"}

# Alias retained for older helper code.
CORE_PATTERN_INTERVALS = SR_GAP_RETEST_INTERVALS


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

    last_error = "Twelve Data returned no usable market data."

    try:
        for resolved_symbol in twelve_symbol_candidates(symbol, grp):
            j = twelve_get_json(
                "https://api.twelvedata.com/time_series",
                {
                    "symbol": resolved_symbol,
                    "interval": interval,
                    "outputsize": outputsize,
                    "apikey": TWELVE_KEY,
                    "format": "JSON",
                    "timezone": "UTC",
                },
                timeout=15
            )

            if j.get("status") == "error":
                last_error = j.get("message", "Twelve Data returned an error.")
                continue

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
                        "_source_symbol": resolved_symbol,
                        "_source_label": source_label_for(symbol, grp, resolved_symbol),
                    })
                except (KeyError, TypeError, ValueError):
                    pass

            if len(candles) < 40:
                last_error = f"Not enough {interval} candle history returned for {resolved_symbol}."
                continue

            with _CACHE_LOCK:
                old = _OHLC_CACHE.get(cache_key)
                if not old or len(candles) >= len(old["candles"]):
                    _OHLC_CACHE[cache_key] = {"saved_at": now, "candles": candles}

            if grp == "CFD":
                print("CFD market source", symbol, "->", resolved_symbol)

            return candles, None

        return None, last_error

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


def interval_seconds(interval):
    mapping = {
        "1min": 60,
        "1h": 60 * 60,
        "2h": 2 * 60 * 60,
        "4h": 4 * 60 * 60,
        "8h": 8 * 60 * 60,
        "12h": 12 * 60 * 60,
        "1day": 24 * 60 * 60,
        "1week": 7 * 24 * 60 * 60,
    }
    return mapping.get(interval)


def parse_candle_utc(value):
    if not value:
        return None
    try:
        from datetime import timezone
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fully_closed_candles(candles, interval, now_utc=None):
    """
    Keep only candles whose complete interval has elapsed.
    Twelve Data OHLC is requested in UTC, so this avoids accidentally
    accepting a still-forming 4H/8H/Daily candle as a trigger.
    """
    from datetime import timezone, timedelta

    seconds = interval_seconds(interval)
    if not candles or not seconds:
        return []

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    duration = timedelta(seconds=seconds)
    closed = []

    for c in candles:
        start = parse_candle_utc(c.get("datetime", ""))
        if start is None:
            continue
        if start + duration <= now_utc:
            closed.append(c)

    return closed


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
    Edo NORMAL Trend Pullback confirmation.

    This is deliberately separate from the S/R gap-and-retest setup.

    NORMAL pullback rule:
      - previous candle and confirmation candle must be opposite colours
      - confirmation candle must be fully closed by the caller
      - NO 50% body-penetration requirement

    The minimum 2 same-colour pullback candles are checked separately by
    detect_trend_pullback().
    """
    if i <= 0 or i >= len(candles):
        return None

    prev = candles[i - 1]
    curr = candles[i]

    prev_colour = candle_colour(prev)
    curr_colour = candle_colour(curr)

    if prev_colour == "red" and curr_colour == "green":
        return {
            "direction": "bullish",
            "index": i,
            "date": curr.get("datetime", ""),
            "close": float(curr["close"]),
        }

    if prev_colour == "green" and curr_colour == "red":
        return {
            "direction": "bearish",
            "index": i,
            "date": curr.get("datetime", ""),
            "close": float(curr["close"]),
        }

    return None


def sr_confirmation_at(candles, i):
    """
    Edo S/R GAP-AND-RETEST confirmation — 50% RULE APPLIES HERE ONLY.

    Bearish resistance retest:
      - previous candle is green
      - next fully closed candle is red
      - red candle closes below the 50% midpoint of the previous green body

    Bullish support retest:
      - previous candle is red
      - next fully closed candle is green
      - green candle closes above the 50% midpoint of the previous red body

    This helper is NOT used by the normal minimum-2-candle pullback setup.
    """
    if i <= 0 or i >= len(candles):
        return None

    prev = candles[i - 1]
    curr = candles[i]

    prev_colour = candle_colour(prev)
    curr_colour = candle_colour(curr)

    body = abs(float(prev["close"]) - float(prev["open"]))
    if body <= 0:
        return None

    midpoint = (float(prev["open"]) + float(prev["close"])) / 2.0

    if prev_colour == "red" and curr_colour == "green":
        penetration = ((float(curr["close"]) - float(prev["close"])) / body) * 100.0
        if float(curr["close"]) > midpoint and penetration >= 50.0:
            return {
                "direction": "bullish",
                "index": i,
                "penetration": penetration,
                "date": curr.get("datetime", ""),
                "close": float(curr["close"]),
                "previous_midpoint": midpoint,
            }

    if prev_colour == "green" and curr_colour == "red":
        penetration = ((float(prev["close"]) - float(curr["close"])) / body) * 100.0
        if float(curr["close"]) < midpoint and penetration >= 50.0:
            return {
                "direction": "bearish",
                "index": i,
                "penetration": penetration,
                "date": curr.get("datetime", ""),
                "close": float(curr["close"]),
                "previous_midpoint": midpoint,
            }

    return None


def recent_sr_confirmations(candles, lookback=7):
    """Newest-first 50% confirmations used ONLY for S/R gap-and-retest."""
    found = []
    first = max(1, len(candles) - lookback)

    for i in range(first, len(candles)):
        c = sr_confirmation_at(candles, i)
        if c:
            found.append(c)

    return list(reversed(found))


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
    Edo S/R Gap-and-Retest trading rule.

    IMPORTANT:
      * Historical HIGH = RESISTANCE only -> bearish/SELL confirmation only.
      * Historical LOW  = SUPPORT only    -> bullish/BUY confirmation only.
      * The second touch must retrace at least 50% of the move-away distance
        back toward the original S/R level. It may return all the way to, or
        slightly through, the original level.
      * The opposite-colour confirmation must also satisfy the separate 50%
        previous-candle BODY rule from sr_confirmation_at().
      * Reject the setup if the confirmation candle itself has already shot
        to/near the previous structural target (old high for BUY, old low for
        SELL). There must still be useful room after confirmation.

    These 50% rules apply ONLY to this S/R setup. They are not used by the
    normal minimum-2-candle Trend Pullback setup.
    """
    i = conf["index"]
    direction = conf["direction"]

    if i < 16:
        return None

    ar = avg_range(candles, i + 1, 20)
    if ar <= 0:
        return None

    # The second touch must be immediately before/around the confirmation.
    touch_start = max(3, i - 4)
    touch_end = i + 1
    touch_slice = candles[touch_start:touch_end]
    if not touch_slice:
        return None

    if direction == "bullish":
        # BUY can come ONLY from established SUPPORT (historical swing LOW).
        rel_touch_i = min(range(len(touch_slice)), key=lambda k: touch_slice[k]["low"])
        retest_price = float(touch_slice[rel_touch_i]["low"])
        retest_index = touch_start + rel_touch_i
        swings = swing_points(candles[:touch_start], "low")
        level_source = "historical swing low / support"
        confirm_close = float(candles[i]["close"])
        confirm_extreme = float(candles[i]["high"])
    else:
        # SELL can come ONLY from established RESISTANCE (historical swing HIGH).
        rel_touch_i = max(range(len(touch_slice)), key=lambda k: touch_slice[k]["high"])
        retest_price = float(touch_slice[rel_touch_i]["high"])
        retest_index = touch_start + rel_touch_i
        swings = swing_points(candles[:touch_start], "high")
        level_source = "historical swing high / resistance"
        confirm_close = float(candles[i]["close"])
        confirm_extreme = float(candles[i]["low"])

    # Same-zone tolerance allows ordinary wick variation while preserving the
    # distinction between support and resistance.
    zone_tolerance = max(ar * 0.45, abs(retest_price) * 0.0015)

    candidates = []

    for old_i, old_price in swings:
        old_price = float(old_price)
        separation = touch_start - old_i

        if separation < 8 or separation > 120:
            continue

        # The second touch must still be in the same broad S/R area. The new
        # 50%-depth test below determines how deeply price returned toward it.
        if abs(old_price - retest_price) > zone_tolerance:
            continue

        # Original level must be a meaningful swing extreme, not a minor pivot.
        left = max(0, old_i - 6)
        right = min(touch_start, old_i + 7)
        neighbourhood = candles[left:right]
        if len(neighbourhood) < 5:
            continue

        if direction == "bullish":
            neighbourhood_extreme = min(float(c["low"]) for c in neighbourhood)
            if old_price > neighbourhood_extreme + ar * 0.15:
                continue
        else:
            neighbourhood_extreme = max(float(c["high"]) for c in neighbourhood)
            if old_price < neighbourhood_extreme - ar * 0.15:
                continue

        between = candles[old_i + 1:retest_index]
        if not between:
            continue

        # Price must first move materially AWAY from the original S/R level.
        # Then the second touch must retrace at least 50% of that move back
        # toward the original level. A full return is valid too.
        if direction == "bullish":
            departure_extreme = max(float(c["high"]) for c in between)
            move_distance = departure_extreme - old_price
            moved_away = move_distance
            if move_distance < ar * 2.0:
                continue
            retest_depth_pct = ((departure_extreme - retest_price) / move_distance) * 100.0
        else:
            departure_extreme = min(float(c["low"]) for c in between)
            move_distance = old_price - departure_extreme
            moved_away = move_distance
            if move_distance < ar * 2.0:
                continue
            retest_depth_pct = ((retest_price - departure_extreme) / move_distance) * 100.0

        if retest_depth_pct < 50.0:
            continue

        zone_centre = (old_price + retest_price) / 2.0

        # Before the second touch, price must approach the level from the
        # correct side, then confirmation must close back away from the zone.
        approach_start = max(old_i + 1, retest_index - 4)
        approach = candles[approach_start:retest_index]
        if not approach:
            continue

        if direction == "bullish":
            if max(float(c["close"]) for c in approach) <= zone_centre + ar * 0.20:
                continue
            if confirm_close <= zone_centre:
                continue
        else:
            if min(float(c["close"]) for c in approach) >= zone_centre - ar * 0.20:
                continue
            if confirm_close >= zone_centre:
                continue

        # Confirmation must not consume the whole trade in one candle. Use
        # structure that existed BEFORE the confirmation/retest as the target.
        target = previous_target(candles, direction, retest_index)
        target_near_tol = ar * 0.35
        if target is not None:
            target = float(target)
            if direction == "bullish":
                if target > zone_centre and confirm_extreme >= target - target_near_tol:
                    continue
            else:
                if target < zone_centre and confirm_extreme <= target + target_near_tol:
                    continue

        closeness = 1.0 - min(1.0, abs(old_price - retest_price) / zone_tolerance)
        candidates.append((
            old_i, old_price, separation, closeness, moved_away,
            zone_centre, retest_depth_pct, departure_extreme, target
        ))

    if not candidates:
        return None

    (
        old_i, old_price, separation, closeness, moved_away,
        zone_centre, retest_depth_pct, departure_extreme, target
    ) = max(candidates, key=lambda x: (x[3], x[2]))

    score = 6.0
    score += min(3.0, separation / 15.0)
    score += closeness * 2.0
    score += min(2.0, max(0.0, conf["penetration"] - 50.0) / 25.0)
    score += min(1.5, max(0.0, retest_depth_pct - 50.0) / 35.0)

    return {
        "name": "S/R GAP RETEST SETUP",
        "direction": direction,
        "confirmed": True,
        "score": score,
        "confirmation_date": conf["date"],
        "confirmation_close": conf["close"],
        "penetration": conf["penetration"],
        "retest_depth_pct": retest_depth_pct,
        "departure_extreme": departure_extreme,
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



def detect_support_resistance_signal(candles):
    """
    Edo Support / Resistance REACTION alert for 8H, Daily and Weekly.

    This is NOT a simple "price is near an old level" alert.

    A valid alert requires:
      1) an established support/resistance zone from prior swing reactions
      2) price moved clearly away from that zone after a prior reaction
      3) the newest fully CLOSED candle retests the same zone
      4) the newest candle shows rejection/bounce away from the zone

    Works in bullish, bearish and range-bound markets.
    """
    if not candles or len(candles) < 30:
        return []

    latest = candles[-1]
    history = candles[:-1]
    recent = history[-100:] if len(history) > 100 else history

    latest_open = float(latest["open"])
    latest_high = float(latest["high"])
    latest_low = float(latest["low"])
    latest_close = float(latest["close"])

    recent_ranges = [
        max(float(c["high"]) - float(c["low"]), 0.0)
        for c in recent[-24:]
    ]
    med_range = median_value(recent_ranges)
    if med_range <= 0:
        return []

    zone_tol = med_range * 0.35
    move_away_min = med_range * 1.20

    highs = swing_points(recent, "high")
    lows = swing_points(recent, "low")

    def grouped_zones(points):
        zones = []
        for idx, level in points:
            level = float(level)
            placed = False
            for z in zones:
                center = sum(x["level"] for x in z) / len(z)
                if abs(level - center) <= zone_tol:
                    z.append({"idx": idx, "level": level})
                    placed = True
                    break
            if not placed:
                zones.append([{"idx": idx, "level": level}])

        out = []
        for z in zones:
            if len(z) < 2:
                continue
            center = sum(x["level"] for x in z) / len(z)
            out.append({
                "level": center,
                "touches": len(z),
                "points": z,
            })
        return out

    support_zones = grouped_zones(lows)
    resistance_zones = grouped_zones(highs)

    found = []

    # SUPPORT retest + bounce
    for z in support_zones:
        level = z["level"]

        # newest candle must actually reach/retest support
        if latest_low > level + zone_tol:
            continue

        body_low = min(latest_open, latest_close)
        lower_wick = max(0.0, body_low - latest_low)
        body = abs(latest_close - latest_open)

        # close back above the zone with visible rejection
        rejection_ok = (
            latest_close > level
            and lower_wick >= max(body * 0.50, med_range * 0.12)
        )
        if not rejection_ok:
            continue

        valid_prior = None
        for pt in reversed(z["points"]):
            idx = pt["idx"]
            if idx >= len(recent) - 3:
                continue

            after = recent[idx + 1:]
            if not after:
                continue

            highest_after = max(float(c["high"]) for c in after)
            if highest_after - level >= move_away_min:
                valid_prior = pt
                break

        if valid_prior is None:
            continue

        found.append({
            "name": "SUPPORT RETEST / BOUNCE WATCH",
            "direction": "bullish",
            "confirmed": True,
            "score": 7.0 + min(2.0, z["touches"] * 0.4),
            "confirmation_date": latest.get("datetime", ""),
            "confirmation_close": latest_close,
            "level": level,
            "touches": z["touches"],
            "prior_reaction_index": valid_prior["idx"],
            "context": "support_resistance",
            "trend": local_structure_trend(candles, len(candles)-1),
            "target": None,
        })

    # RESISTANCE retest + rejection
    for z in resistance_zones:
        level = z["level"]

        if latest_high < level - zone_tol:
            continue

        body_high = max(latest_open, latest_close)
        upper_wick = max(0.0, latest_high - body_high)
        body = abs(latest_close - latest_open)

        rejection_ok = (
            latest_close < level
            and upper_wick >= max(body * 0.50, med_range * 0.12)
        )
        if not rejection_ok:
            continue

        valid_prior = None
        for pt in reversed(z["points"]):
            idx = pt["idx"]
            if idx >= len(recent) - 3:
                continue

            after = recent[idx + 1:]
            if not after:
                continue

            lowest_after = min(float(c["low"]) for c in after)
            if level - lowest_after >= move_away_min:
                valid_prior = pt
                break

        if valid_prior is None:
            continue

        found.append({
            "name": "RESISTANCE RETEST / REJECTION WATCH",
            "direction": "bearish",
            "confirmed": True,
            "score": 7.0 + min(2.0, z["touches"] * 0.4),
            "confirmation_date": latest.get("datetime", ""),
            "confirmation_close": latest_close,
            "level": level,
            "touches": z["touches"],
            "prior_reaction_index": valid_prior["idx"],
            "context": "support_resistance",
            "trend": local_structure_trend(candles, len(candles)-1),
            "target": None,
        })

    return found


def detect_trend_pullback(candles, conf, allow_sr_exception=False):
    """
    Edo Trend Pullback rule — NO 50% penetration requirement.

    BEARISH trend / possible SELL:
      1) Minimum 2 consecutive bullish fully CLOSED candles pull upward.
      2) Next fully CLOSED candle is bearish.
      3) No minimum body-penetration percentage is required.

    BULLISH trend / possible BUY:
      1) Minimum 2 consecutive bearish fully CLOSED candles pull downward.
      2) Next fully CLOSED candle is bullish.
      3) No minimum body-penetration percentage is required.

    No forming candle may trigger a signal.
    """
    i = conf["index"]
    if i < 2:
        return None

    direction = conf["direction"]
    confirm_candle = candles[i]
    confirm_close = float(confirm_candle["close"])

    if direction == "bearish":
        run_colour = "green"
        required_trend = "bearish"
        run_count, run_start = count_same_colour_before(candles, i, run_colour)
        if run_count < 2:
            return None

    elif direction == "bullish":
        run_colour = "red"
        required_trend = "bullish"
        run_count, run_start = count_same_colour_before(candles, i, run_colour)
        if run_count < 2:
            return None

    else:
        return None

    trend = local_structure_trend(candles, run_start)

    # Normal rule: setup direction must agree with established local structure.
    # 8H / Daily may keep the candidate only while evaluating Edo's genuine
    # SECOND S/R REACTION WITH GAP early/developing-trend exception.
    if trend != required_trend and not allow_sr_exception:
        return None

    score = 6.0 + min(3.0, (run_count - 2) * 0.75)
    target = previous_target(candles, direction, run_start)

    return {
        "name": "TREND PULLBACK SETUP",
        "direction": direction,
        "confirmed": True,
        "score": score,
        "confirmation_date": conf["date"],
        "confirmation_close": confirm_close,
        "run_count": run_count,
        "run_colour": run_colour,
        "run_dates": [
            candles[j].get("datetime", "")
            for j in range(run_start, i)
        ],
        "trend": trend,
        "required_trend": required_trend,
        "normal_local_trend_ok": trend == required_trend,
        "context": "trend",
        "target": target,
    }

def sr_second_reaction_exception(candles, setup):
    """
    Edo 8H / Daily early-trend exception.

    Allows the normal 2+ same-colour + opposite-colour CLOSED confirmation setup before a
    strong trend is fully established ONLY when the SAME fully closed
    confirmation candle is also a genuine second separated reaction from an
    established historical S/R zone.

    Bullish setup -> second support retest/bounce with a real move-away gap.
    Bearish setup -> second resistance retest/rejection with a real move-away gap.

    This does not create a signal by itself. The normal 2+ / opposite-colour closed-candle
    pattern must already be valid.
    """
    if not candles or not setup:
        return None

    reactions = detect_support_resistance_signal(candles)
    wanted = "SUPPORT RETEST / BOUNCE WATCH" if setup.get("direction") == "bullish" \
        else "RESISTANCE RETEST / REJECTION WATCH"

    confirmation_date = setup.get("confirmation_date", "")

    matches = [
        r for r in reactions
        if r.get("name") == wanted
        and r.get("direction") == setup.get("direction")
        and r.get("confirmation_date", "") == confirmation_date
    ]

    if not matches:
        return None

    best = max(matches, key=lambda r: r.get("score", 0))
    return best


def apply_edo_8h_daily_context_rule(candles, interval, setups):
    """
    4H:
      handled elsewhere and remains STRONG-TREND ONLY.

    8H:
      normal strong-trend continuation is allowed;
      OR the second separated S/R reaction exception may allow the normal
      2+ / opposite-colour setup before the trend has become strong.

    Daily:
      normal local-trend retracement is allowed;
      OR the same second separated S/R reaction exception may allow the
      normal 2+ / opposite-colour setup while a new trend is developing.

    No forming candle can qualify.
    """
    if interval not in ("8h", "1day"):
        return setups

    out = []
    for p in setups:
        if p.get("name") != "TREND PULLBACK SETUP":
            out.append(p)
            continue

        # Normal local price-structure rule already agrees.
        if p.get("normal_local_trend_ok"):
            out.append(p)
            continue

        reaction = sr_second_reaction_exception(candles, p)
        if reaction:
            q = dict(p)
            q["sr_second_reaction_exception"] = True
            q["sr_exception_level"] = reaction.get("level")
            q["sr_exception_touches"] = reaction.get("touches", 2)
            q["context"] = "second_sr_reaction"
            # This is deliberately an early/developing-trend setup, not a
            # claim that the market is already strongly trending.
            q["trend"] = "developing"
            out.append(q)

    return out


def describe_setup(p):
    bullish = p["direction"] == "bullish"
    icon = "🟢" if bullish else "🔴"
    css = "buy" if bullish else "sell"
    direction_word = "Bullish" if bullish else "Bearish"

    if p["name"] == "S/R GAP RETEST SETUP":
        weak_text = " Weakness was also detected in the retest candles." if p["weak_retest"] else ""
        detail = (
            f"{direction_word} S/R Gap-Retest confirmation on {p['confirmation_date']}. "
            f"Price first established a wick-defined support/resistance zone, moved clearly away, "
            f"then returned to RETEST the same zone after {p['separation']} candles. "
            f"The second touch retraced {p.get('retest_depth_pct', 0):.0f}% back toward the original S/R level "
            f"(minimum 50% required). The opposite-colour confirmation candle fully closed away from the zone and "
            f"{p['penetration']:.0f}% through the previous candle BODY (minimum 50% required)."
        )

        level_text = (
            f"Established retest zone: {p['level']:.5f} • "
            f"Earlier wick level: {p['old_level']:.5f} on {p['old_date']} • "
            f"Second {p['level_source']} test: {p['retest_price']:.5f} on {p['retest_date']} • "
            f"Confirmation close: {p['confirmation_close']:.5f}"
        )

    elif p["name"] in (
        "SUPPORT RETEST / BOUNCE WATCH",
        "RESISTANCE RETEST / REJECTION WATCH"
    ):
        zone_word = "support" if p["direction"] == "bullish" else "resistance"
        action_word = "Bullish watch" if bullish else "Bearish watch"
        trend_word = str(p.get("trend", "mixed")).upper()

        detail = (
            f"{action_word}: the newest fully CLOSED candle has RETESTED an established "
            f"{zone_word} zone and shown a rejection/bounce away from it. "
            f"The zone has at least {p.get('touches', 2)} historical swing touches, and price "
            f"previously moved clearly away before returning. Current local structure is "
            f"{trend_word}. This alert is allowed in bullish, bearish and range-bound markets."
        )

        level_text = (
            f"{zone_word.title()} zone: {p['level']:.5f} • "
            f"Latest close: {p['confirmation_close']:.5f}"
        )

    elif p["name"] == "TREND PULLBACK SETUP":
        run_dates = ", ".join(p.get("run_dates", []))
        htf_text = ""
        if p.get("higher_tf_filter"):
            htf = p.get("higher_tf_states", {})
            htf_text = (
                f" Strong 4H filter passed: "
                f"8H {htf.get('8H','')}, 12H {htf.get('12H','')}, 1D {htf.get('1D','')}."
            )

        detail = (
            f"{direction_word} trend-pullback confirmation on {p['confirmation_date']}. "
            f"{p['run_count']} {p['run_colour']} CLOSED candles pulled against the larger "
            f"{p['trend']} price structure, then the opposite-colour confirmation candle "
            f"fully CLOSED. No minimum body-penetration percentage is required. "
            f"Pullback candle times: {run_dates or 'n/a'}."
            f"{htf_text}"
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
        "confirmation_date": p.get("confirmation_date", ""),
        "confirmed": True,
        "score": p.get("score", 0),
    }



def median_value(values):
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return 0.0
    n = len(clean)
    mid = n // 2
    if n % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


def detect_weekly_spike(closed_weekly):
    """
    Edo Weekly Spike warning — TREND FILTERED.

    Warning only, never a BUY/SELL signal.

    The newest fully CLOSED Weekly candle can trigger only when there was a
    clear price-action trend BEFORE the spike candle:

      Prior Weekly trend BULLISH -> only an unusual UPPER wick can alert.
      Prior Weekly trend BEARISH -> only an unusual LOWER wick can alert.
      Prior Weekly trend MIXED   -> NO Weekly Spike alert.

    Trend is determined by EdoSignal's existing naked price-structure logic
    (recent close progress + swing highs/lows). No moving averages, RSI, etc.

    The wick itself must also be clearly out of ordinary:
      - at least 2.2x the recent median same-side wick
      - at least 45% of the recent median weekly range
      - at least 1.2x the current candle body
    """
    if not closed_weekly or len(closed_weekly) < 14:
        return None

    spike_index = len(closed_weekly) - 1

    # IMPORTANT: this looks ONLY at candles BEFORE the spike candle.
    prior_trend = local_structure_trend(closed_weekly, spike_index)

    # Edo only wants weekly spike alerts after a clear bullish/bearish trend.
    if prior_trend not in ("bullish", "bearish"):
        return None

    current = closed_weekly[-1]
    history = closed_weekly[-13:-1]

    o = float(current["open"])
    h = float(current["high"])
    l = float(current["low"])
    c = float(current["close"])

    body = abs(c - o)
    current_range = max(h - l, 1e-12)
    upper = max(0.0, h - max(o, c))
    lower = max(0.0, min(o, c) - l)

    hist_upper = []
    hist_lower = []
    hist_ranges = []

    for x in history:
        xo = float(x["open"])
        xh = float(x["high"])
        xl = float(x["low"])
        xc = float(x["close"])
        hist_upper.append(max(0.0, xh - max(xo, xc)))
        hist_lower.append(max(0.0, min(xo, xc) - xl))
        hist_ranges.append(max(0.0, xh - xl))

    med_upper = max(median_value(hist_upper), 1e-12)
    med_lower = max(median_value(hist_lower), 1e-12)
    med_range = max(median_value(hist_ranges), 1e-12)

    body_floor = max(body, current_range * 0.06, 1e-12)

    upper_ratio = upper / med_upper
    lower_ratio = lower / med_lower

    upper_hit = (
        upper_ratio >= 2.2
        and upper >= med_range * 0.45
        and upper >= body_floor * 1.2
    )
    lower_hit = (
        lower_ratio >= 2.2
        and lower >= med_range * 0.45
        and lower >= body_floor * 1.2
    )

    # Directional reversal filter:
    # bullish run -> rejection above
    # bearish run -> rejection below
    if prior_trend == "bullish":
        if not upper_hit:
            return None
        side = "upper"
        label = "UPPER"
        ratio = upper_ratio
        meaning = (
            "Weekly trend was BULLISH before this candle. "
            "Unusually long upper wick may show rejection at the top / possible bearish reversal area."
        )
    else:
        if not lower_hit:
            return None
        side = "lower"
        label = "LOWER"
        ratio = lower_ratio
        meaning = (
            "Weekly trend was BEARISH before this candle. "
            "Unusually long lower wick may show rejection at the bottom / possible bullish reversal area."
        )

    date = current.get("datetime", "")
    message = (
        f"{label} wick spike on the newest fully CLOSED Weekly candle ({date}). "
        f"Prior Weekly trend: {prior_trend.upper()}. "
        f"Wick is about {ratio:.1f}x its recent normal size. {meaning} "
        f"Warning only — wait for your normal candle confirmation before trading."
    )

    return {
        "side": side,
        "label": label,
        "ratio": ratio,
        "date": date,
        "prior_trend": prior_trend,
        "message": message,
        "upper_wick": upper,
        "lower_wick": lower,
        "body": body,
        "range": current_range,
    }


def notify_weekly_spike(symbol, grp, spike):
    """Send one Pushover confirmation per detected fully closed weekly spike."""
    if not spike:
        return

    candle_date = spike.get("date", "")
    side = spike.get("side", "")
    if not candle_date or not side:
        return

    try:
        with db_conn() as c:
            c.execute(
                """
                INSERT INTO weekly_spike_notifications(
                    grp, symbol, candle_date, side, wick_ratio, sent_at
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    grp,
                    symbol,
                    candle_date,
                    side,
                    float(spike.get("ratio", 0.0)),
                    datetime.utcnow().isoformat()
                )
            )
            c.commit()
    except sqlite3.IntegrityError:
        return

    send_push(
        f"🟣 {symbol} [{grp}] — WEEKLY SPIKE",
        spike["message"]
    )


def collect_weekly_spike(symbol, grp="FOREX"):
    candles, error = get_ohlc(symbol, "1week", outputsize=80, grp=grp)
    if error:
        return None, None, error

    closed = fully_closed_candles(candles, "1week")
    if len(closed) < 14:
        return None, None, "Not enough fully closed Weekly candle history."

    latest_closed_date = closed[-1].get("datetime", "")
    spike = detect_weekly_spike(closed)
    return spike, latest_closed_date, None


def weekly_spike_monitor():
    """
    Low-credit Weekly Spike monitor.

    It checks one saved market per minute. This gives regular coverage without
    competing heavily with the normal price/trend/pattern monitors.
    """
    time.sleep(210)
    index = 0

    while True:
        try:
            if manual_api_priority_active():
                time.sleep(15)
                continue

            with db_conn() as c:
                rows = c.execute(
                    "SELECT symbol, grp FROM favorites "
                    "WHERE grp IN ('FOREX','CRYPTO','CFD') ORDER BY grp,symbol"
                ).fetchall()

            if rows:
                if index >= len(rows):
                    index = 0

                row = rows[index]
                index = (index + 1) % len(rows)

                symbol = row["symbol"]
                grp = row["grp"]

                spike, latest_closed_date, error = collect_weekly_spike(symbol, grp)

                if error:
                    print("weekly spike monitor error", symbol, error)
                elif latest_closed_date:
                    with db_conn() as c:
                        state = c.execute(
                            """
                            SELECT last_closed_date
                            FROM weekly_spike_state
                            WHERE grp=? AND symbol=?
                            """,
                            (grp, symbol)
                        ).fetchone()

                        previous = state["last_closed_date"] if state else ""

                        if state is None:
                            c.execute(
                                """
                                INSERT INTO weekly_spike_state(
                                    grp, symbol, last_closed_date, updated
                                ) VALUES(?,?,?,?)
                                """,
                                (
                                    grp,
                                    symbol,
                                    latest_closed_date,
                                    datetime.utcnow().isoformat()
                                )
                            )
                            c.commit()

                            # On first installation, alert once if the latest
                            # closed Weekly candle itself is already a spike.
                            if spike:
                                notify_weekly_spike(symbol, grp, spike)

                        elif latest_closed_date != previous:
                            c.execute(
                                """
                                UPDATE weekly_spike_state
                                SET last_closed_date=?, updated=?
                                WHERE grp=? AND symbol=?
                                """,
                                (
                                    latest_closed_date,
                                    datetime.utcnow().isoformat(),
                                    grp,
                                    symbol
                                )
                            )
                            c.commit()

                            if spike:
                                notify_weekly_spike(symbol, grp, spike)

        except Exception as e:
            print("weekly spike monitor error", e)

        time.sleep(60)




def strong_higher_timeframe_trend(symbol, grp="FOREX", interval="4h"):
    """
    Edo strong-trend filter using CLOSED higher-timeframe candles only.

    4H continuation setup:
      require 8H + 12H + 1D all aligned with the setup direction.

    8H continuation setup:
      no higher-timeframe filter is used; this branch is retained only for
      compatibility and is not called by the active 8H signal flow.

    Daily normal Trend Pullback is allowed without a higher-timeframe filter.
    Weekly normal Trend Pullback remains disabled.
    No indicators are used; Bullish = close > open, Bearish = close < open.
    """
    if interval == "4h":
        checks = [("8H", "8h"), ("12H", "12h"), ("1D", "1day")]
    elif interval == "8h":
        checks = [("12H", "12h"), ("1D", "1day"), ("1W", "1week")]
    else:
        return None, {}, None

    states = {}

    for label, tf_interval in checks:
        if tf_interval == "12h":
            candles, err = get_candles(symbol, "12h", outputsize=6, grp=grp)
            if err:
                return None, states, err
            closed = last_closed_candle(candles, "12h")
        else:
            candles, err = get_ohlc(symbol, tf_interval, outputsize=60, grp=grp)
            if err:
                return None, states, err
            closed_list = fully_closed_candles(candles, tf_interval)
            closed = closed_list[-1] if closed_list else None

        if closed is None:
            return None, states, f"Not enough completed {label} candle data."

        states[label] = analyse_candle(closed)

    labels = [label for label, _ in checks]

    if all(states[x] == "Bullish" for x in labels):
        return "bullish", states, None
    if all(states[x] == "Bearish" for x in labels):
        return "bearish", states, None

    return None, states, None


def apply_strong_trend_filter(symbol, grp, interval, setups):
    """
    4H Trend Pullback signals are continuation-only signals.

    A bullish 4H setup is allowed only when all required CLOSED higher
    timeframes are Bullish. A bearish 4H setup is allowed only when they
    are all Bearish.

    8H Trend Pullback is intentionally NOT filtered by higher-timeframe trend.
    Daily Trend Pullback is intentionally NOT filtered by higher-timeframe trend.
    Weekly normal Trend Pullback remains disabled elsewhere.
    """
    if interval != "4h":
        return setups, None, None

    higher_direction, states, error = strong_higher_timeframe_trend(
        symbol, grp, interval
    )
    if error:
        return [], states, error

    if higher_direction is None:
        if interval == "8h":
            exception_setups = []
            for p in setups:
                if p.get("name") != "TREND PULLBACK SETUP":
                    exception_setups.append(p)
                elif p.get("sr_second_reaction_exception"):
                    q = dict(p)
                    q["higher_tf_filter"] = False
                    q["higher_tf_states"] = dict(states)
                    q["signal_context"] = "8H second S/R reaction with gap — developing trend"
                    exception_setups.append(q)
            return exception_setups, states, None
        return [], states, None

    filtered = []
    for p in setups:
        if p.get("name") != "TREND PULLBACK SETUP":
            # Keep non-trading WATCH alerts separate.
            filtered.append(p)
            continue

        if p.get("direction") == higher_direction:
            p = dict(p)
            p["higher_tf_filter"] = True
            p["higher_tf_direction"] = higher_direction
            p["higher_tf_states"] = dict(states)
            filtered.append(p)
        elif interval == "8h" and p.get("sr_second_reaction_exception"):
            # Early/developing-trend exception: a genuine second separated
            # S/R reaction may qualify even before higher TFs fully align.
            p = dict(p)
            p["higher_tf_filter"] = False
            p["higher_tf_states"] = dict(states)
            p["signal_context"] = "8H second S/R reaction with gap — developing trend"
            filtered.append(p)

    return filtered, states, None


def confirmation_room_filter(candles, setup, interval):
    """
    Edo's 'do not chase the confirmation candle into S/R' filter.

    Applied ONLY to 4H and 8H Trend Pullback trading signals.

    The original setup still requires:
      2+ same-colour retracement candles
      opposite-colour CLOSED confirmation
      opposite-colour fully CLOSED confirmation candle

    This extra filter REJECTS the trade signal when the confirmation candle
    has already travelled too far and closes at/very near the next important
    historical swing level in the continuation direction.

    It also flags an unusually oversized confirmation body as an extra reason
    when that candle is already pressing into the historical level.
    """
    if interval not in ("4h", "8h"):
        return True, None

    if setup.get("name") != "TREND PULLBACK SETUP":
        return True, None

    i = None
    confirmation_date = setup.get("confirmation_date", "")
    for idx, c in enumerate(candles):
        if c.get("datetime", "") == confirmation_date:
            i = idx
            break

    if i is None or i < 10:
        return True, None

    run_count = int(setup.get("run_count", 2))
    run_start = max(0, i - run_count)
    direction = setup.get("direction")
    confirm = candles[i]
    confirm_close = float(confirm["close"])
    confirm_body = abs(float(confirm["close"]) - float(confirm["open"]))

    # Use only history BEFORE the retracement started to find the old level.
    history = candles[max(0, run_start - 70):run_start]
    if len(history) < 8:
        return True, None

    recent_before = candles[max(0, run_start - 24):run_start]
    ranges = [
        max(0.0, float(c["high"]) - float(c["low"]))
        for c in recent_before
    ]
    bodies = [
        abs(float(c["close"]) - float(c["open"]))
        for c in recent_before
    ]

    positive_ranges = sorted(x for x in ranges if x > 0)
    positive_bodies = sorted(x for x in bodies if x > 0)

    if not positive_ranges:
        return True, None

    med_range = positive_ranges[len(positive_ranges)//2]
    med_body = (
        positive_bodies[len(positive_bodies)//2]
        if positive_bodies else med_range * 0.5
    )

    # The 'near level' zone is deliberately fairly tight: we only reject
    # when the confirmation is effectively arriving at the old turning point.
    near_tol = med_range * 0.35

    if direction == "bullish":
        pts = swing_points(history, "high")
        if not pts:
            return True, None

        levels = sorted(float(v) for _, v in pts if float(v) > float(candles[run_start]["low"]))
        if not levels:
            return True, None

        # Nearest historical resistance at/above the confirmation close,
        # otherwise the closest resistance just crossed by the candle.
        above = [v for v in levels if v >= confirm_close]
        level = min(above) if above else max(levels)

        distance = level - confirm_close
        at_or_near = abs(distance) <= near_tol or confirm_close >= level
        oversized = med_body > 0 and confirm_body >= med_body * 1.8

        if at_or_near:
            reason = (
                f"REJECTED: bullish confirmation moved too far and closed at/near "
                f"historical resistance {level:.5f}. "
                f"Confirmation close {confirm_close:.5f}; "
                f"only {abs(distance):.5f} room remained."
            )
            if oversized:
                reason += " Confirmation body was also unusually large."
            return False, reason

    elif direction == "bearish":
        pts = swing_points(history, "low")
        if not pts:
            return True, None

        levels = sorted(float(v) for _, v in pts if float(v) < float(candles[run_start]["high"]))
        if not levels:
            return True, None

        below = [v for v in levels if v <= confirm_close]
        level = max(below) if below else min(levels)

        distance = confirm_close - level
        at_or_near = abs(distance) <= near_tol or confirm_close <= level
        oversized = med_body > 0 and confirm_body >= med_body * 1.8

        if at_or_near:
            reason = (
                f"REJECTED: bearish confirmation moved too far and closed at/near "
                f"historical support {level:.5f}. "
                f"Confirmation close {confirm_close:.5f}; "
                f"only {abs(distance):.5f} room remained."
            )
            if oversized:
                reason += " Confirmation body was also unusually large."
            return False, reason

    return True, None


def apply_confirmation_room_filter(candles, interval, setups):
    """
    Remove overextended 4H/8H trading signals from notification eligibility,
    while keeping the rejection reason so the manual Signal page can explain
    exactly what Edo does not like about the setup.
    """
    accepted = []
    rejected = []

    for p in setups:
        ok, reason = confirmation_room_filter(candles, p, interval)
        if ok:
            accepted.append(p)
        else:
            rp = dict(p)
            rp["rejected"] = True
            rp["rejection_reason"] = reason
            rejected.append(rp)

    return accepted, rejected


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

    # Use timestamp-based closure checking instead of blindly dropping
    # only the last returned candle.
    closed_candles = fully_closed_candles(candles, interval)

    if len(closed_candles) < 40:
        return None, f"Not enough fully closed {interval} candle history returned."

    found = []

    # A) NORMAL Edo Trend Pullback — 4H, 8H and Daily.
    #    Minimum 2 same-colour fully CLOSED pullback candles,
    #    then an opposite-colour fully CLOSED confirmation.
    #    NO 50% rule applies to this signal.
    if interval in NORMAL_PULLBACK_INTERVALS:
        for conf in recent_confirmations(closed_candles, lookback=7):
            pullback = detect_trend_pullback(
                closed_candles,
                conf,
                allow_sr_exception=False
            )
            if pullback:
                found.append(pullback)

    # B) Edo S/R GAP-AND-RETEST — 8H, Daily and Weekly.
    #    Established high/low on the left -> clear move away / separation ->
    #    later retest of the same zone -> opposite-colour CLOSED confirmation.
    #    The 50% previous-candle BODY rule applies ONLY to this signal.
    if interval in SR_GAP_RETEST_INTERVALS:
        for sr_conf in recent_sr_confirmations(closed_candles, lookback=9):
            sr_setup = detect_bounce_retest(closed_candles, sr_conf)
            if sr_setup:
                found.append(sr_setup)

    # Avoid duplicate descriptions of the same setup/direction/confirmation.
    unique = {}
    for p in found:
        key = (p["name"], p["direction"], p["confirmation_date"])
        if key not in unique or p.get("score", 0) > unique[key].get("score", 0):
            unique[key] = p

    found = list(unique.values())

    # Hard whitelist: ONLY Edo's requested signals.
    if interval == "4h":
        found = [p for p in found if p.get("name") == "TREND PULLBACK SETUP"]
    elif interval == "8h":
        found = [
            p for p in found
            if p.get("name") in ("TREND PULLBACK SETUP", "S/R GAP RETEST SETUP")
        ]
    elif interval == "1day":
        found = [
            p for p in found
            if p.get("name") in ("TREND PULLBACK SETUP", "S/R GAP RETEST SETUP")
        ]
    elif interval == "1week":
        found = [p for p in found if p.get("name") == "S/R GAP RETEST SETUP"]
    else:
        found = []

    # Edo rule: ONLY the 4H normal Trend Pullback requires strong higher-timeframe
    # trend alignment. The 8H and Daily normal Trend Pullbacks are pure
    # candle-sequence logic: minimum 2 same-colour pullback candles, then the
    # first opposite-colour fully CLOSED confirmation candle. No higher-timeframe
    # trend filter on 8H or Daily.
    higher_tf_states = None
    if interval == "4h":
        normal_found = [p for p in found if p.get("name") == "TREND PULLBACK SETUP"]
        other_found = [p for p in found if p.get("name") != "TREND PULLBACK SETUP"]

        normal_found, higher_tf_states, htf_error = apply_strong_trend_filter(
            symbol, grp, interval, normal_found
        )
        if htf_error:
            return None, htf_error

        found = normal_found + other_found

    # Edo filter: do not chase a valid 4H/8H confirmation candle if it has
    # already shot into the next important historical S/R level.
    found, rejected_room_setups = apply_confirmation_room_filter(
        closed_candles, interval, found
    )

    found.sort(
        key=lambda p: (
            p.get("confirmation_date", ""),
            p.get("score", 0)
        ),
        reverse=True
    )

    latest_closed_date = closed_candles[-1].get("datetime", "")
    current_patterns = [
        p for p in found
        if p.get("confirmation_date", "") == latest_closed_date
    ]

    bullish = [p for p in current_patterns if p["direction"] == "bullish"]
    bearish = [p for p in current_patterns if p["direction"] == "bearish"]

    if bullish and not bearish:
        signal = "NEW BULLISH SETUP TRIGGERED"
        icon, css = "🟢", "buy"
        summary = "The newest fully CLOSED candle completed a valid bullish Edo pattern."
    elif bearish and not bullish:
        signal = "NEW BEARISH SETUP TRIGGERED"
        icon, css = "🔴", "sell"
        summary = "The newest fully CLOSED candle completed a valid bearish Edo pattern."
    elif bullish and bearish:
        signal = "NEW MIXED SETUPS"
        icon, css = "🟡", "wait"
        summary = (
            "The newest fully CLOSED candle produced conflicting valid setup evidence. "
            "Review the naked chart before trading."
        )
    else:
        signal = "NO NEW SETUP ON LATEST CLOSED CANDLE"
        icon, css = "⚪", "neutral"
        if found:
            summary = (
                "No pattern triggered on the newest fully closed candle. "
                "The most recent older valid trigger is shown below for reference."
            )
        else:
            if rejected_room_setups:
                summary = rejected_room_setups[0].get(
                    "rejection_reason",
                    "Pattern detected, but rejected because the confirmation candle moved too far into historical support/resistance."
                )
            elif interval == "4h" and higher_tf_states:
                state_text = " | ".join(f"{k} {v}" for k, v in higher_tf_states.items())
                rule_text = "8H + 12H + 1D"
                summary = (
                    f"No {interval.upper()} setup passed the strong higher-timeframe filter. "
                    f"For this alert, {rule_text} must all agree with the setup direction. "
                    f"Current higher-timeframe state: {state_text}."
                )
            else:
                summary = "No recent setup matches your candle-close pattern rules on this timeframe."

    # Weekly Spike disabled: Edo wants ONLY his requested trading signals.
    weekly_spike = None

    data = {
        "price": closed_candles[-1]["close"],
        "latest_closed_date": latest_closed_date,
        "market_source": closed_candles[-1].get("_source_label", ""),
        "weekly_spike": weekly_spike,
        "signal": signal,
        "signal_icon": icon,
        "signal_css": css,
        "summary": summary,
        "patterns": [describe_setup(p) for p in found[:4]],
        "rejected_setups": [
            p.get("rejection_reason", "")
            for p in rejected_room_setups[:3]
            if p.get("rejection_reason")
        ],
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    PATTERN_SIGNAL_CACHE[cache_key] = {"saved_at": now, "data": data}
    return data, None




def pattern_signal_family(p):
    """
    Group signals that mean the same thing to Edo so only one phone push
    of that type is sent for the same pair/direction on the same Perth day.

    Strategy families:
      TREND PULLBACK = normal minimum-2-candle setup, NO 50% rule.
      S/R GAP RETEST = separated historical support/resistance retest,
                       WITH the 50% confirmation rule.
    """
    name = str(p.get("name", "")).upper()

    if "S/R GAP RETEST" in name:
        return "SR_GAP_RETEST"
    if "TREND PULLBACK" in name:
        return "TREND_PULLBACK"
    if "RANGE" in name and "REVERSAL" in name:
        return "RANGE_REVERSAL"

    return name.replace(" ", "_") or "OTHER"


def pattern_push_priority(p):
    """
    If two equivalent signals are found on the same scan, prefer the stronger
    confirmation so Edo receives one useful notification instead of two.
    """
    name = str(p.get("name", "")).upper()

    if "S/R GAP RETEST" in name:
        return 25
    if "TREND PULLBACK" in name:
        return 20
    if "RANGE" in name and "REVERSAL" in name:
        return 20
    return 10


def perth_day_string():
    try:
        return datetime.now(ZoneInfo("Australia/Perth")).strftime("%Y-%m-%d")
    except Exception:
        # Perth is UTC+8 year-round; fallback only if timezone data is unavailable.
        return datetime.utcnow().strftime("%Y-%m-%d")


def reserve_daily_pattern_push(grp, symbol, p):
    """
    Edo rule:
      ONE automatic pattern/watch Pushover per market pair per Perth day.

    Example:
      if GBP/USD already sent any Trend Pullback or S/R Watch today,
      no second automatic pattern/watch Pushover for GBP/USD is sent today.

    Signal type and direction do NOT matter for the daily limit.
    The limit is persisted in SQLite, so restart/redeploy does not reset it.
    """
    local_day = perth_day_string()

    try:
        with db_conn() as c:
            c.execute(
                """
                INSERT INTO pair_daily_pushes(
                    grp, symbol, local_day, signal_name, direction, sent_at
                )
                VALUES(?,?,?,?,?,?)
                """,
                (
                    grp,
                    symbol,
                    local_day,
                    p.get("name", ""),
                    str(p.get("direction", "")),
                    datetime.utcnow().isoformat(),
                )
            )
            c.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def forex_weekend_closed(now_utc=None):
    """
    True while the normal spot-Forex weekend session is closed.

    Forex closes at about 5pm New York Friday and reopens about 5pm
    New York Sunday. Using America/New_York automatically follows DST.
    Crypto is never affected by this helper.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    ny = now_utc.astimezone(ZoneInfo("America/New_York"))
    weekday = ny.weekday()  # Monday=0 ... Sunday=6
    minutes = ny.hour * 60 + ny.minute
    close_minutes = 17 * 60

    if weekday == 4 and minutes >= close_minutes:  # Friday after 17:00 NY
        return True
    if weekday == 5:  # Saturday
        return True
    if weekday == 6 and minutes < close_minutes:  # Sunday before 17:00 NY
        return True
    return False


def pattern_candle_close_utc(candle_start, interval):
    """Return the UTC close time for a candle-start timestamp when possible."""
    try:
        start = parse_candle_utc(candle_start)
        if start is None:
            return None
        seconds = interval_seconds(interval)
        if not seconds:
            return None
        return start + timedelta(seconds=seconds)
    except Exception:
        return None


def _save_pattern_monitor_baseline(grp, symbol, interval, latest_closed_date):
    """Advance the persistent monitor state without generating a phone push."""
    with db_conn() as c:
        c.execute(
            """
            INSERT INTO pattern_monitor_state(
                grp, symbol, interval, last_closed_date, updated
            ) VALUES(?,?,?,?,?)
            ON CONFLICT(grp, symbol, interval) DO UPDATE SET
                last_closed_date=excluded.last_closed_date,
                updated=excluded.updated
            """,
            (
                grp,
                symbol,
                interval,
                latest_closed_date,
                datetime.utcnow().isoformat(),
            )
        )
        c.commit()


def notify_new_pattern_setups(symbol, interval, patterns, latest_closed_date, grp="FOREX"):
    """
    Notify only for a genuinely NEW fully closed candle.

    Safety rules:
      * A Railway restart/redeploy must never replay a candle that had already
        closed before this Python process started.
      * FOREX pattern pushes are not sent during the weekend market closure.
        The newest closed candle is baselined instead, so it cannot replay on
        Saturday/Sunday or when the market reopens.
      * CRYPTO remains 24/7.
    """
    if not latest_closed_date:
        return

    # Never turn an old candle into a "new" push just because Railway restarted.
    close_utc = pattern_candle_close_utc(latest_closed_date, interval)
    if close_utc is not None and close_utc <= PROCESS_STARTED_UTC:
        _save_pattern_monitor_baseline(
            grp, symbol, interval, latest_closed_date
        )
        print(
            "pattern baseline after restart",
            grp, symbol, interval, latest_closed_date
        )
        return

    # Edo rule: no delayed Forex pattern alerts over the closed weekend.
    # Advance the state silently so Friday candles cannot replay on Saturday,
    # Sunday, or after the Sunday reopen.
    if str(grp).upper() == "FOREX" and forex_weekend_closed():
        _save_pattern_monitor_baseline(
            grp, symbol, interval, latest_closed_date
        )
        print(
            "forex weekend pattern push suppressed/baselined",
            symbol, interval, latest_closed_date
        )
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

    # First keep only the strongest signal in each equivalent signal family
    # for this newest closed candle. This prevents e.g. SUPPORT BOUNCE WATCH
    # and BOUNCE / RETEST SETUP both firing for the same USD/CHF reaction.
    newest_patterns = [
        p for p in patterns
        if p.get("confirmation_date", "") == latest_closed_date
    ]

    best_by_family = {}
    for p in newest_patterns:
        family_key = (pattern_signal_family(p), str(p.get("direction", "")).lower())
        current = best_by_family.get(family_key)
        if current is None or pattern_push_priority(p) > pattern_push_priority(current):
            best_by_family[family_key] = p

    for p in best_by_family.values():
        confirmation_date = p.get("confirmation_date", "")
        if not confirmation_date:
            continue

        # Edo rule: once this pair has produced ANY automatic pattern/watch
        # Pushover today (Perth day), do not send another for this pair today.
        if not reserve_daily_pattern_push(grp, symbol, p):
            print(
                "daily pair push suppressed",
                grp, symbol, p.get("name"), p.get("direction"), perth_day_string()
            )
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

        if interval == "4h" and p.get("higher_tf_filter"):
            htf = p.get("higher_tf_states", {})
            trend_line = (
                f"Strong higher-timeframe trend: "
                f"8H {htf.get('8H','')} | 12H {htf.get('12H','')} | 1D {htf.get('1D','')}. "
            )
        else:
            trend_line = ""

        if p.get("name") == "S/R GAP RETEST SETUP":
            zone_word = "SUPPORT" if p["direction"] == "bullish" else "RESISTANCE"
            push_body = (
                f"{p['name']} confirmed on the NEWEST CLOSED {tf_label} candle "
                f"({confirmation_date}). "
                f"Established {zone_word} on the left, price moved clearly away, "
                f"then returned after separation to retest the same zone. "
                f"Second-touch depth: {p.get('retest_depth_pct', 0):.0f}% back toward the original level. "
                f"The opposite-colour confirmation closed {p.get('penetration', 0):.0f}% "
                f"through the previous candle body. "
                f"{direction_word} possibility. Review the chart before trading."
            )
        else:
            push_body = (
                f"{p['name']} confirmed on the NEWEST CLOSED {tf_label} candle "
                f"({confirmation_date}). "
                f"{trend_line}"
                f"{direction_word} possibility. Review the chart before trading."
            )

        send_push(
            f"{icon} {symbol} [{grp}] — {p['name']}",
            push_body
        )


def collect_closed_pattern_setups(symbol, interval, grp="FOREX"):
    """
    Collect Edo's trading setups from fully closed candles only: normal Trend Pullback (4H/8H/Daily, no 50%) and S/R Gap-Retest (8H/Daily/Weekly, 50% rules).

    Returns:
      setups, latest_closed_date, error
    """
    candles, error = get_ohlc(symbol, interval, outputsize=140, grp=grp)
    if error:
        return None, None, error
    if not candles or len(candles) < 41:
        return None, None, f"Not enough fully closed {interval} candle history returned."

    # Use the same timestamp-based closure logic as the manual Signal page.
    closed_candles = fully_closed_candles(candles, interval)
    if len(closed_candles) < 40:
        return None, None, f"Not enough fully closed {interval} candle history returned."

    latest_closed_date = closed_candles[-1].get("datetime", "")
    found = []

    # A) NORMAL Trend Pullback — 4H, 8H and Daily, NO 50% rule.
    if interval in NORMAL_PULLBACK_INTERVALS:
        for conf in recent_confirmations(closed_candles, lookback=7):
            pullback = detect_trend_pullback(
                closed_candles,
                conf,
                allow_sr_exception=False
            )
            if pullback:
                found.append(pullback)

    # B) S/R GAP-AND-RETEST — 8H, Daily and Weekly, WITH 50% rule.
    if interval in SR_GAP_RETEST_INTERVALS:
        for sr_conf in recent_sr_confirmations(closed_candles, lookback=9):
            sr_setup = detect_bounce_retest(closed_candles, sr_conf)
            if sr_setup:
                found.append(sr_setup)

    unique = {}
    for p in found:
        key = (p["name"], p["direction"], p["confirmation_date"])
        if key not in unique or p.get("score", 0) > unique[key].get("score", 0):
            unique[key] = p

    setups = list(unique.values())

    # Hard whitelist for automatic phone notifications.
    if interval == "4h":
        setups = [p for p in setups if p.get("name") == "TREND PULLBACK SETUP"]
    elif interval == "8h":
        setups = [
            p for p in setups
            if p.get("name") in ("TREND PULLBACK SETUP", "S/R GAP RETEST SETUP")
        ]
    elif interval == "1day":
        setups = [
            p for p in setups
            if p.get("name") in ("TREND PULLBACK SETUP", "S/R GAP RETEST SETUP")
        ]
    elif interval == "1week":
        setups = [p for p in setups if p.get("name") == "S/R GAP RETEST SETUP"]
    else:
        setups = []

    # Edo rule: ONLY 4H normal Trend Pullback uses the strong higher-timeframe
    # trend filter. 8H and Daily normal Trend Pullbacks must NOT be blocked by
    # trend alignment. The separate S/R Gap-Retest setup is also independent.

    if interval == "4h":
        normal_setups = [p for p in setups if p.get("name") == "TREND PULLBACK SETUP"]
        other_setups = [p for p in setups if p.get("name") != "TREND PULLBACK SETUP"]

        normal_setups, higher_tf_states, htf_error = apply_strong_trend_filter(
            symbol, grp, interval, normal_setups
        )
        if htf_error:
            return None, latest_closed_date, htf_error

        setups = normal_setups + other_setups

    # Reject overextended 4H/8H confirmations that have already arrived at
    # the next important historical S/R level. Rejected setups do NOT Push.
    setups, rejected_room_setups = apply_confirmation_room_filter(
        closed_candles, interval, setups
    )
    for rp in rejected_room_setups:
        print(
            "pattern rejected - confirmation too far into S/R",
            symbol, interval, rp.get("rejection_reason", "")
        )

    return setups, latest_closed_date, None


def pattern_signal_monitor():
    """
    Grow-55 background pattern scheduler.

    Goal:
      - scan 4H/8H/Daily for Edo's normal Trend Pullback
      - scan 8H/Daily/Weekly for Edo's S/R Gap-Retest
      - NEVER crowd out manual Trend / Signal page requests

    Protection:
      - manual_api_priority_active() always wins
      - only ONE background API job is processed at a time
      - 20-second gap between background jobs
      - each symbol/timeframe is checked only when its due interval expires

    Check cadence per saved market:
      4H     every 15 minutes
      8H     every 20 minutes
      Daily  every 60 minutes
      Weekly every 6 hours

    These are scan frequencies only. Signals still require a NEW fully CLOSED
    candle, so repeated scans cannot create duplicate signals.
    """
    time.sleep(180)

    due_seconds = {
        "4h": 15 * 60,
        "8h": 20 * 60,
        "1day": 60 * 60,
        "1week": 6 * 60 * 60,
    }

    last_checked = {}

    while True:
        try:
            # Manual Trend/Signal page requests have absolute priority.
            if manual_api_priority_active():
                time.sleep(10)
                continue

            with db_conn() as c:
                rows = c.execute(
                    "SELECT symbol, grp FROM favorites "
                    "WHERE grp IN ('FOREX','CRYPTO','CFD') "
                    "ORDER BY grp,symbol"
                ).fetchall()

            now_ts = time.time()
            jobs = []

            for row in rows:
                symbol = row["symbol"]
                grp = row["grp"]

                for tf in PATTERN_TIMEFRAMES:
                    interval = tf["value"]
                    key = (grp, symbol, interval)
                    previous = last_checked.get(key, 0.0)
                    due = due_seconds.get(interval, 30 * 60)

                    if now_ts - previous >= due:
                        # Oldest/most-overdue jobs first.
                        overdue = now_ts - previous - due
                        jobs.append((overdue, symbol, grp, interval, key))

            jobs.sort(reverse=True, key=lambda x: x[0])

            if not jobs:
                time.sleep(20)
                continue

            # Process only ONE job, then yield. This is deliberate so a user
            # opening Trend or Signal is not stuck behind a large background batch.
            _, symbol, grp, interval, key = jobs[0]

            # Check again immediately before consuming an API call.
            if manual_api_priority_active():
                time.sleep(10)
                continue

            setups, latest_closed_date, error = collect_closed_pattern_setups(
                symbol, interval, grp
            )

            # Mark checked even on a normal API/data error so a broken symbol
            # cannot spin rapidly and monopolise background capacity.
            last_checked[key] = time.time()

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

        # Important Grow-55 safety / UI-responsiveness gap.
        time.sleep(20)


def get_daily_candles_for_alignment(symbol, outputsize=1800, grp=None):
    if not TWELVE_KEY:
        return None, "Twelve Data API key is not configured."

    last_error = "Could not download daily alignment data."

    try:
        for resolved_symbol in twelve_symbol_candidates(symbol, grp):
            j = twelve_get_json(
                "https://api.twelvedata.com/time_series",
                {
                    "symbol": resolved_symbol,
                    "interval": "1day",
                    "outputsize": outputsize,
                    "apikey": TWELVE_KEY,
                    "format": "JSON",
                    "timezone": "UTC",
                },
                timeout=20
            )

            if j.get("status") == "error":
                last_error = j.get("message", "Twelve Data returned an error.")
                continue

            values = j.get("values") or []
            rows = []

            for row in reversed(values):
                try:
                    dt = datetime.fromisoformat(row["datetime"])
                    rows.append((dt, float(row["close"])))
                except Exception:
                    pass

            if len(rows) < 300:
                last_error = f"Not enough daily history for {resolved_symbol}."
                continue

            if grp == "CFD":
                print("CFD alignment source", symbol, "->", resolved_symbol)

            return rows, None

        return None, last_error

    except TwelveDataCoolingDown:
        return None, "API cooling down — please try again in a few seconds."
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

        closed = last_closed_candle(candles, interval)
        if closed is None:
            return "", f"Not enough completed {label} candle data."

        signal_states[label] = analyse_candle(closed)

    # Weekly is reference-only. If Weekly data fails, the real trigger
    # still works from 12H + 8H + 4H + 1H.
    weekly_state = ""
    weekly_candles, weekly_err = get_candles(symbol, "1week", outputsize=3, grp=grp)
    if not weekly_err:
        weekly_closed = last_closed_candle(weekly_candles, "1week")
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



def twelve_symbol_candidates(symbol, grp=None):
    """
    Return Twelve Data symbols in preferred order.

    For Edo's US CFD aliases we now TRY the direct index first:
      DJ30 / US30  -> DJI, then DIA fallback
      NAS100/US100 -> NDX, then QQQ fallback
      SP500/US500  -> GSPC, then SPY fallback

    Twelve Data access can vary by plan/instrument, so the ETF fallback keeps
    EdoSignal working if a direct index symbol is unavailable.
    """
    s = symbol.upper().strip().replace(" ", "")

    if grp == "CFD" or s in {
        "DJ30", "US30", "DOW30",
        "NAS100", "NASDAQ100", "US100", "USTEC",
        "SP500", "US500", "S&P500", "SPX500"
    }:
        direct = {
            "DJ30": ["DJI", "DIA"],
            "US30": ["DJI", "DIA"],
            "DOW30": ["DJI", "DIA"],

            "NAS100": ["NDX", "QQQ"],
            "NASDAQ100": ["NDX", "QQQ"],
            "US100": ["NDX", "QQQ"],
            "USTEC": ["NDX", "QQQ"],

            "SP500": ["GSPC", "SPY"],
            "US500": ["GSPC", "SPY"],
            "S&P500": ["GSPC", "SPY"],
            "SPX500": ["GSPC", "SPY"],
        }
        if s in direct:
            return direct[s]

    return [twelve_symbol(symbol, grp)]


def source_label_for(symbol, grp, resolved_symbol):
    s = symbol.upper().strip().replace(" ", "")
    if grp != "CFD":
        return resolved_symbol

    direct_sets = {
        "DJ30": "DJI", "US30": "DJI", "DOW30": "DJI",
        "NAS100": "NDX", "NASDAQ100": "NDX", "US100": "NDX", "USTEC": "NDX",
        "SP500": "GSPC", "US500": "GSPC", "S&P500": "GSPC", "SPX500": "GSPC",
    }
    proxy_sets = {
        "DJ30": "DIA", "US30": "DIA", "DOW30": "DIA",
        "NAS100": "QQQ", "NASDAQ100": "QQQ", "US100": "QQQ", "USTEC": "QQQ",
        "SP500": "SPY", "US500": "SPY", "S&P500": "SPY", "SPX500": "SPY",
    }

    if resolved_symbol == direct_sets.get(s):
        return f"{resolved_symbol} direct index"
    if resolved_symbol == proxy_sets.get(s):
        return f"{resolved_symbol} ETF fallback"
    return resolved_symbol


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
        US500 / SP500 -> direct index first, SPY fallback
        NAS100 / US100 -> direct index first, QQQ fallback
        US30 / DJ30 -> direct index first, DIA fallback
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
        # Fallback mappings used only if the preferred direct index call is unavailable.
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
    Build synthetic 12H candles from 3 consecutive 4H candles.

    Important: Twelve Data can anchor 4H candles at different UTC hours for
    different markets (for example 00/04/08... or 01/05/09...).  Therefore
    we must not assume every market starts its 4H candles at 00:00 UTC.

    This function detects the market's 4H phase from the returned timestamps,
    groups into two 12H blocks per day using that phase, and only keeps blocks
    that contain all three consecutive 4H parts.
    """
    from collections import Counter
    from datetime import timedelta

    parsed = []
    for c in candles_4h or []:
        dt = parse_candle_utc(c.get("datetime", ""))
        if dt is not None:
            parsed.append((dt, c))

    if not parsed:
        return []

    parsed.sort(key=lambda item: item[0])

    # Detect the UTC start-hour phase of the 4H series.
    # Examples: 00/04/08... -> phase 0, 01/05/09... -> phase 1.
    phase_counts = Counter(dt.hour % 4 for dt, _ in parsed)
    phase = phase_counts.most_common(1)[0][0]

    buckets = {}
    for dt, c in parsed:
        shifted = dt - timedelta(hours=phase)
        bucket_hour = 0 if shifted.hour < 12 else 12
        bucket_shifted = shifted.replace(
            hour=bucket_hour, minute=0, second=0, microsecond=0
        )
        bucket_start = bucket_shifted + timedelta(hours=phase)
        buckets.setdefault(bucket_start, []).append((dt, c))

    synthetic = []
    for bucket_start in sorted(buckets):
        group = sorted(buckets[bucket_start], key=lambda item: item[0])

        if len(group) != 3:
            continue

        expected = [
            bucket_start,
            bucket_start + timedelta(hours=4),
            bucket_start + timedelta(hours=8),
        ]
        actual = [dt for dt, _ in group]

        # Keep only a real 3 x 4H sequence; reject gaps/missing candles.
        if actual != expected:
            continue

        parts = [c for _, c in group]
        synthetic.append({
            "datetime": bucket_start.strftime("%Y-%m-%d %H:%M:%S"),
            "open": parts[0]["open"],
            "high": max(x["high"] for x in parts),
            "low": min(x["low"] for x in parts),
            "close": parts[-1]["close"],
            "_parts": 3,
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

        if len(candles_12h) < 1:
            return None, "Not enough 4H candle history to build completed 12H candles."

        return candles_12h[-outputsize:], None

    candles, error = get_ohlc(symbol, interval, outputsize=max(40, outputsize), grp=grp)
    if error:
        return None, error
    return candles[-outputsize:], None


def last_closed_candle(candles, interval):
    """
    Return the newest candle that is ACTUALLY fully closed.

    This uses the candle timestamp + interval duration instead of assuming
    candles[-2] is closed. Twelve Data may return a set where the newest item
    is already closed, so blindly using [-2] can make EdoSignal one candle
    behind the broker chart.
    """
    closed = fully_closed_candles(candles, interval)
    if not closed:
        return None
    return closed[-1]


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


def analyse_closes(candles, interval):
    """
    Analyse ONLY the newest truly fully closed candlestick colour.
    No SMA/EMA/RSI logic is used.
    """
    closed = last_closed_candle(candles, interval)
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

        closed = last_closed_candle(candles, interval)
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
            "closed_time": closed.get("datetime", ""),
        })

    # Existing signal rows stay unchanged.
    for label, interval in signal_intervals:
        candles, error = get_candles(symbol, interval, outputsize=3, grp=grp)

        if error:
            return None, error

        closed = last_closed_candle(candles, interval)
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
            "closed_time": closed.get("datetime", ""),
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



@APP.route('/news-reactions')
def news_reactions_api():
    return jsonify(news_reaction_payload())


@APP.route('/refresh-news', methods=['POST'])
def refresh_news_now():
    """
    Manual Forex Factory calendar refresh for AJAX use.
    The browser stays on the HOME page.
    This does not use Twelve Data credits.
    """
    try:
        ok, error = refresh_economic_news()

        cached_count = 0
        try:
            with db_conn() as c:
                cached_count = c.execute(
                    "SELECT COUNT(*) AS n FROM economic_news"
                ).fetchone()["n"]
        except Exception:
            pass

        if error:
            print("manual Forex Factory refresh:", error)
            return jsonify(
                ok=False,
                error=str(error),
                cached_high_impact_events=cached_count
            ), 200

        return jsonify(
            ok=True,
            cached_high_impact_events=cached_count,
            source=_get_news_feed_status().get("source_url", "")
        ), 200

    except Exception as e:
        print("manual Forex Factory refresh error", e)
        return jsonify(
            ok=False,
            error="News refresh failed temporarily."
        ), 200


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

    # News comes from SQLite cache only, so HOME remains fast.
    news_items = cached_home_news(limit=6)
    news_configured = True
    news_feed_status = _get_news_feed_status()
    news_feed_unavailable = bool(
        not news_items and news_feed_status.get("last_error")
    )

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
        trend_snapshots=trend_snapshots,
        news_items=news_items,
        news_configured=news_configured,
        news_feed_status=news_feed_status,
        news_feed_unavailable=news_feed_unavailable
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
            latest_closed_date='—',
            market_source='',
            weekly_spike=None,
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
        latest_closed_date=data.get('latest_closed_date', ''),
        market_source=data.get('market_source', ''),
        weekly_spike=data.get('weekly_spike'),
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

# Weekly Spike warning is one of Edo's intended signals.
# It is a warning only and uses fully closed Weekly candles.
threading.Thread(
    target=weekly_spike_monitor,
    daemon=True
).start()

threading.Thread(
    target=economic_news_monitor,
    daemon=True
).start()

threading.Thread(
    target=economic_news_warning_monitor,
    daemon=True
).start()

threading.Thread(
    target=economic_news_reaction_monitor,
    daemon=True
).start()


if __name__ == '__main__':
    APP.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', '8080'))
    )
