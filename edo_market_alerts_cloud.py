import os, json, time, sqlite3, threading
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string
import requests

APP = Flask(__name__)
DB = os.environ.get('EDO_DB', 'edo_market_alerts.db')
TWELVE_KEY = os.environ.get('TWELVE_DATA_API_KEY', '')
PUSHOVER_APP_TOKEN = os.environ.get('PUSHOVER_APP_TOKEN', '')
PUSHOVER_USER_KEY = os.environ.get('PUSHOVER_USER_KEY', '')
CHECK_SECONDS = int(os.environ.get('CHECK_SECONDS', '30'))

COLORS = {'FOREX':'#2980ff','CRYPTO':'#9b59ff','CFD':'#ff9f2f'}

HTML = r'''<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Edo Market Alerts</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#07111f;color:#eef6ff;margin:0}.wrap{max-width:760px;margin:auto;padding:18px}.card{background:#0d1b2a;border-radius:18px;padding:16px;margin:12px 0}.row{display:flex;gap:8px;flex-wrap:wrap}input,select,button{font-size:16px;border:0;border-radius:12px;padding:12px}input,select{background:#13263b;color:#fff;flex:1}button{background:#1fd1a5;font-weight:700}.market{display:flex;justify-content:space-between;align-items:center;padding:12px;border-radius:14px;margin:8px 0;background:#12263b}.pill{padding:5px 9px;border-radius:999px;font-size:12px;font-weight:800}.small{color:#8ca7bf;font-size:13px}.danger{background:#ff5e73;color:#fff}.secondary{background:#23394f;color:#fff}h1{font-size:27px;margin-bottom:3px}h2{font-size:18px}.price{font-variant-numeric:tabular-nums;font-weight:700}.status{font-size:12px;font-weight:800}
</style></head><body><div class="wrap"><h1>📈 EDO MARKET ALERTS</h1><div class="small">Forex • Crypto • CFD • Cloud price alarms</div>
<div class="card"><h2>➕ Save market / alert</h2><form method="post" action="/add"><div class="row"><input name="symbol" placeholder="USD/CAD or BTC/USD" required><select name="group"><option>FOREX</option><option>CRYPTO</option><option>CFD</option></select></div><div class="row" style="margin-top:8px"><input name="target" type="number" step="any" placeholder="Target price" required><select name="direction"><option value="ABOVE">ABOVE</option><option value="BELOW">BELOW</option></select><button>ARM</button></div></form></div>
<div class="card"><h2>⭐ My Market Library</h2>{% for m in markets %}<div class="market"><div><span class="pill" style="background:{{ colors[m['grp']] }}22;color:{{ colors[m['grp']] }}">{{m['grp']}}</span> <b>{{m['symbol']}}</b><div class="small">Target {{m['direction']}} {{m['target']}}</div></div><div style="text-align:right"><div class="price">{{m['last_price'] if m['last_price'] is not none else '—'}}</div><div class="status">{{'✅ TRIGGERED' if m['triggered'] else '🟢 ARMED'}}</div><div><a href="/reset/{{m['id']}}"><button class="secondary">Reset</button></a> <a href="/delete/{{m['id']}}"><button class="danger">Delete</button></a></div></div></div>{% endfor %}</div>
<div class="card"><h2>🔔 Notification test</h2><a href="/test"><button>Send test to iPhone</button></a><div class="small" style="margin-top:8px">Use Pushover on your iPhone. Enable Pushover in Withings notifications for ScanWatch alerts.</div></div>
</div></body></html>'''


def db_conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with db_conn() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,grp TEXT,direction TEXT,target REAL,triggered INTEGER DEFAULT 0,last_price REAL,created TEXT)''')
        c.commit()

def send_push(title, msg):
    if not PUSHOVER_APP_TOKEN or not PUSHOVER_USER_KEY:
        print('Pushover not configured:', title, msg)
        return
    try:
        requests.post('https://api.pushover.net/1/messages.json', data={'token':PUSHOVER_APP_TOKEN,'user':PUSHOVER_USER_KEY,'title':title,'message':msg,'sound':'cashregister'}, timeout=10)
    except Exception as e:
        print('push error', e)

def latest_price(symbol):
    if not TWELVE_KEY: return None
    try:
        r = requests.get('https://api.twelvedata.com/price', params={'symbol':symbol,'apikey':TWELVE_KEY}, timeout=10)
        j = r.json()
        return float(j['price']) if 'price' in j else None
    except Exception as e:
        print('price error', symbol, e); return None

def monitor():
    while True:
        try:
            with db_conn() as c:
                rows = c.execute('SELECT * FROM alerts').fetchall()
                for a in rows:
                    p = latest_price(a['symbol'])
                    if p is None: continue
                    c.execute('UPDATE alerts SET last_price=? WHERE id=?',(p,a['id']))
                    if not a['triggered']:
                        hit = (a['direction']=='ABOVE' and p>=a['target']) or (a['direction']=='BELOW' and p<=a['target'])
                        if hit:
                            c.execute('UPDATE alerts SET triggered=1 WHERE id=?',(a['id'],))
                            send_push(f"🚨 {a['symbol']} PRICE ALERT", f"{a['symbol']} is {p}\nTarget: {a['direction']} {a['target']}")
                c.commit()
        except Exception as e: print('monitor error',e)
        time.sleep(CHECK_SECONDS)

@APP.route('/')
def home():
    with db_conn() as c: markets=c.execute('SELECT * FROM alerts ORDER BY grp,symbol').fetchall()
    return render_template_string(HTML, markets=markets, colors=COLORS)

@APP.post('/add')
def add():
    symbol=request.form['symbol'].upper().strip(); grp=request.form['group']; direction=request.form['direction']; target=float(request.form['target'])
    with db_conn() as c:
        c.execute('INSERT INTO alerts(symbol,grp,direction,target,created) VALUES(?,?,?,?,?)',(symbol,grp,direction,target,datetime.utcnow().isoformat())); c.commit()
    return '<meta http-equiv="refresh" content="0;url=/">'

@APP.route('/reset/<int:i>')
def reset(i):
    with db_conn() as c: c.execute('UPDATE alerts SET triggered=0 WHERE id=?',(i,)); c.commit()
    return '<meta http-equiv="refresh" content="0;url=/">'

@APP.route('/delete/<int:i>')
def delete(i):
    with db_conn() as c: c.execute('DELETE FROM alerts WHERE id=?',(i,)); c.commit()
    return '<meta http-equiv="refresh" content="0;url=/">'

@APP.route('/test')
def test():
    send_push('🔔 EDO MARKET ALERT TEST','Your cloud market alerts can reach this iPhone while the screen is locked.')
    return '<meta http-equiv="refresh" content="0;url=/">'

@APP.route('/health')
def health(): return jsonify(ok=True)

init_db()
threading.Thread(target=monitor, daemon=True).start()

if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=int(os.environ.get('PORT', '8080')))
