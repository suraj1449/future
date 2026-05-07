"""
Nifty Futures OI Analysis Dashboard
=====================================
1. Fill in API_KEY and ACCESS_TOKEN below
2. Run:  python nifty_oi_dashboard.py
3. Open: http://localhost:5001

Features:
  - 1/3/5/15 min intervals
  - Historical date picker (any past trading day)
  - All 4 SSE streams open simultaneously — every interval updates in background
  - Client-side cache — tab switches are instant, no spinner
  - Day theme by default

Fixes:
  - shiftDate: uses local date parts (not toISOString) to avoid UTC offset bug on IST
  - shiftDate: do...while loop so it steps correctly without skipping days
  - shiftDate: forward button now works correctly and clamps to today
  - todayStr: uses local date parts instead of toISOString (UTC shift fix)
"""

from flask import Flask, jsonify, render_template_string, Response, request
from kiteconnect import KiteConnect
import datetime, threading, time, logging, queue
from zoneinfo import ZoneInfo
import threading as _threading
import os

# ── Credentials ──────────────────────────────────────────────────────────────
API_KEY      = os.getenv("KITE_API_KEY", "")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "")
FETCH_DELAY_SECONDS = 10   # seconds after candle close before fetching
# ─────────────────────────────────────────────────────────────────────────────

app  = Flask(__name__)
kite = None
MARKET_TZ = ZoneInfo("Asia/Kolkata")

INTERVALS = {
    "1min":  {"kite": "minute",   "minutes": 1},
    "3min":  {"kite": "3minute",  "minutes": 3},
    "5min":  {"kite": "5minute",  "minutes": 5},
    "15min": {"kite": "15minute", "minutes": 15},
}

# ── Server-side cache (today only) ───────────────────────────────────────────
_raw_cache  = {k: [] for k in INTERVALS}
_cache_lock = threading.Lock()
_cache_ts   = {k: None for k in INTERVALS}

# ── SSE queues ────────────────────────────────────────────────────────────────
_subscribers      = {k: [] for k in INTERVALS}
_subscribers_lock = threading.Lock()


def _has_credentials(api_key, access_token):
    return bool((api_key or "").strip()) and bool((access_token or "").strip())


def _build_kite_client(api_key=None, access_token=None):
    api_key = API_KEY if api_key is None else api_key
    access_token = ACCESS_TOKEN if access_token is None else access_token
    if not _has_credentials(api_key, access_token):
        return None
    client = KiteConnect(api_key=api_key)
    client.set_access_token(access_token)
    return client


def set_kite_credentials(api_key, access_token):
    global API_KEY, ACCESS_TOKEN, kite
    API_KEY = api_key
    ACCESS_TOKEN = access_token
    kite = _build_kite_client(api_key, access_token)


def ensure_kite():
    global kite
    if kite is None:
        kite = _build_kite_client()
    if kite is None:
        raise RuntimeError("Set API_KEY and ACCESS_TOKEN before running FUTURE_BIAS.py")
    return kite


def market_now():
    return datetime.datetime.now(MARKET_TZ).replace(tzinfo=None)


def market_today():
    return market_now().date()


def _notify(interval_key, ts_str):
    with _subscribers_lock:
        dead = []
        for q in _subscribers[interval_key]:
            try:   q.put_nowait(ts_str)
            except queue.Full: dead.append(q)
        for q in dead:
            _subscribers[interval_key].remove(q)


def _next_fire(minutes):
    now      = market_now()
    now_s    = now.hour * 3600 + now.minute * 60 + now.second
    anchor   = 9 * 3600 + 15 * 60
    step     = minutes * 60
    if now_s <= anchor:
        fire_s = anchor + step + FETCH_DELAY_SECONDS
    else:
        done   = (now_s - anchor) // step
        fire_s = anchor + (done + 1) * step + FETCH_DELAY_SECONDS
        if fire_s <= now_s:
            fire_s += step
    fh, rem  = divmod(int(fire_s), 3600)
    fm, fs   = divmod(rem, 60)
    fire_dt  = now.replace(hour=fh % 24, minute=fm, second=fs, microsecond=0)
    if fire_dt < now:
        fire_dt += datetime.timedelta(days=1)
    return fire_dt


def _refresh_loop(key):
    minutes = INTERVALS[key]["minutes"]
    while True:
        fire_at   = _next_fire(minutes)
        sleep_sec = (fire_at - market_now()).total_seconds()
        logging.info("[%s] next fetch at %s (%.1fs)", key, fire_at.strftime("%H:%M:%S"), sleep_sec)
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        try:
            candles = _fetch_raw(key, market_today())
            now     = market_now()
            with _cache_lock:
                _raw_cache[key] = candles
                _cache_ts[key]  = now
            ts = now.strftime("%H:%M:%S")
            logging.info("[%s] updated — %d candles @ %s", key, len(candles), ts)
            _notify(key, ts)
        except Exception as e:
            logging.warning("[%s] fetch error: %s", key, e)


def _get_token(date):
    client = ensure_kite()
    instruments = client.instruments("NFO")
    futs = [i for i in instruments
            if i["name"] == "NIFTY" and i["instrument_type"] == "FUT"
            and i["expiry"] >= date]
    futs.sort(key=lambda x: x["expiry"])
    return futs[0]["instrument_token"] if futs else None


def classify(pc, oc):
    if pc > 0 and oc > 0: return "Long Build-up",  "#00c853"
    if pc < 0 and oc > 0: return "Short Build-up",  "#b71c1c"
    if pc > 0 and oc < 0: return "Short Covering",  "#69f0ae"
    if pc < 0 and oc < 0: return "Long Unwinding",  "#ef9a9a"
    return "Neutral", "#9e9e9e"


def _fetch_raw(key, date):
    client = ensure_kite()
    token = _get_token(date)
    if not token:
        return []
    today = market_today()
    from_dt = datetime.datetime.combine(date, datetime.time(9, 15))
    to_dt   = market_now() if date == today else \
              datetime.datetime.combine(date, datetime.time(15, 30))
    candles = client.historical_data(token, from_dt, to_dt, INTERVALS[key]["kite"], oi=True)
    if date == today and len(candles) > 1:
        candles = candles[:-1]
    return candles


def build_rows(candles, mode="close"):
    rows = []
    for i in range(1, len(candles)):
        p, c = candles[i-1], candles[i]
        pval  = c["open"]  if mode == "open"  else c["close"]
        ppval = p["open"]  if mode == "open"  else p["close"]
        pc    = pval - ppval
        oi_n, oi_p = c.get("oi", 0), p.get("oi", 0)
        oc    = oi_n - oi_p
        s     = max(0, i - 10)
        avg_v = sum(x["volume"] for x in candles[s:i]) / (i - s)
        lbl, col = classify(pc, oc)
        rows.append({
            "time":           str(c["date"]),
            "close":          round(c["close"], 2),
            "price_mode_val": round(pval, 2),
            "price_chg":      round(pc, 2),
            "oi":             oi_n,
            "oi_chg":         oc,
            "volume":         c["volume"],
            "avg_vol":        round(avg_v, 0),
            "vol_signal":     "High" if c["volume"] >= avg_v else "Low",
            "label":          lbl,
            "color":          col,
        })
    return rows


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/data/<iv>")
def api_data(iv):
    try:
        if iv not in INTERVALS:
            return jsonify({"error": "bad interval"}), 400
        mode = request.args.get("mode", "close")
        if mode not in ("close", "open"): mode = "close"

        ds    = request.args.get("date", "")
        today = market_today()
        try:
            req_date = datetime.date.fromisoformat(ds) if ds else today
        except ValueError:
            req_date = today
        is_today = req_date == today

        if is_today:
            with _cache_lock:
                candles, ts = _raw_cache[iv], _cache_ts[iv]
            if not candles:
                candles = _fetch_raw(iv, today)
                now = market_now()
                with _cache_lock:
                    _raw_cache[iv] = candles
                    _cache_ts[iv]  = now
                ts = now
        else:
            candles = _fetch_raw(iv, req_date)
            ts = None

        rows = build_rows(candles, mode)
        return jsonify({
            "rows":      rows,
            "cached_at": ts.strftime("%H:%M:%S") if ts else req_date.isoformat(),
            "row_count": len(rows),
            "is_today":  is_today,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stream/<iv>")
def api_stream(iv):
    if iv not in INTERVALS:
        return jsonify({"error": "bad interval"}), 400
    q = queue.Queue(maxsize=10)
    with _subscribers_lock:
        _subscribers[iv].append(q)

    def gen():
        try:
            while True:
                try:    ts = q.get(timeout=25); yield "event: update\ndata: {}\n\n".format(ts)
                except queue.Empty: yield ": heartbeat\n\n"
        finally:
            with _subscribers_lock:
                try: _subscribers[iv].remove(q)
                except ValueError: pass

    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nifty Futures -- OI Analysis</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Barlow+Condensed:wght@400;600;700&display=swap" rel="stylesheet">
<style>
:root,[data-theme="light"]{
  --bg:#f4f6fa;--surface:#fff;--surface2:#eef0f5;--border:#d1d8e0;
  --accent:#b8860b;--text:#1a2030;--muted:#7a8599;--tab-text:#5a6577;
  --th-bg:#eef0f5;--th-color:#b8860b;--row-hover:#e4e8f0;
  --hist-bg:#ddeeff;--hist-text:#1a6fa0;
}
[data-theme="dark"]{
  --bg:#0b0e14;--surface:#131720;--surface2:#1a1f2b;--border:#2a3040;
  --accent:#e8b84b;--text:#cdd6e0;--muted:#8a96a8;--tab-text:#b0bac8;
  --th-bg:#1a1f2b;--th-color:#e8b84b;--row-hover:#1c2230;
  --hist-bg:#1a2540;--hist-text:#7ecfff;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Barlow Condensed',sans-serif;min-height:100vh;transition:background .3s,color .3s}
header{border-bottom:1px solid var(--border);padding:14px 24px;display:flex;align-items:center;justify-content:space-between;background:var(--surface);flex-wrap:wrap;gap:10px;transition:background .3s}
header h1{font-size:20px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--accent)}
.hdr-right{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
#clock{font-family:'IBM Plex Mono',monospace;font-size:12px;color:var(--tab-text)}
.date-bar{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.date-label{font-family:'IBM Plex Mono',monospace;font-size:11px;letter-spacing:1px;text-transform:uppercase;color:var(--muted)}
.date-nav{display:flex;align-items:center;gap:3px}
.date-nav button{width:26px;height:26px;background:transparent;border:1px solid var(--border);color:var(--tab-text);font-size:14px;cursor:pointer;border-radius:3px;transition:.2s;display:flex;align-items:center;justify-content:center}
.date-nav button:hover{border-color:var(--accent);color:var(--accent)}
#date-input{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:5px 8px;font-family:'IBM Plex Mono',monospace;font-size:12px;border-radius:3px;cursor:pointer;outline:none;transition:border-color .2s}
#date-input:hover,#date-input:focus{border-color:var(--accent)}
[data-theme="dark"] #date-input::-webkit-calendar-picker-indicator{filter:invert(1) brightness(.7)}
.today-btn{padding:5px 10px;background:transparent;border:1px solid var(--accent);color:var(--accent);font-family:'IBM Plex Mono',monospace;font-size:11px;letter-spacing:1px;cursor:pointer;border-radius:3px;transition:.2s;text-transform:uppercase}
.today-btn:hover{background:var(--accent);color:#000}
#hist-banner{display:none;background:var(--hist-bg);border-bottom:1px solid var(--border);padding:7px 24px;font-family:'IBM Plex Mono',monospace;font-size:12px;color:var(--hist-text);align-items:center;gap:8px}
#hist-banner.show{display:flex}
.theme-toggle{display:flex;align-items:center;gap:8px;cursor:pointer;user-select:none}
.theme-toggle span{font-family:'IBM Plex Mono',monospace;font-size:11px;color:var(--tab-text);letter-spacing:1px}
.tog-track{width:44px;height:22px;border-radius:11px;background:var(--border);border:1px solid var(--accent);position:relative;cursor:pointer;transition:background .3s}
[data-theme="light"] .tog-track{background:#d4a017}
.tog-thumb{width:16px;height:16px;border-radius:50%;background:var(--accent);position:absolute;top:2px;left:3px;transition:transform .3s}
[data-theme="light"] .tog-thumb{transform:translateX(22px)}
.tabs{display:flex;gap:4px;padding:14px 24px 0;flex-wrap:wrap;align-items:center}
.tab{padding:7px 18px;font-family:'IBM Plex Mono',monospace;font-size:13px;border:1px solid var(--border);background:var(--surface);color:var(--tab-text);cursor:pointer;letter-spacing:1px;transition:all .2s}
.tab:hover{border-color:var(--accent);color:var(--accent)}
.tab.active{border-color:var(--accent);background:var(--accent);color:#000;font-weight:600}
.refresh-btn{padding:6px 12px;background:transparent;border:1px solid var(--accent);color:var(--accent);font-family:'IBM Plex Mono',monospace;font-size:12px;cursor:pointer;letter-spacing:1px;transition:.2s}
.refresh-btn:hover{background:var(--accent);color:#000}
.mode-group{margin-left:auto;display:flex;align-items:center;gap:8px}
.mode-label{font-family:'IBM Plex Mono',monospace;font-size:11px;color:var(--tab-text);letter-spacing:1px;text-transform:uppercase;white-space:nowrap}
.mode-pill{display:flex;border:1px solid var(--accent);border-radius:4px;overflow:hidden;font-family:'IBM Plex Mono',monospace;font-size:12px}
.mode-pill button{padding:6px 12px;border:none;background:transparent;color:var(--tab-text);cursor:pointer;letter-spacing:1px;text-transform:uppercase;transition:all .2s}
.mode-pill button:first-child{border-right:1px solid var(--accent)}
.mode-pill button.active{background:var(--accent);color:#000;font-weight:600}
.mode-pill button:not(.active):hover{color:var(--accent)}
.mode-badge{display:inline-block;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:600;letter-spacing:1px;background:var(--accent);color:#000;margin-left:6px;vertical-align:middle}
.live-dot{width:8px;height:8px;border-radius:50%;background:#00c853;display:inline-block;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.main{padding:18px 24px 40px}
.legend{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px}
.legend-item{display:flex;align-items:center;gap:6px;font-size:13px;font-family:'IBM Plex Mono',monospace}
.dot{width:12px;height:12px;border-radius:50%;flex-shrink:0}
.chart-box{background:var(--surface);border:1px solid var(--border);padding:16px;margin-bottom:16px;transition:background .3s,border-color .3s}
.chart-label{font-size:11px;color:var(--muted);font-family:'IBM Plex Mono',monospace;margin-bottom:10px;letter-spacing:1px;text-transform:uppercase}
#chart-canvas{width:100%;height:160px;display:block}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-family:'IBM Plex Mono',monospace;font-size:12px}
th{background:var(--th-bg);color:var(--th-color);text-align:left;padding:10px 12px;border-bottom:1px solid var(--border);letter-spacing:1px;font-size:11px;text-transform:uppercase;position:sticky;top:0;transition:background .3s,color .3s}
td{padding:8px 12px;border-bottom:1px solid var(--border);white-space:nowrap}
tr:hover td{background:var(--row-hover)}
.badge{display:inline-block;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:600;color:#000}
[data-theme="light"] .badge{color:#1a1a1a}
.loading{text-align:center;padding:60px;color:var(--muted);font-family:'IBM Plex Mono',monospace;font-size:14px}
.error{text-align:center;padding:40px;color:#ef9a9a;font-family:'IBM Plex Mono',monospace;font-size:13px}
.open-hl{color:#7ecfff!important}
[data-theme="light"] .open-hl{color:#1a6fa0!important}
</style>
</head>
<body>

<header>
  <h1>Nifty Futures &middot; OI Analysis</h1>
  <div class="hdr-right">
    <div class="date-bar">
      <span class="date-label">Date</span>
      <div class="date-nav">
        <button onclick="shiftDate(-1)" title="Prev trading day">&lsaquo;</button>
        <input type="date" id="date-input" onchange="onDateChange()">
        <button onclick="shiftDate(1)" title="Next trading day">&rsaquo;</button>
      </div>
      <button class="today-btn" onclick="goToday()">Today</button>
    </div>
    <span id="clock">--</span>
    <span id="live-ind"></span>
    <div class="theme-toggle" onclick="toggleTheme()" title="Toggle theme">
      <span id="t-icon">&#9728;&#65039;</span>
      <div class="tog-track"><div class="tog-thumb"></div></div>
      <span id="t-label">DAY</span>
    </div>
  </div>
</header>

<div id="hist-banner">
  <span>&#128197;</span>
  <span id="hist-txt">Historical mode</span>
</div>

<div class="tabs">
  <button class="tab active" onclick="loadTab('1min',this)">1 MIN</button>
  <button class="tab"        onclick="loadTab('3min',this)">3 MIN</button>
  <button class="tab"        onclick="loadTab('5min',this)">5 MIN</button>
  <button class="tab"        onclick="loadTab('15min',this)">15 MIN</button>
  <button class="refresh-btn" onclick="hardRefresh()">&#8635; REFRESH</button>
  <div class="mode-group">
    <span class="mode-label">Price &Delta; on</span>
    <div class="mode-pill">
      <button id="btn-close" class="active" onclick="setMode('close')">CLOSE</button>
      <button id="btn-open"                 onclick="setMode('open')">OPEN</button>
    </div>
  </div>
</div>

<div class="main">
  <div class="legend">
    <div class="legend-item"><div class="dot" style="background:#00c853"></div> Long Build-up</div>
    <div class="legend-item"><div class="dot" style="background:#b71c1c"></div> Short Build-up</div>
    <div class="legend-item"><div class="dot" style="background:#69f0ae"></div> Short Covering</div>
    <div class="legend-item"><div class="dot" style="background:#ef9a9a"></div> Long Unwinding</div>
    <div class="legend-item"><div class="dot" style="background:#9e9e9e"></div> Neutral</div>
  </div>
  <div class="chart-box">
    <div class="chart-label" id="chart-label">Close Price with OI Signal Dots</div>
    <canvas id="chart-canvas"></canvas>
  </div>
  <div id="table-container"><div class="loading">Loading&hellip;</div></div>
</div>

<script>
(function () {
  'use strict';

  // ── State ────────────────────────────────────────────────────────────────
  var currentTab  = '1min';
  var currentMode = 'close';
  var currentDate = todayStr();
  var chartData   = [];
  var cache  = {};          // "iv|date|mode" -> { rows, cached_at }
  var sseMap = {};          // iv -> EventSource
  var ALL_IV = ['1min','3min','5min','15min'];

  // ── Helpers ──────────────────────────────────────────────────────────────

  // FIX: Use local date parts instead of toISOString() which returns UTC.
  // On IST (UTC+5:30) toISOString() can return the previous calendar day,
  // causing the date to appear shifted back by one day.
  function todayStr() {
    var d = new Date();
    var y = d.getFullYear();
    var m = String(d.getMonth() + 1).padStart(2, '0');
    var day = String(d.getDate()).padStart(2, '0');
    return y + '-' + m + '-' + day;
  }

  function isToday(d) { return d === todayStr(); }
  function ck(iv, d, m) { return iv + '|' + d + '|' + m; }
  function fmtDate(ds) {
    if (!ds) return '';
    var p = ds.split('-');
    var mo = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return p[2] + ' ' + mo[+p[1]-1] + ' ' + p[0];
  }
  function bustToday(iv) {
    var t = todayStr();
    ALL_IV.forEach(function(m) { delete cache[ck(iv, t, m)]; });
    ['close','open'].forEach(function(m) { delete cache[ck(iv, t, m)]; });
  }

  // ── Clock ────────────────────────────────────────────────────────────────
  setInterval(function () {
    document.getElementById('clock').textContent =
      new Date().toLocaleTimeString('en-IN', { hour12: false });
  }, 1000);

  // ── Date picker ──────────────────────────────────────────────────────────
  (function () {
    var inp = document.getElementById('date-input');
    inp.max = todayStr();
    inp.value = currentDate;
  }());

  window.onDateChange = function () {
    currentDate = document.getElementById('date-input').value || todayStr();
    applyDateMode();
    fetchData(currentTab);
  };

  window.goToday = function () {
    currentDate = todayStr();
    document.getElementById('date-input').value = currentDate;
    applyDateMode();
    fetchData(currentTab);
    connectAllSSE();
  };

  // FIX: shiftDate rewritten to:
  //   1. Parse date using local year/month/day parts (avoids UTC offset shifting the day)
  //   2. Use do...while so it always steps at least 1 day then skips weekends correctly
  //   3. Clamp forward navigation to today using local date comparison
  //   4. Format result using local date parts (not toISOString)
  window.shiftDate = function (delta) {
    // Parse as local date to avoid UTC offset issue
    var parts = currentDate.split('-');
    var d = new Date(+parts[0], +parts[1] - 1, +parts[2]);

    // Step one day at a time in the requested direction, skipping weekends
    do {
      d.setDate(d.getDate() + delta);
    } while (d.getDay() === 0 || d.getDay() === 6);

    // Clamp to today (compare using local midnight)
    var today = new Date();
    today.setHours(0, 0, 0, 0);
    if (d > today) d = today;

    // Format as YYYY-MM-DD using local date parts
    var y   = d.getFullYear();
    var mo  = String(d.getMonth() + 1).padStart(2, '0');
    var day = String(d.getDate()).padStart(2, '0');
    currentDate = y + '-' + mo + '-' + day;

    document.getElementById('date-input').value = currentDate;
    applyDateMode();
    fetchData(currentTab);
    if (isToday(currentDate)) { connectAllSSE(); } else { disconnectAllSSE(); }
  };

  function applyDateMode() {
    var banner = document.getElementById('hist-banner');
    var ind    = document.getElementById('live-ind');
    if (isToday(currentDate)) {
      banner.classList.remove('show');
      ind.innerHTML = '<span class="live-dot"></span>';
    } else {
      document.getElementById('hist-txt').textContent =
        'Historical: ' + fmtDate(currentDate) + ' \u2014 live refresh off';
      banner.classList.add('show');
      ind.innerHTML = '';
    }
  }

  // ── Theme ────────────────────────────────────────────────────────────────
  window.toggleTheme = function () {
    var html = document.documentElement;
    var dark = html.getAttribute('data-theme') === 'dark';
    html.setAttribute('data-theme', dark ? 'light' : 'dark');
    document.getElementById('t-icon').innerHTML  = dark ? '&#9728;&#65039;' : '&#127769;';
    document.getElementById('t-label').textContent = dark ? 'DAY' : 'NIGHT';
    if (chartData.length) renderChart(chartData);
  };

  // ── Price mode ───────────────────────────────────────────────────────────
  window.setMode = function (mode) {
    if (mode === currentMode) return;
    currentMode = mode;
    document.getElementById('btn-close').classList.toggle('active', mode === 'close');
    document.getElementById('btn-open').classList.toggle('active',  mode === 'open');
    document.getElementById('chart-label').textContent =
      (mode === 'open' ? 'Open' : 'Close') + ' Price with OI Signal Dots';
    fetchData(currentTab);
  };

  // ── Tab switch ───────────────────────────────────────────────────────────
  window.loadTab = function (iv, btn) {
    currentTab = iv;
    document.querySelectorAll('.tab').forEach(function (t) { t.classList.remove('active'); });
    if (btn) btn.classList.add('active');
    fetchData(iv);
  };

  window.hardRefresh = function () {
    delete cache[ck(currentTab, currentDate, currentMode)];
    doFetch(currentTab);
  };

  // ── Fetch ────────────────────────────────────────────────────────────────
  function fetchData(iv) {
    var key = ck(iv, currentDate, currentMode);
    if (cache[key]) {
      // Serve instantly from cache — no spinner
      var c = cache[key];
      chartData = c.rows;
      if (c.cached_at && isToday(currentDate)) {
        document.getElementById('clock').textContent = 'Updated ' + c.cached_at;
      }
      renderTable(chartData);
      renderChart(chartData);
    } else {
      doFetch(iv);
    }
  }

  function doFetch(iv) {
    document.getElementById('table-container').innerHTML =
      '<div class="loading">Loading ' + iv + ' (' + currentMode + ')' +
      (isToday(currentDate) ? ' today' : ' ' + fmtDate(currentDate)) + '&hellip;</div>';

    var url = '/api/data/' + iv + '?mode=' + currentMode + '&date=' + currentDate;
    fetchJson(url)
      .then(function (json) {
        if (json.error) throw new Error(json.error);
        var rows = json.rows || [];
        cache[ck(iv, currentDate, currentMode)] = { rows: rows, cached_at: json.cached_at };
        if (iv !== currentTab) return;   // user switched tab while fetching
        chartData = rows;
        if (json.cached_at && isToday(currentDate)) {
          document.getElementById('clock').textContent = 'Updated ' + json.cached_at;
        }
        renderTable(rows);
        renderChart(rows);
      })
      .catch(function (e) {
        if (iv !== currentTab) return;
        document.getElementById('table-container').innerHTML =
          '<div class="error">&#9888; ' + e.message + '</div>';
      });
  }

  // ── SSE — all 4 streams always open ─────────────────────────────────────
  function connectOne(iv) {
    if (sseMap[iv]) { sseMap[iv].close(); }
    var es = new EventSource('/api/stream/' + iv);
    sseMap[iv] = es;

    es.addEventListener('update', function () {
      if (!isToday(currentDate)) return;
      bustToday(iv);
      // Pre-fetch both modes silently for this interval
      ['close', 'open'].forEach(function (m) {
        fetchJson('/api/data/' + iv + '?mode=' + m + '&date=' + currentDate)
          .then(function (json) {
            if (json.error) return;
            cache[ck(iv, currentDate, m)] = { rows: json.rows || [], cached_at: json.cached_at };
            // Re-render only if this is the active tab + mode
            if (iv === currentTab && m === currentMode) {
              var c = cache[ck(iv, currentDate, m)];
              chartData = c.rows;
              if (c.cached_at) {
                document.getElementById('clock').textContent = 'Updated ' + c.cached_at;
              }
              renderTable(chartData);
              renderChart(chartData);
            }
          })
          .catch(function () {});
      });
    });

    es.onerror = function () {
      es.close();
      delete sseMap[iv];
      setTimeout(function () { connectOne(iv); }, 5000);
    };
  }

  function fetchJson(url) {
    return fetch(url).then(function (r) {
      return r.text().then(function (text) {
        try {
          return JSON.parse(text);
        } catch (err) {
          if (!r.ok) {
            throw new Error('HTTP ' + r.status + ' from ' + url);
          }
          throw new Error('Server returned non-JSON response for ' + url);
        }
      });
    });
  }

  function connectAllSSE() {
    if (!isToday(currentDate)) { disconnectAllSSE(); return; }
    ALL_IV.forEach(connectOne);
  }

  function disconnectAllSSE() {
    ALL_IV.forEach(function (k) {
      if (sseMap[k]) { sseMap[k].close(); delete sseMap[k]; }
    });
  }

  // ── Table ────────────────────────────────────────────────────────────────
  function renderTable(rows) {
    if (!rows.length) {
      document.getElementById('table-container').innerHTML =
        '<div class="loading">No data for this date / interval.</div>';
      return;
    }
    var rev  = rows.slice().reverse();
    var dark = document.documentElement.getAttribute('data-theme') === 'dark';
    var posC = dark ? '#69f0ae' : '#1a7a40';
    var negC = dark ? '#ef9a9a' : '#c0392b';
    var isOpen = currentMode === 'open';
    var hlCls  = isOpen ? 'open-hl' : '';
    var badge  = '<span class="mode-badge">' + currentMode.toUpperCase() + '</span>';

    var h = '<div class="table-wrap"><table><thead><tr>' +
      '<th>Time</th>' +
      '<th>Close</th>' +
      '<th>' + (isOpen ? 'OPEN' : 'CLOSE') + ' ' + badge + '</th>' +
      '<th>Price &Delta;</th>' +
      '<th>Dir</th>' +
      '<th>Open Interest</th>' +
      '<th>OI &Delta;</th>' +
      '<th>Volume</th>' +
      '<th>Avg Vol(10)</th>' +
      '<th>Vol Signal</th>' +
      '<th>Interpretation</th>' +
      '</tr></thead><tbody>';

    rev.forEach(function (r) {
      var pC = r.price_chg >= 0 ? posC : negC;
      var oC = r.oi_chg    >= 0 ? posC : negC;
      h += '<tr>' +
        '<td>' + r.time.slice(11, 16) + '</td>' +
        '<td>' + r.close.toLocaleString('en-IN') + '</td>' +
        '<td class="' + hlCls + '">' + r.price_mode_val.toLocaleString('en-IN') + '</td>' +
        '<td style="color:' + pC + '" class="' + hlCls + '">' + (r.price_chg >= 0 ? '+' : '') + r.price_chg + '</td>' +
        '<td>' + (r.price_chg >= 0 ? '&#8679;' : '&#8681;') + '</td>' +
        '<td>' + r.oi.toLocaleString('en-IN') + '</td>' +
        '<td style="color:' + oC + '">' + (r.oi_chg >= 0 ? '+' : '') + r.oi_chg.toLocaleString('en-IN') + '</td>' +
        '<td>' + r.volume.toLocaleString('en-IN') + '</td>' +
        '<td>' + r.avg_vol.toLocaleString('en-IN') + '</td>' +
        '<td>' + (r.vol_signal === 'High' ? '&#8679; High' : '&#8681; Low') + '</td>' +
        '<td><span class="badge" style="background:' + r.color + '">' + r.label + '</span></td>' +
        '</tr>';
    });
    h += '</tbody></table></div>';
    document.getElementById('table-container').innerHTML = h;
  }

  // ── Chart ────────────────────────────────────────────────────────────────
  function renderChart(rows) {
    var canvas = document.getElementById('chart-canvas');
    var ctx    = canvas.getContext('2d');
    var W = canvas.offsetWidth, H = 160;
    canvas.width = W; canvas.height = H;
    if (!rows.length) return;
    ctx.clearRect(0, 0, W, H);

    var dark   = document.documentElement.getAttribute('data-theme') === 'dark';
    var gridC  = dark ? '#1e2530' : '#d1d8e0';
    var lblC   = dark ? '#5a6577' : '#7a8599';
    var lineC  = currentMode === 'open'
      ? (dark ? '#7ecfff' : '#1a6fa0')
      : (dark ? '#e8b84b' : '#b8860b');
    var dotBdr = dark ? '#0b0e14' : '#f4f6fa';

    var prices = rows.map(function (r) { return r.close; });
    var mn = Math.min.apply(null, prices), mx = Math.max.apply(null, prices);
    var pad = { l: 10, r: 10, t: 15, b: 25 };
    var iW = W - pad.l - pad.r, iH = H - pad.t - pad.b, rng = mx - mn || 1;
    function toX(i) { return pad.l + (i / (rows.length - 1 || 1)) * iW; }
    function toY(p) { return pad.t + iH - ((p - mn) / rng) * iH; }

    // Grid
    ctx.strokeStyle = gridC; ctx.lineWidth = 1;
    for (var g = 0; g <= 4; g++) {
      var gy = pad.t + (g / 4) * iH;
      ctx.beginPath(); ctx.moveTo(pad.l, gy); ctx.lineTo(pad.l + iW, gy); ctx.stroke();
      ctx.fillStyle = lblC; ctx.font = '10px IBM Plex Mono';
      ctx.fillText((mx - (g / 4) * rng).toFixed(0), 2, gy + 4);
    }
    // Price line
    ctx.beginPath();
    rows.forEach(function (r, i) {
      var x = toX(i), y = toY(r.close);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.strokeStyle = lineC; ctx.lineWidth = 1.5; ctx.stroke();
    // Signal dots
    rows.forEach(function (r, i) {
      var x = toX(i), y = toY(r.close);
      ctx.beginPath(); ctx.arc(x, y, 5, 0, Math.PI * 2);
      ctx.fillStyle = r.color; ctx.fill();
      ctx.strokeStyle = dotBdr; ctx.lineWidth = 1; ctx.stroke();
    });
    // Time labels
    ctx.fillStyle = lblC; ctx.font = '9px IBM Plex Mono';
    var step = Math.max(1, Math.floor(rows.length / 8));
    rows.forEach(function (r, i) {
      if (i % step === 0) ctx.fillText(r.time.slice(11, 16), toX(i) - 12, H - 5);
    });
  }

  // ── Boot ─────────────────────────────────────────────────────────────────
  applyDateMode();
  fetchData('1min');
  connectAllSSE();   // opens all 4 SSE streams simultaneously

}());
</script>
</body>
</html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

@app.route("/healthz")
def healthz():
    return {"ok": True}

def _keep_alive():
    import requests, time
    url = os.getenv("RENDER_EXTERNAL_URL", "")
    if not url:
        return
    while True:
        time.sleep(600)
        try:
            requests.get(url + "/healthz", timeout=10)
        except Exception:
            pass

def _startup_future():
    try:
        ensure_kite()
        logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")

        # Warm up today's cache — exact same logic as before
        today = market_today()
        for k in INTERVALS:
            try:
                candles = _fetch_raw(k, today)
                now = market_now()
                with _cache_lock:
                    _raw_cache[k] = candles
                    _cache_ts[k]  = now
                print("  ok {:6s}  {} candles".format(k, len(candles)))
            except Exception as e:
                print("  !! {:6s}  {}".format(k, e))

        # Start refresh loops — exact same logic as before
        for k in INTERVALS:
            _threading.Thread(
                target=_refresh_loop,
                args=(k,),
                daemon=True,
                name="refresh-"+k
            ).start()
            fire_at   = _next_fire(INTERVALS[k]["minutes"])
            sleep_sec = (fire_at - market_now()).total_seconds()
            print("  {:<8}  {:>12}  {:>9.1f}s".format(
                k, fire_at.strftime("%H:%M:%S"), sleep_sec))

        # Keep alive thread
        _threading.Thread(target=_keep_alive, daemon=True).start()

    except Exception as e:
        print("Startup error:", e)

with app.app_context():
    _startup_future()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    print("\n  http://localhost:" + str(port))
    print("=" * 55)
    app.run(debug=False, port=port, host="0.0.0.0", threaded=True)
