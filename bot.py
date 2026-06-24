#!/usr/bin/env python3
"""
monad-tg-bot — Telegram monitoring/ops bot for the Monad node on ovh-117-52.

Pure stdlib (urllib, json, subprocess). Single-threaded loop:
  - long-polls Telegram getUpdates for commands (/status, /sync, /disk, /waltrace, /node, /help)
  - every CHECK_INTERVAL runs health checks and pushes alerts on state change

Config: /opt/monad-tg-bot/config.env   State: /opt/monad-tg-bot/state.json
Runs as root (needs systemctl/journalctl/df and the root-owned watchdog log).
"""
import json, os, re, ssl, subprocess, sys, time, urllib.parse, urllib.request

CFG_PATH   = "/opt/monad-tg-bot/config.env"
STATE_PATH = "/opt/monad-tg-bot/state.json"
WATCHDOG_LOG = "/var/log/monad-waltrace-watchdog.log"
RPC = "http://localhost:8080"
SERVICES = ["monad-bft", "monad-execution", "monad-rpc"]
HOST = os.uname().nodename

# ---------- config ----------
def load_cfg():
    cfg = {}
    try:
        with open(CFG_PATH) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return cfg

CFG = load_cfg()
TOKEN = CFG.get("BOT_TOKEN", "").strip()
ALLOWED = set(x.strip() for x in CFG.get("ALLOWED_CHAT_IDS", "").split(",") if x.strip())
CHECK_INTERVAL   = int(CFG.get("CHECK_INTERVAL", "60"))
DISK_WARN_PCT    = int(CFG.get("DISK_WARN_PCT", "85"))
SYNC_LAG_WARN    = int(CFG.get("SYNC_LAG_WARN_SEC", "30"))
API = "https://api.telegram.org/bot%s/" % TOKEN
SSLCTX = ssl.create_default_context()

# ---------- telegram api ----------
def tg(method, params=None, timeout=35):
    data = urllib.parse.urlencode(params or {}).encode()
    req = urllib.request.Request(API + method, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=SSLCTX) as r:
            return json.load(r)
    except Exception as e:
        sys.stderr.write("tg %s error: %s\n" % (method, e)); sys.stderr.flush()
        return None

def send(chat_id, text):
    # plain text, split if very long
    for i in range(0, len(text), 3900):
        tg("sendMessage", {"chat_id": chat_id, "text": text[i:i+3900],
                           "disable_web_page_preview": "true"})

def broadcast(text):
    for cid in ALLOWED:
        send(cid, text)

# ---------- shell helpers ----------
def sh(cmd, timeout=10):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=timeout).stdout.strip()
    except Exception:
        return ""

def rpc(method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "method": method,
                       "params": params or [], "id": 1}).encode()
    req = urllib.request.Request(RPC, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=6) as r:
            return json.load(r).get("result")
    except Exception:
        return None

# ---------- checks ----------
def svc_active(s):   return sh("systemctl is-active %s" % s) == "active"
def svc_start(s):    return sh("systemctl show %s -p ExecMainStartTimestamp --value" % s)

def get_sync():
    syncing = rpc("eth_syncing")
    blk = rpc("eth_blockNumber")
    ts  = None
    b = rpc("eth_getBlockByNumber", ["latest", False])
    if isinstance(b, dict) and b.get("timestamp"):
        ts = int(b["timestamp"], 16)
    height = int(blk, 16) if blk else None
    lag = (int(time.time()) - ts) if ts else None
    return {"syncing": syncing, "height": height, "lag": lag}

def get_disk():
    out = sh("df -P / | tail -1")  # md3
    parts = out.split()
    if len(parts) >= 5:
        pct = int(parts[4].rstrip("%"))
        avail_gb = int(parts[3]) / 1024 / 1024
        return {"pct": pct, "avail_gb": round(avail_gb, 1)}
    return {"pct": None, "avail_gb": None}

def last_watchdog_action():
    out = sh("grep -E 'ACTION|restarted' %s 2>/dev/null | tail -1" % WATCHDOG_LOG)
    return out

def watchdog_restart_count():
    return sh("grep -c 'ACTION: restarting' %s 2>/dev/null" % WATCHDOG_LOG) or "0"

def node_version():
    v = sh("monad-node --version 2>/dev/null")
    m = re.search(r'"tag":"([^"]+)"', v)
    return m.group(1) if m else (v[:40] or "?")

# ---------- formatted reports (for commands) ----------
def fmt_status():
    L = ["📟 Monad node — %s" % HOST]
    bad = [s for s in SERVICES if not svc_active(s)]
    L.append("Сервисы: " + ("✅ все active" if not bad else "🔴 down: " + ", ".join(bad)))
    s = get_sync()
    if s["height"] is not None:
        sync_txt = "✅ у типа" if s["syncing"] in (False, None) else "🟡 syncing"
        lag = "?" if s["lag"] is None else "%ds" % s["lag"]
        L.append("Синк: %s  блок %s  lag %s" % (sync_txt, s["height"], lag))
    else:
        L.append("Синк: 🔴 RPC не отвечает")
    d = get_disk()
    if d["pct"] is not None:
        icon = "✅" if d["pct"] < DISK_WARN_PCT else "🟡"
        L.append("Диск /: %s %d%% занято, %s ГБ свободно" % (icon, d["pct"], d["avail_gb"]))
    L.append("Версия: %s" % node_version())
    L.append("Waltrace-рестартов всего: %s" % watchdog_restart_count())
    return "\n".join(L)

def fmt_sync():
    s = get_sync()
    if s["height"] is None:
        return "🔴 RPC :8080 не отвечает"
    return ("Синк %s\nblock: %s\nlag: %s\neth_syncing: %s" % (
        "✅ у типа" if s["syncing"] in (False, None) else "🟡 syncing",
        s["height"], "?" if s["lag"] is None else "%ds" % s["lag"], s["syncing"]))

def fmt_disk():
    d = get_disk()
    top = sh("df -h / | tail -1")
    return "Диск /: %s\n%s" % (
        "—" if d["pct"] is None else "%d%% занято, %s ГБ свободно" % (d["pct"], d["avail_gb"]), top)

def fmt_waltrace():
    cnt = watchdog_restart_count()
    last = last_watchdog_action() or "нет записей"
    flood = sh("journalctl -u monad-bft --since '5 min ago' --no-pager 2>/dev/null | grep -c 'waltrace thread stopped'")
    return ("Waltrace watchdog\nВсего авто-рестартов: %s\nФлуд за 5 мин: %s\nПоследнее действие:\n%s"
            % (cnt, flood, last))

def fmt_node():
    up = sh("systemctl show monad-bft -p ExecMainStartTimestamp --value")
    peers = sh("journalctl -u monad-bft --since '1 min ago' --no-pager 2>/dev/null | grep -oE 'node_id\":\"02[0-9a-f]{6}' | sort -u | wc -l")
    return ("Нода %s\nВерсия: %s\nmonad-bft с: %s\nУникальных пиров в логе/мин: %s"
            % (HOST, node_version(), up, peers))

HELP = ("Monad node bot — %s\n\n"
        "/status — общая сводка\n"
        "/sync — статус синхронизации\n"
        "/disk — диск/ресурсы\n"
        "/waltrace — watchdog/waltrace\n"
        "/node — версия/аптайм/пиры\n"
        "/id — показать chat id\n"
        "/help — помощь" % HOST)

# ---------- state & alerts ----------
def load_state():
    try:
        with open(STATE_PATH) as f: return json.load(f)
    except Exception:
        return {}

def save_state(st):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f: json.dump(st, f)
    os.replace(tmp, STATE_PATH)

def monitor(st):
    alerts = []
    # services up/down + restart detection
    for s in SERVICES:
        active = svc_active(s)
        start = svc_start(s)
        prev_active = st.get("active_" + s)
        prev_start  = st.get("start_" + s)
        if prev_active is True and not active:
            alerts.append("🔴 СЕРВИС УПАЛ: %s неактивен!" % s)
        elif prev_active is False and active:
            alerts.append("✅ Сервис восстановлен: %s снова active" % s)
        if prev_start and start and start != prev_start and active:
            wd = last_watchdog_action()
            tag = " (watchdog/waltrace — ожидаемо)" if (s == "monad-bft" and wd and "restart" in wd.lower()) else ""
            alerts.append("🔄 РЕСТАРТ: %s перезапущен%s\nстарт: %s" % (s, tag, start))
        st["active_" + s] = active
        st["start_" + s]  = start
    # sync lag
    s = get_sync()
    behind = s["height"] is not None and s["lag"] is not None and s["lag"] > SYNC_LAG_WARN
    rpc_dead = s["height"] is None
    if rpc_dead and not st.get("rpc_dead"):
        alerts.append("🔴 RPC :8080 не отвечает")
    elif not rpc_dead and st.get("rpc_dead"):
        alerts.append("✅ RPC :8080 снова отвечает")
    st["rpc_dead"] = rpc_dead
    if behind and not st.get("behind"):
        alerts.append("🟡 ОТСТАВАНИЕ СИНКА: lag %ds (блок %s)" % (s["lag"], s["height"]))
    elif not behind and st.get("behind") and not rpc_dead:
        alerts.append("✅ Синк восстановлен (lag %ss)" % (s["lag"]))
    st["behind"] = behind
    # disk
    d = get_disk()
    if d["pct"] is not None:
        warn = d["pct"] >= DISK_WARN_PCT
        if warn and not st.get("disk_warn"):
            alerts.append("🟡 ДИСК: / занят на %d%% (%s ГБ свободно)" % (d["pct"], d["avail_gb"]))
        elif not warn and st.get("disk_warn"):
            alerts.append("✅ Диск ок: %d%% занято" % d["pct"])
        st["disk_warn"] = warn
    # waltrace auto-restart counter
    cnt = int(watchdog_restart_count() or 0)
    prev_cnt = st.get("wd_count")
    if prev_cnt is not None and cnt > prev_cnt:
        alerts.append("⚠️ WALTRACE: watchdog сделал авто-рестарт ноды (всего: %d)\n%s"
                      % (cnt, last_watchdog_action()))
    st["wd_count"] = cnt

    if alerts:
        broadcast("\n\n".join(alerts))
    save_state(st)

# ---------- command handling ----------
def handle(msg):
    chat = msg.get("chat", {})
    cid = str(chat.get("id", ""))
    text = (msg.get("text") or "").strip()
    if not text.startswith("/"):
        return
    cmd = text.split()[0].lstrip("/").split("@")[0].lower()
    if cmd in ("id", "start"):
        send(cid, "chat_id: %s\nДобавь его в ALLOWED_CHAT_IDS в config.env и перезапусти сервис.\n\n%s"
                  % (cid, HELP if cid in ALLOWED else ""))
        return
    if cid not in ALLOWED:
        send(cid, "⛔ Не авторизован. Твой chat_id: %s" % cid)
        return
    if cmd == "status":   send(cid, fmt_status())
    elif cmd == "sync":   send(cid, fmt_sync())
    elif cmd == "disk":   send(cid, fmt_disk())
    elif cmd == "waltrace": send(cid, fmt_waltrace())
    elif cmd == "node":   send(cid, fmt_node())
    elif cmd == "help":   send(cid, HELP)
    else: send(cid, "Неизвестная команда. /help")

# ---------- main loop ----------
def main():
    if not TOKEN:
        sys.stderr.write("BOT_TOKEN не задан в %s — выход\n" % CFG_PATH); sys.exit(1)
    st = load_state()
    offset = st.get("offset", 0)
    # init service baselines so first tick doesn't false-alarm
    monitor(st)
    broadcast("🟢 monad-tg-bot запущен на %s. /help — команды." % HOST)
    last_check = time.time()
    while True:
        upd = tg("getUpdates", {"offset": offset, "timeout": 20}, timeout=35)
        if upd and upd.get("ok"):
            for u in upd["result"]:
                offset = u["update_id"] + 1
                st["offset"] = offset
                m = u.get("message") or u.get("edited_message")
                if m:
                    try: handle(m)
                    except Exception as e:
                        sys.stderr.write("handle error: %s\n" % e)
            save_state(st)
        if time.time() - last_check >= CHECK_INTERVAL:
            try: monitor(st)
            except Exception as e:
                sys.stderr.write("monitor error: %s\n" % e)
            last_check = time.time()

if __name__ == "__main__":
    main()
