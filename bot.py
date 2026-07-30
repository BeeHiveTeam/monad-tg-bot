#!/usr/bin/env python3
"""
monad-tg-bot — Telegram monitoring/ops bot for the Monad node on ovh-117-52.

Pure stdlib (urllib, json, subprocess). Single-threaded loop:
  - long-polls Telegram getUpdates for commands (/status, /sync, /disk, /waltrace, /node, /help)
  - every CHECK_INTERVAL runs health checks and pushes alerts on state change

Config: /opt/monad-tg-bot/config.env   State: /opt/monad-tg-bot/state.json
Runs as root (needs systemctl/journalctl/df and the root-owned watchdog log).
"""
import calendar, json, os, re, ssl, subprocess, sys, time, urllib.parse, urllib.request

CFG_PATH   = "/opt/monad-tg-bot/config.env"
STATE_PATH = "/opt/monad-tg-bot/state.json"
WATCHDOG_LOG = "/var/log/monad-waltrace-watchdog.log"
# Действие watchdog связываем с рестартом ноды только если оно не старше этого.
WATCHDOG_FRESH_SEC = 900
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
STALL_TICKS      = int(CFG.get("STALL_TICKS", "5"))       # тиков без роста блока до алерта (5*60с=5мин)
REALERT_SEC      = int(CFG.get("REALERT_SEC", "1800"))    # повтор алерта критического состояния, сек
API = "https://api.telegram.org/bot%s/" % TOKEN
SSLCTX = ssl.create_default_context()

# ---------- telegram api ----------
def tg(method, params=None, timeout=35):
    data = urllib.parse.urlencode(params or {}).encode()
    req = urllib.request.Request(API + method, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=SSLCTX) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        # тело ошибки Telegram содержит error_code/description — отдаём наверх,
        # чтобы отличать "колбэк протух" (400) от сетевых сбоев (None)
        try:
            body = json.load(e)
        except Exception:
            body = {"ok": False, "error_code": e.code}
        sys.stderr.write("tg %s error: %s %s\n" % (method, e, body.get("description", "")))
        sys.stderr.flush()
        return body
    except Exception as e:
        sys.stderr.write("tg %s error: %s\n" % (method, e)); sys.stderr.flush()
        return None

KEYBOARD = json.dumps({"inline_keyboard": [
    [{"text": "📟 Статус", "callback_data": "status"}, {"text": "🔄 Синк", "callback_data": "sync"}],
    [{"text": "💾 Диск", "callback_data": "disk"}, {"text": "🐕 Waltrace", "callback_data": "waltrace"}],
    [{"text": "🖥 Нода", "callback_data": "node"}, {"text": "❓ Помощь", "callback_data": "help"}],
]})

def send(chat_id, text, kb=False):
    # plain text, split if very long; True если все части доставлены
    # kb=True — прикрепить инлайн-кнопки к последней части
    ok = True
    chunks = [text[i:i+3900] for i in range(0, len(text), 3900)]
    for n, chunk in enumerate(chunks):
        params = {"chat_id": chat_id, "text": chunk,
                  "disable_web_page_preview": "true"}
        if kb and n == len(chunks) - 1:
            params["reply_markup"] = KEYBOARD
        r = tg("sendMessage", params)
        if not (r and r.get("ok")):
            ok = False
    return ok

def broadcast(text, st=None):
    """Шлёт алерт всем; недоставленное кладёт в очередь st для ретрая."""
    for cid in ALLOWED:
        if send(cid, text, kb=True):
            sys.stderr.write("alert delivered to %s: %s\n" % (cid, text.splitlines()[0][:60]))
        elif st is not None:
            st.setdefault("pending_alerts", []).append({"cid": cid, "text": text})
            sys.stderr.write("alert QUEUED (send failed) for %s\n" % cid)

def retry_pending(st):
    """Ретрай недоставленных алертов (переживают сетевые дыры к Telegram)."""
    pend = st.get("pending_alerts") or []
    if not pend:
        return
    left = []
    for a in pend[:20]:
        if send(a["cid"], "(повтор) " + a["text"], kb=True):
            sys.stderr.write("queued alert delivered to %s\n" % a["cid"])
        else:
            left.append(a)
    st["pending_alerts"] = left + pend[20:]

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
    """Последняя строка действия watchdog (для показа в /waltrace), без учёта времени."""
    out = sh("grep -E 'ACTION|restarted' %s 2>/dev/null | tail -1" % WATCHDOG_LOG)
    return out

def watchdog_action_age():
    """Возраст последнего действия watchdog в секундах, либо None если не разобрать."""
    out = last_watchdog_action()
    if not out:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z", out)
    if not m:
        return None
    try:
        return time.time() - calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None

def recent_watchdog_action(max_age=WATCHDOG_FRESH_SEC):
    """То же, но пусто если действие СТАРОЕ.

    Атрибуция рестарта раньше смотрела на последнюю строку лога без проверки
    времени. Когда waltrace-watchdog отключили, в логе навсегда осталась старая
    строка 'restarted monad-bft', и ЛЮБОЙ последующий рестарт ноды помечался
    '(watchdog/waltrace — ожидаемо)' — то есть настоящая нештатная перезагрузка
    выглядела безобидной.
    """
    age = watchdog_action_age()
    if age is None or age > max_age:
        return ""
    return last_watchdog_action()

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
    age = watchdog_action_age()
    # Без отметки возраста замороженный лог (watchdog снят с cron после того,
    # как баг waltrace починили апстримом в monad 0.15.2) читается как свежий.
    if age is not None and age > WATCHDOG_FRESH_SEC:
        last += "\n⚠️ запись старая (%d ч назад) — watchdog, вероятно, отключён" % int(age // 3600)
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
            wd = recent_watchdog_action()
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
    # БЛОК НЕ РАСТЁТ (урок инцидента 2026-07-19: 5ч фриза с одним тихим алертом)
    h = s["height"]
    if h is not None:
        if st.get("last_height") == h:
            st["stall_ticks"] = st.get("stall_ticks", 0) + 1
        else:
            if st.get("stalled"):
                alerts.append("✅ Блоки снова растут: %s" % h)
            st["stall_ticks"] = 0
            st["stalled"] = False
        if st.get("stall_ticks", 0) >= STALL_TICKS and not st.get("stalled"):
            alerts.append("🔴 БЛОКИ НЕ РАСТУТ: высота застряла на %s (~%d мин). Consensus стоит!"
                          % (h, st["stall_ticks"] * CHECK_INTERVAL // 60))
            st["stalled"] = True
        st["last_height"] = h
    # РЕ-АЛЕРТ критических состояний каждые REALERT_SEC (не молчать часами!)
    now = time.time()
    crit = []
    if st.get("stalled"):  crit.append("🔴 ВСЁ ЕЩЁ СТОИТ: блок %s не растёт" % st.get("last_height"))
    if st.get("rpc_dead"): crit.append("🔴 RPC всё ещё не отвечает")
    for svc in SERVICES:
        if st.get("active_" + svc) is False:
            crit.append("🔴 %s всё ещё down" % svc)
    if crit:
        if now - st.get("last_realert", 0) >= REALERT_SEC:
            alerts.append("⏰ НАПОМИНАНИЕ (повтор каждые %d мин):\n" % (REALERT_SEC // 60)
                          + "\n".join(crit))
            st["last_realert"] = now
    else:
        st["last_realert"] = 0
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
        broadcast("\n\n".join(alerts), st)
    retry_pending(st)
    save_state(st)

# ---------- command handling ----------
CB_SEEN = {}  # (cid:data) -> ts последнего исполнения кнопки, для дебаунса

def dispatch(cid, cmd):
    # общий диспетчер для /команд и нажатий кнопок
    if cmd == "status":   send(cid, fmt_status(), kb=True)
    elif cmd == "sync":   send(cid, fmt_sync(), kb=True)
    elif cmd == "disk":   send(cid, fmt_disk(), kb=True)
    elif cmd == "waltrace": send(cid, fmt_waltrace(), kb=True)
    elif cmd == "node":   send(cid, fmt_node(), kb=True)
    elif cmd == "help":   send(cid, HELP, kb=True)
    else: send(cid, "Неизвестная команда. /help", kb=True)

def handle(msg):
    chat = msg.get("chat", {})
    cid = str(chat.get("id", ""))
    text = (msg.get("text") or "").strip()
    if not text.startswith("/"):
        return
    cmd = text.split()[0].lstrip("/").split("@")[0].lower()
    if cmd in ("id", "start"):
        send(cid, "chat_id: %s\nДобавь его в ALLOWED_CHAT_IDS в config.env и перезапусти сервис.\n\n%s"
                  % (cid, HELP if cid in ALLOWED else ""), kb=cid in ALLOWED)
        return
    if cid not in ALLOWED:
        send(cid, "⛔ Не авторизован. Твой chat_id: %s" % cid)
        return
    dispatch(cid, cmd)

def handle_callback(cb):
    # нажатие инлайн-кнопки: гасим "часики" и выполняем как команду
    # message может отсутствовать (недоступное сообщение) — fallback на id нажавшего
    cid = str((cb.get("message") or {}).get("chat", {}).get("id", "")
              or cb.get("from", {}).get("id", ""))
    t0 = time.time()
    r = tg("answerCallbackQuery", {"callback_query_id": cb.get("id", "")}, timeout=15)
    sys.stderr.write("callback data=%s answer_ok=%s answer_took=%.1fs\n"
                     % (cb.get("data"), (r or {}).get("ok"), time.time() - t0))
    if cid not in ALLOWED:
        send(cid, "⛔ Не авторизован. Твой chat_id: %s" % cid)
        return
    # дебаунс: та же кнопка не чаще раза в 10с — гасит спам при серии нажатий
    # и при пачке протухших колбэков после залипания доставки. Отвечаем ВСЕГДА,
    # даже если "часики" погасить не удалось (Telegram бывает тормозит сам ответ
    # на answerCallbackQuery на 10-15с и потом отдаёт "query is too old").
    data = (cb.get("data") or "").strip().lower()
    key = cid + ":" + data
    now = time.time()
    if now - CB_SEEN.get(key, 0) < 10:
        sys.stderr.write("debounce %s\n" % key)
        return
    CB_SEEN[key] = now
    t1 = time.time()
    dispatch(cid, data)
    sys.stderr.write("dispatch %s took=%.1fs\n" % (cb.get("data"), time.time() - t1))

# ---------- main loop ----------
def main():
    if not TOKEN:
        sys.stderr.write("BOT_TOKEN не задан в %s — выход\n" % CFG_PATH); sys.exit(1)
    st = load_state()
    offset = st.get("offset", 0)
    # init service baselines so first tick doesn't false-alarm
    monitor(st)
    broadcast("🤖 monad-tg-bot запущен на %s. /help — команды." % HOST)
    last_check = time.time()
    while True:
        tp = time.time()
        # allowed_updates явно: настройка ПЕРСИСТЕНТНА на стороне Telegram —
        # без callback_query в списке нажатия кнопок молча не доставляются
        upd = tg("getUpdates", {"offset": offset, "timeout": 20,
                                "allowed_updates": '["message","edited_message","callback_query"]'},
                 timeout=35)
        n = len(upd["result"]) if (upd and upd.get("ok")) else -1
        if n != 0:
            sys.stderr.write("poll took=%.1fs updates=%d\n" % (time.time() - tp, n))
        if upd and upd.get("ok"):
            for u in upd["result"]:
                offset = u["update_id"] + 1
                st["offset"] = offset
                m = u.get("message") or u.get("edited_message")
                if m:
                    try: handle(m)
                    except Exception as e:
                        sys.stderr.write("handle error: %s\n" % e)
                cb = u.get("callback_query")
                if cb:
                    try: handle_callback(cb)
                    except Exception as e:
                        sys.stderr.write("callback error: %s\n" % e)
            save_state(st)
        elif upd is not None:
            time.sleep(2)  # HTTP-ошибка API (401/409/429): не долбим в busy-loop
        if time.time() - last_check >= CHECK_INTERVAL:
            try: monitor(st)
            except Exception as e:
                sys.stderr.write("monitor error: %s\n" % e)
            last_check = time.time()

if __name__ == "__main__":
    main()
