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
# Логи watchdog-ов. Исторически бот знал только про waltrace-watchdog, но баг waltrace
# починен апстримом в monad 0.15.2, и этот watchdog снят с cron — его лог заморожен навсегда.
# Живой сейчас monad-stall-watchdog (зависание консенсуса, урок инцидента 2026-07-19), и его
# бот не видел вовсе. Смотрим оба: waltrace оставлен на случай возврата, ведущий — stall.
# Известные пути — как запасной вариант. Реальный список берём из cron: в природе встречаются
# как минимум три watchdog-а (monad-stall-watchdog, monad-waltrace-watchdog и monad-watchdog из
# репозитория monad-tools), и на второй нашей ноде работает именно третий, чей лог
# /var/log/monad-watchdog.log в захардкоженном списке отсутствовал — мониторинг watchdog там был
# мёртв целиком, при этом бот бодро рапортовал бы «watchdog молчит».
WATCHDOG_LOGS_DEFAULT = [
    "/var/log/monad-stall-watchdog.log",
    "/var/log/monad-waltrace-watchdog.log",
    "/var/log/monad-watchdog.log",
]

def _discover_watchdog_logs():
    """Пути логов watchdog из crontab root плюс известные значения по умолчанию."""
    found = []
    try:
        out = subprocess.run("crontab -l 2>/dev/null", shell=True, capture_output=True,
                             text=True, timeout=10).stdout
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "watchdog" not in line:
                continue
            m = re.search(r">>?\s*(/[^\s]+\.log)", line)
            if m:
                found.append(m.group(1))
    except Exception:
        pass
    for p in WATCHDOG_LOGS_DEFAULT:
        if p not in found:
            found.append(p)
    return found

WATCHDOG_LOGS = _discover_watchdog_logs()
WATCHDOG_LOG = WATCHDOG_LOGS_DEFAULT[1]   # обратная совместимость для /waltrace
# Действие watchdog связываем с рестартом ноды только если оно не старше этого.
WATCHDOG_FRESH_SEC = 900
RPC = "http://localhost:8080"
SERVICES = ["monad-bft", "monad-execution", "monad-rpc"]
# Сколько рестартов за один цикл мониторинга считать crash-loop'ом, а не единичным рестартом.
CRASHLOOP_RESTARTS = int(os.environ.get("CRASHLOOP_RESTARTS", "2"))
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
PENDING_MAX     = int(CFG.get("PENDING_MAX", "50"))      # максимум недоставленных алертов
PENDING_TTL_SEC = int(CFG.get("PENDING_TTL_SEC", "21600"))  # 6ч: позже алерт неактуален
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
            st.setdefault("pending_alerts", []).append({"cid": cid, "text": text, "ts": time.time()})
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
    # Cap + TTL: очередь не ограничивалась и не истекала. При долгой недоступности
    # Telegram она росла без предела, а retry_pending() внутри monitor() пытался до 20
    # отправок по 35с — до ~12 минут блокировки тика: не идёт long-poll (команды не
    # отвечают) и не выполняются проверки. Старые алерты приезжали через часы без
    # отметки времени и читались как свежая авария.
    keep = (left + pend[20:])[-PENDING_MAX:]
    now = time.time()
    st["pending_alerts"] = [a for a in keep
                            if now - float(a.get("ts") or now) <= PENDING_TTL_SEC]

# ---------- shell helpers ----------
def sh(cmd, timeout=10):
    """Вывод команды; "" и при ошибке, и при пустом выводе.

    Оставлено для мест, где различать эти два случая не нужно. Там, где нужно
    (состояние сервисов, диск, счётчики), используй sh_try().
    """
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=timeout).stdout.strip()
    except Exception:
        return ""

def sh_try(cmd, timeout=10):
    """(получилось, вывод). Отличает «команда не выполнилась» от «пустой вывод».

    Раньше всё шло через sh(), и таймаут systemd-dbus давал "" → svc_active()=False →
    ложный «🔴 СЕРВИС УПАЛ», а svc_start()="" ломал детект рестарта: пустое prev_start
    falsy, поэтому РЕАЛЬНЫЙ рестарт, совпавший с одной осечкой systemctl, не репортился
    вообще. Подвисший df (а он подвисает как раз при проблемах с IO) молча отключал
    проверку диска.
    Процессы убиваем группой: при shell=True с пайпами SIGKILL получал только sh,
    а journalctl/grep оставались осиротевшими и жгли IO рядом с нодой.
    """
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                           timeout=timeout, start_new_session=True)
        return (p.returncode == 0), p.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, ""
    except Exception:
        return False, ""

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
def svc_state(s):
    """(известно, active, sub) одним вызовом systemctl show.

    `activating` (и `auto-restart`) — это НЕ падение: на каждом штатном рестарте тик,
    попавший в фазу запуска, выдавал пару ложных алертов «СЕРВИС УПАЛ» → «восстановлен».
    Ночной пейдж на плановой операции быстро обесценивает сам алерт.
    """
    ok, out = sh_try("systemctl show %s -p ActiveState -p SubState --value" % s)
    if not ok or not out:
        return False, None, None
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    active = lines[0] if lines else ""
    sub = lines[1] if len(lines) > 1 else ""
    return True, active, sub

def svc_active(s):
    """True / False / None (не смогли узнать — состояние НЕ трогаем в monitor)."""
    known, active, sub = svc_state(s)
    if not known:
        return None
    if active == "active":
        return True
    if active in ("activating", "deactivating", "reloading") or sub == "auto-restart":
        return None          # переходная фаза — не считаем падением
    return False

def svc_start(s):
    ok, out = sh_try("systemctl show %s -p ExecMainStartTimestamp --value" % s)
    return out if ok else None

def svc_restarts(s):
    """NRestarts или None. Счётчик рестартов, выполненных самим systemd (Restart=on-failure).

    Зачем отдельно от svc_active: сервис в crash-loop проходит цикл
    active -> failed -> auto-restart -> activating -> active за считаные секунды. Тик
    мониторинга почти всегда застаёт его либо в переходной фазе (svc_active -> None,
    состояние не трогаем), либо в короткое окно active (-> True). Значение False не
    наблюдается практически никогда, поэтому «СЕРВИС УПАЛ» не срабатывает, и сервис,
    падающий по десять раз в минуту, выглядит совершенно здоровым. Растущий NRestarts —
    признак, который от фазы опроса не зависит.
    """
    ok, out = sh_try("systemctl show %s -p NRestarts --value" % s)
    if not ok or not out.strip().isdigit():
        return None
    return int(out.strip())

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
    # df подвисает именно при проблемах с IO, поэтому «не выполнилось» и «пусто» надо
    # различать: раньше оба давали pct=None и проверка диска молча выключалась.
    # По ВСЕМ локальным ФС, а не только по «/». Правила NodeDiskAlmostFull и NodeDiskFillingUp
    # внесены в PROM_SKIP_ALERTS, потому что бот «и так проверяет диск» — но проверял он одну
    # файловую систему, а правила покрывали все. Заполнение /var или отдельного тома оставалось
    # не покрытым ничем: Prometheus молчал по договорённости, бот туда не смотрел.
    # Берём самую заполненную ФС — она и определяет, когда звать людей.
    ok, out = sh_try("df -P -x tmpfs -x devtmpfs -x squashfs -x overlay -x efivarfs 2>/dev/null | tail -n +2")
    if not ok:
        return {"pct": None, "avail_gb": None, "known": False}
    worst = None
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 6:
            continue
        # Только ФС на реальном блочном устройстве. Псевдо-ФС дают бессмысленный процент:
        # /sys/firmware/efi/efivars на этой машине показывает 38% при размере в килобайты и
        # стала бы «самой заполненной», то есть источником вечного ложного алерта.
        if not parts[0].startswith("/dev/"):
            continue
        try:
            pct = int(parts[4].rstrip("%"))
            avail_gb = int(parts[3]) / 1024 / 1024
        except ValueError:
            continue
        if worst is None or pct > worst["pct"]:
            worst = {"pct": pct, "avail_gb": round(avail_gb, 1), "known": True, "mount": parts[5]}
    if worst:
        return worst
    parts = []
    if len(parts) >= 5:
        try:
            pct = int(parts[4].rstrip("%"))
            avail_gb = int(parts[3]) / 1024 / 1024
            return {"pct": pct, "avail_gb": round(avail_gb, 1), "known": True}
        except ValueError:
            pass
    return {"pct": None, "avail_gb": None, "known": False}

# ---------- мост Prometheus → Telegram ----------
# Правила в prometheus/alerts.yml исправно «фаерились», но получателя не было вовсе:
# ни alertmanager в стеке, ни провижининга нотификаций Grafana — /api/v1/alertmanagers
# отдавал active=0. Проще всего переиспользовать этот бот: доставка, повторы, очередь и
# список разрешённых чатов у него уже есть.
# Из config.env, как и всё остальное. Читать это только из os.environ было нельзя: в юните нет
# EnvironmentFile, поэтому у оператора не было ни одного штатного способа задать адрес — бот
# молча ходил бы на localhost:9090 и на хосте с Prometheus в другом месте не пересылал ничего.
PROM_URL = CFG.get("PROM_URL", os.environ.get("PROM_URL", "http://127.0.0.1:9090")).rstrip("/")
# Эти алерты бот проверяет сам, напрямую — пересылать их значило бы дублировать сообщения.
PROM_SKIP_ALERTS = {
    "MonadServiceDown", "MonadLocalRpcDown",
    # NodeDiskAlmostFull дублируется локальной проверкой (теперь по всем ФС) — пропускаем.
    # NodeDiskFillingUp НЕ пропускаем: это predict_linear, прогноз исчерпания за 4 часа.
    # Локального аналога у бота нет и быть не может — он видит только текущий процент.
    "NodeDiskAlmostFull",
}

def prometheus_alerts():
    """[(имя, severity, summary)] по фаерящимся алертам, либо None если Prometheus недоступен."""
    try:
        with urllib.request.urlopen(PROM_URL + "/api/v1/alerts", timeout=6) as r:
            d = json.load(r)
    except Exception:
        return None
    if d.get("status") != "success":
        return None
    out = []
    for a in d.get("data", {}).get("alerts", []):
        if a.get("state") != "firing":
            continue
        name = (a.get("labels") or {}).get("alertname", "?")
        if name in PROM_SKIP_ALERTS:
            continue
        sev = (a.get("labels") or {}).get("severity", "")
        summ = (a.get("annotations") or {}).get("summary", "")
        lbl = (a.get("labels") or {}).get("service") or (a.get("labels") or {}).get("mountpoint") or ""
        key = name + (":" + lbl if lbl else "")
        out.append((key, sev, summ))
    return out

def last_watchdog_action(logs=None):
    """Самая свежая строка действия среди ВСЕХ логов watchdog, без учёта времени.

    Паттерн намеренно без привязки к пунктуации: stall-watchdog пишет
    'ACTION — блок N не растёт M мин: restarting monad-bft', а прежний греп искал
    'ACTION: restarting' и не совпадал бы даже при верном пути к логу.
    """
    best, best_ts = "", -1
    for p in (logs or WATCHDOG_LOGS):
        out = sh("grep -E 'ACTION|ALERT|restarted' %s 2>/dev/null | tail -1" % p)
        if not out:
            continue
        ts = _line_epoch(out)
        if ts is None:          # без метки времени — берём, только если ничего лучше нет
            if best_ts < 0 and not best:
                best = out
            continue
        if ts > best_ts:
            best, best_ts = out, ts
    return best

def _line_epoch(line):
    """Epoch из ведущей метки 2026-07-29T22:55:01Z, либо None."""
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z", line or "")
    if not m:
        return None
    try:
        return calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None

def watchdog_needs_attention():
    """Строка ALERT от watchdog'а, если она свежая.

    Самое тяжёлое сообщение stall-watchdog — 'ALERT — stall N мин, но cooldown не истёк.
    НУЖНО ВМЕШАТЕЛЬСТВО ВРУЧНУЮ' — означает, что нода зависла ПОВТОРНО после авто-рестарта
    и автоматика сдалась. В Telegram оно не уходило никак: ложилось в лог, который никто
    не читает. Теперь это отдельный алерт.
    """
    for p in WATCHDOG_LOGS:
        out = sh("grep -E 'ALERT' %s 2>/dev/null | tail -1" % p)
        if not out:
            continue
        ts = _line_epoch(out)
        if ts is not None and time.time() - ts <= WATCHDOG_FRESH_SEC:
            return out
    return ""

def watchdog_configured():
    """Установлен ли вообще какой-нибудь watchdog на этом хосте.

    Хост без watchdog — законная конфигурация (проверено: на второй нашей ноде нет ни одного
    из логов). Без этой проверки бот вечно слал бы «watchdog молчит» там, где молчать нечему.
    """
    for p in WATCHDOG_LOGS:
        if sh("test -f %s && echo yes" % p) == "yes":
            return True
    # Лога может не быть, если watchdog поставлен, но ещё ни разу не отработал — смотрим cron.
    return sh("crontab -l 2>/dev/null | grep -c '^[^#].*monad-.*watchdog'").strip() not in ("", "0")

def watchdog_alive():
    """(состояние, возраст). Состояние: True живой / False молчит / None не установлен.

    Мёртвый watchdog — авария: его остановка была невидима, нода осталась бы без
    авто-восстановления молча. Но «не установлен» — это не авария, а конфигурация.
    """
    freshest = None
    for p in WATCHDOG_LOGS:
        out = sh("stat -c %%Y %s 2>/dev/null" % p)
        if out.isdigit():
            age = time.time() - int(out)
            if freshest is None or age < freshest:
                freshest = age
    if freshest is None:
        return (False, None) if watchdog_configured() else (None, None)
    return freshest <= 1800, freshest

def watchdog_action_age():
    """Возраст последнего действия watchdog в секундах, либо None если не разобрать."""
    ts = _line_epoch(last_watchdog_action())
    return None if ts is None else time.time() - ts

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
    """Сумма авто-рестартов по всем логам watchdog.

    Считаем по 'restarting' без привязки к пунктуации — прежний 'ACTION: restarting' не
    совпадал с форматом stall-watchdog. И возвращаем None при недоступности лога, а не "0":
    ноль здесь читался как «рестартов не было» и на следующем удачном тике давал ложный
    алерт «watchdog сделал авто-рестарт (всего: 33)» на полностью спокойной ноде.
    """
    total, seen = 0, False
    for p in WATCHDOG_LOGS:
        # Считаем СОБЫТИЯ, а не строки. Два независимых искажения жили в одной команде:
        #
        # 1. Двойной счёт. Один рестарт пишет обе строки — "restarting monad-bft to clear ..."
        #    и следом "restarted monad-bft — flood was ...". На нашем логе это ровно 33 и 33,
        #    то есть счётчик показывал 66 при 33 реальных рестартах.
        # 2. Инверсия. monad-watchdog логирует отказы от рестарта теми же словами:
        #    "Will NOT auto-restart", "NOT auto-restarting (would miss slots)",
        #    "Not auto-restarting on a blind 0", "NOT restarting". Каждый такой отказ —
        #    то есть срабатывание предохранителя — засчитывался как рестарт.
        #
        # Берём максимум из двух маркеров: watchdog может писать любой один из них, но
        # на одно событие не больше одного каждого.
        # Отказы ОТФИЛЬТРОВЫВАЮТСЯ до подсчёта, а не вычитаются после: строка
        # "NOT auto-restarting" сама содержит "restarting", поэтому вычитание уводило
        # результат в минус и обнуляло настоящие рестарты.
        neg = "not[[:space:]]+(auto-)?restart"
        n_ing = sh("grep -iE 'restarting' %s 2>/dev/null | grep -icvE '%s'" % (p, neg))
        n_ed  = sh("grep -iE 'restarted'  %s 2>/dev/null | grep -icvE '%s'" % (p, neg))
        if n_ing.isdigit() and n_ed.isdigit():
            total += max(int(n_ing), int(n_ed)); seen = True
    return total if seen else None

def node_version():
    v = sh("monad-node --version 2>/dev/null")
    m = re.search(r'"tag":"([^"]+)"', v)
    return m.group(1) if m else (v[:40] or "?")

# ---------- formatted reports (for commands) ----------
def fmt_status():
    L = ["📟 Monad node — %s" % HOST]
    _st = {s: svc_active(s) for s in SERVICES}
    bad = [s for s, v in _st.items() if v is False]
    unk = [s for s, v in _st.items() if v is None]
    if bad:
        L.append("Сервисы: 🔴 down: " + ", ".join(bad))
    elif unk:
        L.append("Сервисы: ❔ не удалось опросить: " + ", ".join(unk))
    else:
        L.append("Сервисы: ✅ все active")
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
        L.append("Диск %s: %s %d%% занято, %s ГБ свободно"
                 % (d.get("mount", "/"), icon, d["pct"], d["avail_gb"]))
    L.append("Версия: %s" % node_version())
    _wc = watchdog_restart_count()
    L.append("Авто-рестартов watchdog всего: %s" % ("н/д" if _wc is None else _wc))
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
    if age is not None and age > WATCHDOG_FRESH_SEC:
        last += "\n⚠️ запись старая (%s назад)" % (
            "%d ч" % int(age // 3600) if age >= 3600 else "%d мин" % int(age // 60))
    L = ["Watchdog-и ноды"]
    for p in WATCHDOG_LOGS:
        mt = sh("stat -c %%Y %s 2>/dev/null" % p)
        name = os.path.basename(p).replace(".log", "")
        if not mt.isdigit():
            L.append("  %s: лога нет" % name)
        else:
            a = int(time.time() - int(mt))
            state = "живой" if a <= 1800 else "МОЛЧИТ"
            L.append("  %s: %s, запись %d мин назад" % (name, state, a // 60))
    L.append("Всего авто-рестартов: %s" % ("н/д" if cnt is None else cnt))
    # Флуд waltrace оставлен как диагностика на случай регрессии: баг починен в 0.15.2
    # (cleanup-скрипт больше не удаляет wal_* из-под потока), сейчас должен быть 0.
    L.append("Флуд waltrace за 5 мин: %s (баг починен в 0.15.2, ожидается 0)"
             % sh("journalctl -u monad-bft --since '5 min ago' --no-pager 2>/dev/null | grep -c 'waltrace thread stopped'"))
    need = watchdog_needs_attention()
    if need:
        L.append("🔴 ТРЕБУЕТ ВМЕШАТЕЛЬСТВА:\n  %s" % need)
    L.append("Последнее действие:\n%s" % last)
    return "\n".join(L)

def fmt_node():
    up = sh("systemctl show monad-bft -p ExecMainStartTimestamp --value")
    # Префикс сжатого secp256k1-ключа — 02 ИЛИ 03, в зависимости от чётности Y. Шаблон ловил
    # только 02, то есть ровно половину сети: на живой ноде 94 против 196 уникальных пиров.
    peers = sh("journalctl -u monad-bft --since '1 min ago' --no-pager 2>/dev/null | grep -oE 'node_id\":\"0[23][0-9a-f]{6}' | sort -u | wc -l")
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
        # None = не смогли узнать (осечка systemctl) либо переходная фаза запуска.
        # Состояние НЕ трогаем и переходных алертов не выпускаем: иначе одна осечка давала
        # ложный «СЕРВИС УПАЛ», а на следующем тике — ложное «восстановлен», и, что хуже,
        # затирала prev_start, из-за чего настоящий рестарт оставался незамеченным.
        if active is None or start is None:
            continue
        prev_active = st.get("active_" + s)
        prev_start  = st.get("start_" + s)
        if prev_active is True and active is False:
            alerts.append("🔴 СЕРВИС УПАЛ: %s неактивен!" % s)
        elif prev_active is False and active is True:
            alerts.append("✅ Сервис восстановлен: %s снова active" % s)
        # crash-loop: NRestarts растёт, даже если фаза опроса всё время «здоровая»
        nr = svc_restarts(s)
        prev_nr = st.get("nrestarts_" + s)
        if nr is not None:
            if prev_nr is not None and nr > prev_nr:
                delta = nr - prev_nr
                if delta >= CRASHLOOP_RESTARTS:
                    alerts.append("🔴 CRASH-LOOP: %s перезапущен systemd %d раз(а) за цикл (всего %d)"
                                  % (s, delta, nr))
                else:
                    alerts.append("🟡 %s перезапущен systemd (%d раз всего)" % (s, nr))
            st["nrestarts_" + s] = nr
        if prev_start and start and start != prev_start and active:
            wd = recent_watchdog_action()
            tag = " (watchdog/waltrace — ожидаемо)" if (s == "monad-bft" and wd and "restart" in wd.lower()) else ""
            alerts.append("🔄 РЕСТАРТ: %s перезапущен%s\nстарт: %s" % (s, tag, start))
        st["active_" + s] = active
        st["start_" + s]  = start
    # sync lag
    s = get_sync()
    rpc_dead = s["height"] is None
    if rpc_dead and not st.get("rpc_dead"):
        alerts.append("🔴 RPC :8080 не отвечает")
    elif not rpc_dead and st.get("rpc_dead"):
        alerts.append("✅ RPC :8080 снова отвечает")
    st["rpc_dead"] = rpc_dead
    # lag берётся только из eth_getBlockByNumber. Его таймаут при живом eth_blockNumber давал
    # lag=None → behind=False → «✅ Синк восстановлен (lag None)» посреди реального отставания,
    # состояние сбрасывалось, и на следующем тике снова «🟡 ОТСТАВАНИЕ». Пинг-понг ложных
    # восстановлений на весь инцидент. Нет данных — состояние не трогаем.
    if s["lag"] is None:
        pass
    else:
        behind = s["lag"] > SYNC_LAG_WARN
        if behind and not st.get("behind"):
            alerts.append("🟡 ОТСТАВАНИЕ СИНКА: lag %ds (блок %s)" % (s["lag"], s["height"]))
        elif not behind and st.get("behind"):
            alerts.append("✅ Синк восстановлен (lag %ds)" % s["lag"])
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
            alerts.append("🟡 ДИСК: %s занят на %d%% (%s ГБ свободно)"
                          % (d.get("mount", "/"), d["pct"], d["avail_gb"]))
        elif not warn and st.get("disk_warn"):
            alerts.append("✅ Диск ок: %d%% занято" % d["pct"])
        st["disk_warn"] = warn

    # Пересылка алертов Prometheus. Только транзишны: новый фаерящийся алерт и его снятие,
    # иначе на каждом тике летели бы повторы.
    pa = prometheus_alerts()
    if pa is not None:
        cur = {k: (sev, summ) for k, sev, summ in pa}
        prev = set(st.get("prom_alerts") or [])
        for k in sorted(set(cur) - prev):
            sev, summ = cur[k]
            icon = "🔴" if sev == "critical" else "🟡"
            alerts.append("%s PROMETHEUS [%s]: %s\n%s" % (icon, sev or "?", k, summ))
        for k in sorted(prev - set(cur)):
            alerts.append("✅ PROMETHEUS: снят алерт %s" % k)
        st["prom_alerts"] = sorted(cur)
        if st.get("prom_down"):
            alerts.append("✅ Prometheus снова отвечает")
            st["prom_down"] = False
    elif not st.get("prom_down"):
        # Недоступный Prometheus = мы ослепли по половине сигналов. Раньше об этом никто
        # не узнавал, потому что бот про Prometheus вообще не знал.
        alerts.append("🟡 Prometheus не отвечает — алерты по метрикам не отслеживаются")
        st["prom_down"] = True

    # Счётчик авто-рестартов watchdog. None = лог недоступен: состояние НЕ трогаем, иначе
    # обнуление счётчика на следующем удачном тике даёт ложный алерт про авто-рестарт.
    cnt = watchdog_restart_count()
    if cnt is not None:
        # Семантика счётчика изменилась (теперь суммируются оба лога и паттерн шире), поэтому
        # сохранённое старое значение несопоставимо: без ре-базирования первый же запуск после
        # обновления выдал бы ложный «watchdog сделал авто-рестарт» на скачке 33 → 66.
        if st.get("wd_count_schema") != 2:
            st["wd_count"] = cnt
            st["wd_count_schema"] = 2
        prev_cnt = st.get("wd_count")
        if prev_cnt is not None and cnt > prev_cnt:
            alerts.append("⚠️ WATCHDOG: сделан авто-рестарт ноды (всего: %d)\n%s"
                          % (cnt, last_watchdog_action()))
        st["wd_count"] = cnt

    # Самое тяжёлое сообщение watchdog'а: нода зависла ПОВТОРНО после авто-рестарта и
    # автоматика сдалась. Раньше оно вообще не покидало лог.
    need = watchdog_needs_attention()
    if need and st.get("wd_alert_line") != need:
        alerts.append("🔴🔴 WATCHDOG СДАЛСЯ — нужно вмешательство вручную:\n%s" % need)
        st["wd_alert_line"] = need

    # Мёртвый watchdog = нода без авто-восстановления. Раньше его остановка была невидима.
    alive, wd_age = watchdog_alive()
    if alive is None:
        # Watchdog на этом хосте не установлен — тишина ожидаема, состояние не трогаем.
        pass
    elif not alive and not st.get("wd_dead"):
        alerts.append("🟡 WATCHDOG молчит: логи не обновлялись %s — проверь cron"
                      % ("никогда" if wd_age is None else "%d мин" % int(wd_age // 60)))
        st["wd_dead"] = True
    elif alive and st.get("wd_dead"):
        alerts.append("✅ WATCHDOG снова пишет в лог")
        st["wd_dead"] = False

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
