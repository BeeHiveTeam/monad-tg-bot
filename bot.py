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

# --- IPv4 вперёд ---------------------------------------------------------------
# api.telegram.org отдаёт и A, и AAAA. На хосте с глобальным IPv6, у которого маршрут до
# Telegram не работает (наш случай на OVH), getaddrinfo ставит IPv6 первым, а urllib, в отличие
# от curl, не умеет Happy Eyeballs: он перебирает адреса по порядку и на каждом ждёт ПОЛНЫЙ
# таймаут. Замер: 2 запроса из 20 висли 5.7 и 24.7 с, при принудительном IPv4 — 0 из 20.
# Наружу это выглядело как «кнопка не отвечает»: висел sendMessage, а не обработчик.
# Не фильтруем, а переупорядочиваем — на хосте только с IPv6 список останется прежним.
if os.environ.get("PREFER_IPV4", "1") != "0":
    import socket as _socket
    _orig_getaddrinfo = _socket.getaddrinfo

    def _ipv4_first(*args, **kwargs):
        res = _orig_getaddrinfo(*args, **kwargs)
        return sorted(res, key=lambda r: 0 if r[0] == _socket.AF_INET else 1)

    _socket.getaddrinfo = _ipv4_first

# Пути переопределяемы окружением: иначе бота нельзя ни прогнать на тестовой конфигурации,
# ни поставить в раскладку, отличную от нашей. Умолчания прежние.
CFG_PATH   = os.environ.get("MONAD_TG_BOT_CONFIG", "/opt/monad-tg-bot/config.env")
STATE_PATH = os.environ.get("MONAD_TG_BOT_STATE", "/opt/monad-tg-bot/state.json")
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
# Порог флуда waltrace за 5 мин. Штатно 0; на регрессии 2026-07-31 было ~67 000.
WALTRACE_FLOOD_5MIN = int(os.environ.get("WALTRACE_FLOOD_5MIN", "100"))
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
# UI language: en (default), ru or de. Also switchable at runtime with the 🌐 button or /lang.
_cfg_lang = CFG.get("LANG", "en").strip().lower()
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

# ── i18n ──────────────────────────────────────────────────────────────────────
# One bot, one operator chat, so language is a single setting rather than per-user. Default
# English; LANG=en|ru|de in config.env sets it, the 🌐 button and /lang cycle at runtime.
# Strings keep %s formatting, so tr("key") % args works exactly like the literals it replaced.
DEFAULT_LANG = _cfg_lang if _cfg_lang in ("en", "ru", "de") else "en"
_lang = DEFAULT_LANG

T = {
  # buttons — the language button shows the CURRENT language, not the next one
  "b_status": {"en": "📟 Status",  "ru": "📟 Статус",   "de": "📟 Status"},
  "b_sync":   {"en": "🔄 Sync",    "ru": "🔄 Синк",     "de": "🔄 Sync"},
  "b_disk":   {"en": "💾 Disk",    "ru": "💾 Диск",     "de": "💾 Platte"},
  "b_wal":    {"en": "🐕 Waltrace","ru": "🐕 Waltrace", "de": "🐕 Waltrace"},
  "b_node":   {"en": "🖥 Node",    "ru": "🖥 Нода",     "de": "🖥 Node"},
  "b_help":   {"en": "❓ Help",     "ru": "❓ Помощь",   "de": "❓ Hilfe"},
  "b_lang":   {"en": "🌐 EN",      "ru": "🌐 RU",       "de": "🌐 DE"},
  "b_val":    {"en": "🏛 Validator", "ru": "🏛 Валидатор", "de": "🏛 Validator"},

  # validator
  "a_val_stake": {"en": "🏛 Validator stake changed: %s%s MON (%s -> %s)",
                  "ru": "🏛 Стейк валидатора изменился: %s%s MON (%s -> %s)",
                  "de": "🏛 Validator-Stake geändert: %s%s MON (%s -> %s)"},
  "a_val_comm":  {"en": "⚠️ Validator commission changed: %.2f%% -> %.2f%%. Did you do this?",
                  "ru": "⚠️ Комиссия валидатора изменилась: %.2f%% -> %.2f%%. Это делали вы?",
                  "de": "⚠️ Validator-Provision geändert: %.2f%% -> %.2f%%. Waren Sie das?"},
  "a_val_in":    {"en": "✅ Validator ENTERED the active set (self stake %.2f%%)",
                  "ru": "✅ Валидатор ВОШЁЛ в активный сет (свой стейк %.2f%%)",
                  "de": "✅ Validator IST im aktiven Set (Eigenanteil %.2f%%)"},
  "a_val_out":   {"en": "🔴 Validator LEFT the active set — it no longer signs blocks",
                  "ru": "🔴 Валидатор ВЫШЕЛ из активного сета — блоки больше не подписываются",
                  "de": "🔴 Validator hat das aktive Set VERLASSEN — signiert keine Blöcke mehr"},
  "a_val_id":    {"en": "⚠️ Validator id changed: #%s -> #%s. The node key was re-registered.",
                  "ru": "⚠️ Изменился id валидатора: #%s -> #%s. Ключ ноды перерегистрирован.",
                  "de": "⚠️ Validator-ID geändert: #%s -> #%s. Node-Key neu registriert."},
  "a_val_gone":  {"en": "🔴 Validator #%s is no longer in the registry under this node's key",
                  "ru": "🔴 Валидатора #%s больше нет в реестре под ключом этой ноды",
                  "de": "🔴 Validator #%s ist nicht mehr mit dem Key dieser Node registriert"},
  "vl_title":  {"en": "🏛 Validator #%s", "ru": "🏛 Валидатор #%s", "de": "🏛 Validator #%s"},
  "vl_stake":  {"en": "Self stake: %s MON", "ru": "Свой стейк: %s MON", "de": "Eigener Stake: %s MON"},
  "vl_comm":   {"en": "Commission: %.2f%%", "ru": "Комиссия: %.2f%%", "de": "Provision: %.2f%%"},
  "vl_auth":   {"en": "Auth address: %s", "ru": "Адрес auth: %s", "de": "Auth-Adresse: %s"},
  "vl_set_in": {"en": "Active set: ✅ in (self stake %.2f%%)", "ru": "Активный сет: ✅ внутри (свой стейк %.2f%%)", "de": "Aktives Set: ✅ drin (Eigenanteil %.2f%%)"},
  "vl_set_out":{"en": "Active set: — registered, not yet in", "ru": "Активный сет: — зарегистрирован, ещё не вошли", "de": "Aktives Set: — registriert, noch nicht drin"},
  "vl_set_unknown": {"en": "Active set: ❔ could not read self_stake_bps", "ru": "Активный сет: ❔ не смог прочитать self_stake_bps", "de": "Aktives Set: ❔ self_stake_bps nicht lesbar"},
  "vl_none":   {"en": "🏛 Not a validator — this node's key is in none of the %s registry entries (full node).", "ru": "🏛 Не валидатор — ключа этой ноды нет ни в одной из %s записей реестра (фулл-нода).", "de": "🏛 Kein Validator — der Schlüssel dieser Node fehlt in allen %s Registry-Einträgen (Full Node)."},
  "vl_unknown":{"en": "🏛 Validator status unknown — %s. This is not the same as \"not a validator\".", "ru": "🏛 Статус валидатора неизвестен — %s. Это НЕ то же, что «не валидатор».", "de": "🏛 Validator-Status unbekannt — %s. Das heißt nicht \"kein Validator\"."},
  "vl_why_otel":  {"en": "the metrics collector did not answer", "ru": "коллектор метрик не ответил", "de": "der Metrik-Collector antwortete nicht"},
  "vl_why_nolabel": {"en": "the collector publishes no secp_key label", "ru": "коллектор не публикует метку secp_key", "de": "der Collector veröffentlicht kein secp_key-Label"},
  "vl_why_many":  {"en": "the collector reports more than one secp_key", "ru": "коллектор отдаёт больше одного secp_key", "de": "der Collector meldet mehr als einen secp_key"},
  "vl_why_rpc":   {"en": "the staking precompile did not answer", "ru": "прекомпайл стейкинга не ответил", "de": "das Staking-Precompile antwortete nicht"},
  "vl_why_budget":{"en": "the registry scan hit its call budget", "ru": "обход реестра упёрся в бюджет вызовов", "de": "der Registry-Scan erreichte sein Aufrufbudget"},

  # sync state
  "s_nolag":  {"en": "❔ lag not measured", "ru": "❔ lag не измерен", "de": "❔ Lag nicht messbar"},
  "s_behind": {"en": "🔴 BEHIND",           "ru": "🔴 ОТСТАЁТ",       "de": "🔴 ZURÜCK"},
  "s_sync":   {"en": "🟡 syncing",          "ru": "🟡 syncing",       "de": "🟡 syncing"},
  "s_ok":     {"en": "✅ in sync",           "ru": "✅ в синке",       "de": "✅ synchron"},

  # /status
  "st_down":  {"en": "Services: 🔴 down: ",  "ru": "Сервисы: 🔴 down: ",  "de": "Dienste: 🔴 down: "},
  "st_unk":   {"en": "Services: ❔ could not query: ", "ru": "Сервисы: ❔ не удалось опросить: ", "de": "Dienste: ❔ nicht abfragbar: "},
  "st_ok":    {"en": "Services: ✅ all active", "ru": "Сервисы: ✅ все active", "de": "Dienste: ✅ alle aktiv"},
  "st_sync":  {"en": "Sync: %s  block %s  lag %s", "ru": "Синк: %s  блок %s  lag %s", "de": "Sync: %s  Block %s  Lag %s"},
  "st_norpc": {"en": "Sync: 🔴 RPC not responding", "ru": "Синк: 🔴 RPC не отвечает", "de": "Sync: 🔴 RPC antwortet nicht"},
  "st_disk":  {"en": "Disk %s: %s %d%% used, %s GB free", "ru": "Диск %s: %s %d%% занято, %s ГБ свободно", "de": "Platte %s: %s %d%% belegt, %s GB frei"},
  "st_ver":   {"en": "Version: %s", "ru": "Версия: %s", "de": "Version: %s"},
  "st_wdr":   {"en": "Watchdog auto-restarts total: %s", "ru": "Авто-рестартов watchdog всего: %s", "de": "Watchdog-Neustarts gesamt: %s"},
  "na":       {"en": "n/a", "ru": "н/д", "de": "k.A."},

  # /sync /disk /node
  "sy_norpc": {"en": "🔴 RPC :8080 not responding", "ru": "🔴 RPC :8080 не отвечает", "de": "🔴 RPC :8080 antwortet nicht"},
  "sy_body":  {"en": "Sync %s\nblock: %s\nlag: %s\neth_syncing: %s", "ru": "Синк %s\nblock: %s\nlag: %s\neth_syncing: %s", "de": "Sync %s\nBlock: %s\nLag: %s\neth_syncing: %s"},
  "dk_body":  {"en": "Disk /: %s\n%s", "ru": "Диск /: %s\n%s", "de": "Platte /: %s\n%s"},
  "dk_used":  {"en": "%d%% used, %s GB free", "ru": "%d%% занято, %s ГБ свободно", "de": "%d%% belegt, %s GB frei"},
  "nd_body":  {"en": "Node %s\nVersion: %s\nmonad-bft since: %s\nUnique peers in log/min: %s", "ru": "Нода %s\nВерсия: %s\nmonad-bft с: %s\nУникальных пиров в логе/мин: %s", "de": "Node %s\nVersion: %s\nmonad-bft seit: %s\nEindeutige Peers im Log/Min: %s"},

  # /waltrace
  "wd_title": {"en": "Node watchdogs", "ru": "Watchdog-и ноды", "de": "Node-Watchdogs"},
  "wd_nolog": {"en": "  %s: no log", "ru": "  %s: лога нет", "de": "  %s: kein Log"},
  "wd_entry": {"en": "  %s: %s, written %d min ago", "ru": "  %s: %s, запись %d мин назад", "de": "  %s: %s, Eintrag vor %d Min"},
  "wd_alive": {"en": "alive", "ru": "живой", "de": "aktiv"},
  "wd_quiet": {"en": "SILENT", "ru": "МОЛЧИТ", "de": "STUMM"},
  "wd_total": {"en": "Auto-restarts total: %s", "ru": "Всего авто-рестартов: %s", "de": "Neustarts gesamt: %s"},
  "wd_flood": {"en": "waltrace flood in 5 min: %s (fixed in 0.15.2, expect 0)", "ru": "Флуд waltrace за 5 мин: %s (баг починен в 0.15.2, ожидается 0)", "de": "waltrace-Flut in 5 Min: %s (in 0.15.2 behoben, erwartet 0)"},
  "wd_need":  {"en": "🔴 NEEDS ATTENTION:\n  %s", "ru": "🔴 ТРЕБУЕТ ВМЕШАТЕЛЬСТВА:\n  %s", "de": "🔴 EINGRIFF NÖTIG:\n  %s"},
  "wd_last":  {"en": "Last action:\n%s", "ru": "Последнее действие:\n%s", "de": "Letzte Aktion:\n%s"},
  "wd_none":  {"en": "no entries", "ru": "нет записей", "de": "keine Einträge"},
  "wd_stale": {"en": "\n⚠️ entry is old (%s ago)", "ru": "\n⚠️ запись старая (%s назад)", "de": "\n⚠️ Eintrag ist alt (vor %s)"},
  "t_hours":  {"en": "%d h", "ru": "%d ч", "de": "%d Std"},
  "t_never":  {"en": "never", "ru": "никогда", "de": "nie"},
  "t_mins":   {"en": "%d min", "ru": "%d мин", "de": "%d Min"},

  # alerts
  "a_svc_down": {"en": "🔴 SERVICE DOWN: %s is not active!", "ru": "🔴 СЕРВИС УПАЛ: %s неактивен!", "de": "🔴 DIENST AUSGEFALLEN: %s ist nicht aktiv!"},
  "a_svc_up":   {"en": "✅ Service recovered: %s is active again", "ru": "✅ Сервис восстановлен: %s снова active", "de": "✅ Dienst wiederhergestellt: %s ist wieder aktiv"},
  "a_svc_still":{"en": "🔴 %s still down", "ru": "🔴 %s всё ещё down", "de": "🔴 %s weiterhin ausgefallen"},
  "a_restart":  {"en": "🔄 RESTART: %s restarted%s\nstarted: %s", "ru": "🔄 РЕСТАРТ: %s перезапущен%s\nстарт: %s", "de": "🔄 NEUSTART: %s neu gestartet%s\nStart: %s"},
  "a_expected": {"en": " (watchdog/waltrace — expected)", "ru": " (watchdog/waltrace — ожидаемо)", "de": " (Watchdog/waltrace — erwartet)"},
  "a_crashloop":{"en": "🔴 CRASH-LOOP: %s restarted by systemd %d time(s) this cycle (total %d)", "ru": "🔴 CRASH-LOOP: %s перезапущен systemd %d раз(а) за цикл (всего %d)", "de": "🔴 CRASH-LOOP: %s von systemd %d mal in diesem Zyklus neu gestartet (gesamt %d)"},
  "a_sysrest":  {"en": "🟡 %s restarted by systemd (%d total)", "ru": "🟡 %s перезапущен systemd (%d раз всего)", "de": "🟡 %s von systemd neu gestartet (%d gesamt)"},
  "a_rpc_down": {"en": "🔴 RPC :8080 not responding", "ru": "🔴 RPC :8080 не отвечает", "de": "🔴 RPC :8080 antwortet nicht"},
  "a_rpc_still":{"en": "🔴 RPC still not responding", "ru": "🔴 RPC всё ещё не отвечает", "de": "🔴 RPC antwortet weiterhin nicht"},
  "a_rpc_up":   {"en": "✅ RPC :8080 responding again", "ru": "✅ RPC :8080 снова отвечает", "de": "✅ RPC :8080 antwortet wieder"},
  "a_stuck":    {"en": "🔴 BLOCKS NOT ADVANCING: height stuck at %s (~%d min). Consensus is down!", "ru": "🔴 БЛОКИ НЕ РАСТУТ: высота застряла на %s (~%d мин). Consensus стоит!", "de": "🔴 BLÖCKE STEHEN: Höhe bei %s festgefahren (~%d Min). Consensus steht!"},
  "a_stuck_still":{"en": "🔴 STILL STUCK: block %s is not advancing", "ru": "🔴 ВСЁ ЕЩЁ СТОИТ: блок %s не растёт", "de": "🔴 STEHT WEITERHIN: Block %s wächst nicht"},
  "a_stuck_ok": {"en": "✅ Blocks advancing again: %s", "ru": "✅ Блоки снова растут: %s", "de": "✅ Blöcke wachsen wieder: %s"},
  "a_lag":      {"en": "🟡 SYNC LAG: %ds behind (block %s)", "ru": "🟡 ОТСТАВАНИЕ СИНКА: lag %ds (блок %s)", "de": "🟡 SYNC-RÜCKSTAND: %ds (Block %s)"},
  "a_lag_ok":   {"en": "✅ Sync recovered (lag %ds)", "ru": "✅ Синк восстановлен (lag %ds)", "de": "✅ Sync wiederhergestellt (Lag %ds)"},
  "a_disk":     {"en": "🟡 DISK: %s is %d%% full (%s GB free)", "ru": "🟡 ДИСК: %s занят на %d%% (%s ГБ свободно)", "de": "🟡 PLATTE: %s zu %d%% voll (%s GB frei)"},
  "a_disk_ok":  {"en": "✅ Disk ok: %d%% used", "ru": "✅ Диск ок: %d%% занято", "de": "✅ Platte ok: %d%% belegt"},
  "a_wd_rest":  {"en": "⚠️ WATCHDOG: auto-restarted the node (total: %d)\n%s", "ru": "⚠️ WATCHDOG: сделан авто-рестарт ноды (всего: %d)\n%s", "de": "⚠️ WATCHDOG: Node automatisch neu gestartet (gesamt: %d)\n%s"},
  "a_wd_gave":  {"en": "🔴🔴 WATCHDOG GAVE UP — manual intervention needed:\n%s", "ru": "🔴🔴 WATCHDOG СДАЛСЯ — нужно вмешательство вручную:\n%s", "de": "🔴🔴 WATCHDOG HAT AUFGEGEBEN — manueller Eingriff nötig:\n%s"},
  "a_wd_quiet": {"en": "🟡 WATCHDOG SILENT: logs not updated for %s — check cron", "ru": "🟡 WATCHDOG молчит: логи не обновлялись %s — проверь cron", "de": "🟡 WATCHDOG STUMM: Logs seit %s nicht aktualisiert — cron prüfen"},
  "a_wd_back":  {"en": "✅ WATCHDOG is writing to the log again", "ru": "✅ WATCHDOG снова пишет в лог", "de": "✅ WATCHDOG schreibt wieder ins Log"},
  "a_flood":    {"en": "🔴 waltrace FLOOD: %d lines of \u00abwaltrace thread stopped\u00bb in 5 min. ", "ru": "🔴 ФЛУД waltrace: %d строк «waltrace thread stopped» за 5 мин. ", "de": "🔴 waltrace-FLUT: %d Zeilen \u00abwaltrace thread stopped\u00bb in 5 Min. "},
  "a_flood2":   {"en": "The waltrace thread is dead and the journal is filling up. Restart monad-bft to fix.", "ru": "Поток waltrace мёртв, журнал забивается. Лечится рестартом monad-bft.", "de": "Der waltrace-Thread ist tot und das Journal läuft voll. Neustart von monad-bft behebt es."},
  "a_flood_ok": {"en": "✅ waltrace flood stopped (%d in 5 min)", "ru": "✅ Флуд waltrace прекратился (%d за 5 мин)", "de": "✅ waltrace-Flut beendet (%d in 5 Min)"},
  "a_prom_down":{"en": "🟡 Prometheus not responding — metric alerts are not being tracked", "ru": "🟡 Prometheus не отвечает — алерты по метрикам не отслеживаются", "de": "🟡 Prometheus antwortet nicht — Metrik-Alarme werden nicht verfolgt"},
  "a_prom_up":  {"en": "✅ Prometheus responding again", "ru": "✅ Prometheus снова отвечает", "de": "✅ Prometheus antwortet wieder"},
  "a_prom_clr": {"en": "✅ PROMETHEUS: alert cleared %s", "ru": "✅ PROMETHEUS: снят алерт %s", "de": "✅ PROMETHEUS: Alarm aufgehoben %s"},
  "a_repeat":   {"en": "(repeat) ", "ru": "(повтор) ", "de": "(Wiederholung) "},
  "a_reminder": {"en": "⏰ REMINDER (repeats every %d min):\n", "ru": "⏰ НАПОМИНАНИЕ (повтор каждые %d мин):\n", "de": "⏰ ERINNERUNG (alle %d Min):\n"},

  # misc
  "m_started":  {"en": "🤖 monad-tg-bot started on %s. /help for commands.", "ru": "🤖 monad-tg-bot запущен на %s. /help — команды.", "de": "🤖 monad-tg-bot gestartet auf %s. /help für Befehle."},
  "m_unauth":   {"en": "⛔ Not authorised. Your chat_id: %s", "ru": "⛔ Не авторизован. Твой chat_id: %s", "de": "⛔ Nicht autorisiert. Deine chat_id: %s"},
  "m_chatid":   {"en": "chat_id: %s\nAdd it to ALLOWED_CHAT_IDS in config.env and restart the service.\n\n%s", "ru": "chat_id: %s\nДобавь его в ALLOWED_CHAT_IDS в config.env и перезапусти сервис.\n\n%s", "de": "chat_id: %s\nTrage sie in ALLOWED_CHAT_IDS in config.env ein und starte den Dienst neu.\n\n%s"},
  "m_unknown":  {"en": "Unknown command. /help", "ru": "Неизвестная команда. /help", "de": "Unbekannter Befehl. /help"},
  "m_lang":     {"en": "Language: English. Tap 🌐 or /lang to cycle.", "ru": "Язык: русский. Нажмите 🌐 или /lang для смены.", "de": "Sprache: Deutsch. 🌐 oder /lang zum Wechseln."},
  "help":       {"en": ("Monad node bot — %s\n\n"
                        "/status — overview\n/sync — sync state\n/disk — disk and resources\n"
                        "/waltrace — watchdog/waltrace\n/node — version/uptime/peers\n"
                        "/validator — registry id, self stake, commission\n"
                        "/id — show chat id\n/lang — switch language\n/help — this message"),
                 "ru": ("Monad node bot — %s\n\n"
                        "/status — общая сводка\n/sync — статус синхронизации\n/disk — диск/ресурсы\n"
                        "/waltrace — watchdog/waltrace\n/node — версия/аптайм/пиры\n"
                        "/validator — id в реестре, свой стейк, комиссия\n"
                        "/id — показать chat id\n/lang — сменить язык\n/help — помощь"),
                 "de": ("Monad node bot — %s\n\n"
                        "/status — Übersicht\n/sync — Sync-Status\n/disk — Platte und Ressourcen\n"
                        "/waltrace — Watchdog/waltrace\n/node — Version/Laufzeit/Peers\n"
                        "/validator — Registry-ID, Eigen-Stake, Provision\n"
                        "/id — chat id anzeigen\n/lang — Sprache wechseln\n/help — diese Nachricht")},
}


def tr(key):
    return T[key].get(_lang, T[key]["en"])


def keyboard():
    """Inline keyboard in the current language, with a language toggle."""
    return json.dumps({"inline_keyboard": [
        [{"text": tr("b_status"), "callback_data": "status"}, {"text": tr("b_sync"), "callback_data": "sync"}],
        [{"text": tr("b_disk"), "callback_data": "disk"}, {"text": tr("b_wal"), "callback_data": "waltrace"}],
        [{"text": tr("b_node"), "callback_data": "node"}, {"text": tr("b_help"), "callback_data": "help"}],
        [{"text": tr("b_val"), "callback_data": "validator"}],
        [{"text": tr("b_lang"), "callback_data": "lang"}],
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
            params["reply_markup"] = keyboard()
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
        if send(a["cid"], tr("a_repeat") + a["text"], kb=True):
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

# ---------- validator (staking precompile) ----------
# Роль ноды — это НЕ то же самое, что наличие ключей на диске. Полноценных состояний три:
# нода зарегистрирована и в активном сете, зарегистрирована и ждёт стейка, не зарегистрирована
# вовсе. Плюс четвёртое, отдельное: проверить не смогли. Последнее нельзя показывать как «нулевой
# стейк» — это выдуманный ответ.
STAKING_PRECOMPILE = "0x0000000000000000000000000000000000001000"
GET_VALIDATOR_SEL = "0x2b6d639a"
# Метка secp_key есть только у коллектора; сама нода на :9143 её не публикует. Зато self_stake_bps
# есть у ноды. Поэтому два разных источника, а не один.
OTEL_METRICS = CFG.get("OTEL_METRICS", os.environ.get("OTEL_METRICS", "http://127.0.0.1:8889/metrics"))
NODE_METRICS = CFG.get("NODE_METRICS", os.environ.get("NODE_METRICS", "http://127.0.0.1:9143/metrics"))
VAL_CACHE = CFG.get("VAL_CACHE", os.environ.get("VAL_CACHE", "/opt/monad-tg-bot/validator.json"))
VAL_SCAN_BUDGET = int(CFG.get("VAL_SCAN_BUDGET", "400"))


def _metrics_text(url, timeout=5):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return None


def node_secp():
    """(ключ, причина_отказа). Ровно один из двух не None.

    Причины разделены намеренно: «коллектор не отвечает», «метки нет вовсе» и «ключей несколько»
    чинятся по-разному, и слить их в одно «не смог» значит отправить оператора искать вслепую.
    Несколько ключей — это тоже отказ: какой из них наш, неизвестно, а угадывать нельзя.
    """
    txt = _metrics_text(OTEL_METRICS)
    if txt is None:
        return None, "otel"
    keys = {k.lower() for k in re.findall(r'secp_key="([0-9a-fA-F]{66})"', txt)}
    if not keys:
        return None, "nolabel"
    if len(keys) > 1:
        return None, "many"
    return keys.pop(), None


def self_stake_bps():
    """(значение, удалось_ли). >0 = нода в активном сете валидаторов.

    Формат Prometheus: `имя{метки} значение [timestamp_ms]`, где третье поле НЕОБЯЗАТЕЛЬНО.
    Нода его не ставит, коллектор ставит. Взять последнее поле значит на одном из двух источников
    прочитать timestamp (~1.8e12) и объявить активным валидатором ноду с нулевым стейком.
    Поэтому: срезаем метки и берём второе поле — значение в обоих случаях.
    """
    txt = _metrics_text(NODE_METRICS)
    if txt is None:
        return None, False
    vals = []
    for line in txt.splitlines():
        if line.startswith("#") or not line.startswith("monad_state_node_state_self_stake_bps"):
            continue
        parts = re.sub(r"\{[^}]*\}", "", line).split()
        if len(parts) >= 2:
            try:
                vals.append(float(parts[1]))
            except ValueError:
                pass
    if len(vals) != 1:
        return None, False
    return vals[0], True


def _word(h, i):
    return int(h[i * 64:(i + 1) * 64], 16)


def _get_validator(vid):
    """Слова ответа get_validator(vid), или None если вызов не удался."""
    res = rpc("eth_call", [{"to": STAKING_PRECOMPILE,
                            "data": GET_VALIDATOR_SEL + ("%064x" % vid)}, "latest"])
    if not isinstance(res, str) or len(res) < 2 + 64 * 12:
        return None
    return res[2:]


def _val_secp(h):
    """Ключ из динамической части ответа. Смещения лежат в словах 10 и 11."""
    o = _word(h, 10) // 32
    n = _word(h, o)
    return h[(o + 1) * 64:(o + 1) * 64 + n * 2].lower()


def _registered(h):
    return h is not None and _word(h, 0) != 0


def _registry_top(spent):
    """Наибольший занятый id: удвоение, затем деление пополам.

    Номера выдаются по порядку, поэтому недавно зарегистрированная нода лежит у самой верхушки —
    поиск вниз отсюда находит её за единицы вызовов вместо обхода всего реестра.
    """
    hi = 1
    while _registered(_get_validator(hi)) and hi < 1 << 20:
        spent[0] += 1
        hi *= 2
    spent[0] += 1
    lo = hi // 2
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        spent[0] += 1
        if _registered(_get_validator(mid)):
            lo = mid
        else:
            hi = mid
    return lo


def _cache_read():
    try:
        with open(VAL_CACHE) as f:
            return json.load(f)
    except Exception:
        return {}


def _cache_write(obj):
    try:
        os.makedirs(os.path.dirname(VAL_CACHE), exist_ok=True)
        tmp = VAL_CACHE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(obj, f)
        os.replace(tmp, VAL_CACHE)
    except Exception:
        pass


def validator_lookup():
    """('validator'|'none'|'unknown', info).

    'unknown' — это НЕ 'none'. Нода с недоступным коллектором или молчащим RPC не была показана
    незарегистрированной; сказать про неё «стейк 0» значит придумать ответ.
    """
    secp, why = node_secp()
    if not secp:
        return "unknown", {"why": why}

    cached = _cache_read()
    if cached.get("secp") == secp and isinstance(cached.get("id"), int):
        h = _get_validator(cached["id"])
        if h is None:
            return "unknown", {"why": "rpc"}
        # Кеш подтверждаем ключом, а не принимаем на веру: id могли перерегистрировать.
        if _registered(h) and _val_secp(h) == secp:
            return "validator", _describe(cached["id"], h)

    spent = [0]
    top = _registry_top(spent)
    if top < 1:
        return "unknown", {"why": "rpc"}

    # Отрицательный ответ тоже кешируется, иначе фулл-нода обходила бы весь реестр каждую минуту
    # (сотни eth_call в тик). Ключа нет — значит его нет НИ В ОДНОЙ существующей записи; новые
    # записи получают номера по возрастанию, поэтому досмотреть достаточно то, что появилось
    # после прошлой проверки. Верхнюю границу ищем делением пополам, это единицы вызовов.
    floor = 0
    if cached.get("secp") == secp and cached.get("id") is None:
        seen = cached.get("top")
        if isinstance(seen, int):
            if top <= seen:
                return "none", {"top": top}
            floor = seen

    for vid in range(top, floor, -1):
        if spent[0] >= VAL_SCAN_BUDGET:
            # Бюджет исчерпан — это «не досмотрели», а не «не нашли».
            return "unknown", {"why": "budget", "scanned": spent[0], "top": top}
        spent[0] += 1
        h = _get_validator(vid)
        if h is None:
            return "unknown", {"why": "rpc"}
        if _registered(h) and _val_secp(h) == secp:
            _cache_write({"secp": secp, "id": vid, "top": top})
            return "validator", _describe(vid, h)
    _cache_write({"secp": secp, "id": None, "top": top})
    return "none", {"top": top}


def _describe(vid, h):
    """Только те поля, смысл которых подтверждён: стейк (w2) и комиссия (w4).

    Остальные семь uint256 ABI не именует, и гадать о них в отчёте оператору нельзя.
    Признак активного сета берём отдельно — из документированной метрики self_stake_bps.
    """
    bps, ok = self_stake_bps()
    return {
        "id": vid,
        "stake": _word(h, 2) / 10 ** 18,
        "commission": _word(h, 4) / 10 ** 16,
        "auth": "0x" + h[24:64],
        "bps": bps,
        "bps_ok": ok,
    }


def fmt_validator():
    state, info = validator_lookup()
    if state == "unknown":
        return tr("vl_unknown") % tr("vl_why_" + info.get("why", "rpc"))
    if state == "none":
        return tr("vl_none") % info.get("top", "?")
    L = [tr("vl_title") % info["id"]]
    L.append(tr("vl_stake") % format(info["stake"], ",.0f"))
    L.append(tr("vl_comm") % info["commission"])
    if not info["bps_ok"]:
        L.append(tr("vl_set_unknown"))
    elif info["bps"] > 0:
        L.append(tr("vl_set_in") % (info["bps"] / 100.0))
    else:
        L.append(tr("vl_set_out"))
    L.append(tr("vl_auth") % info["auth"])
    return "\n".join(L)


# ---------- formatted reports (for commands) ----------
def sync_symbol(s):
    """
    Состояние синхронизации по ОТСТАВАНИЮ, а не по флагу eth_syncing.

    eth_syncing=false у Monad значит «нет активного процесса синхронизации» — это же
    возвращает и нода, которая встала намертво. Единственный честный признак — возраст
    последнего блока. Три состояния, а не два: измерили и норма, измерили и отстаём,
    измерить не смогли.
    """
    if s["lag"] is None:
        return tr("s_nolag")
    if s["lag"] > SYNC_LAG_WARN:
        return tr("s_behind")
    if s["syncing"] not in (False, None):
        return tr("s_sync")
    return tr("s_ok")


def fmt_status():
    L = ["📟 Monad node — %s" % HOST]
    _st = {s: svc_active(s) for s in SERVICES}
    bad = [s for s, v in _st.items() if v is False]
    unk = [s for s, v in _st.items() if v is None]
    if bad:
        L.append(tr("st_down") + ", ".join(bad))
    elif unk:
        L.append(tr("st_unk") + ", ".join(unk))
    else:
        L.append(tr("st_ok"))
    s = get_sync()
    if s["height"] is not None:
        # Значок ставится по lag, а НЕ по eth_syncing. eth_syncing=false означает лишь
        # «процесс синхронизации не идёт», и это же значение отдаёт намертво вставшая нода:
        # 2026-08-07 нода стояла на одном блоке и отставала на тысячи, а /status рисовал ✅.
        # lag=None — это «не смог измерить», отдельное третье состояние, не «здоров».
        sync_txt = sync_symbol(s)
        lag = "?" if s["lag"] is None else "%ds" % s["lag"]
        L.append(tr("st_sync") % (sync_txt, s["height"], lag))
    else:
        L.append(tr("st_norpc"))
    d = get_disk()
    if d["pct"] is not None:
        icon = "✅" if d["pct"] < DISK_WARN_PCT else "🟡"
        L.append(tr("st_disk") % (d.get("mount", "/"), icon, d["pct"], d["avail_gb"]))
    L.append(tr("st_ver") % node_version())
    _wc = watchdog_restart_count()
    L.append(tr("st_wdr") % (tr("na") if _wc is None else _wc))
    return "\n".join(L)

def fmt_sync():
    s = get_sync()
    if s["height"] is None:
        return tr("sy_norpc")
    return (tr("sy_body") % (
        sync_symbol(s),
        s["height"], "?" if s["lag"] is None else "%ds" % s["lag"], s["syncing"]))

def fmt_disk():
    d = get_disk()
    top = sh("df -h / | tail -1")
    return tr("dk_body") % (
        "—" if d["pct"] is None else tr("dk_used") % (d["pct"], d["avail_gb"]), top)

def fmt_waltrace():
    cnt = watchdog_restart_count()
    last = last_watchdog_action() or tr("wd_none")
    age = watchdog_action_age()
    if age is not None and age > WATCHDOG_FRESH_SEC:
        last += tr("wd_stale") % (
            tr("t_hours") % int(age // 3600) if age >= 3600 else tr("t_mins") % int(age // 60))
    L = [tr("wd_title")]
    for p in WATCHDOG_LOGS:
        mt = sh("stat -c %%Y %s 2>/dev/null" % p)
        name = os.path.basename(p).replace(".log", "")
        if not mt.isdigit():
            L.append(tr("wd_nolog") % name)
        else:
            a = int(time.time() - int(mt))
            state = tr("wd_alive") if a <= 1800 else tr("wd_quiet")
            L.append(tr("wd_entry") % (name, state, a // 60))
    L.append(tr("wd_total") % (tr("na") if cnt is None else cnt))
    # Флуд waltrace оставлен как диагностика на случай регрессии: баг починен в 0.15.2
    # (cleanup-скрипт больше не удаляет wal_* из-под потока), сейчас должен быть 0.
    L.append(tr("wd_flood")
             % sh("journalctl -u monad-bft --since '5 min ago' --no-pager 2>/dev/null | grep -c 'waltrace thread stopped'"))
    need = watchdog_needs_attention()
    if need:
        L.append(tr("wd_need") % need)
    L.append(tr("wd_last") % last)
    return "\n".join(L)

def fmt_node():
    up = sh("systemctl show monad-bft -p ExecMainStartTimestamp --value")
    # Префикс сжатого secp256k1-ключа — 02 ИЛИ 03, в зависимости от чётности Y. Шаблон ловил
    # только 02, то есть ровно половину сети: на живой ноде 94 против 196 уникальных пиров.
    peers = sh("journalctl -u monad-bft --since '1 min ago' --no-pager 2>/dev/null | grep -oE 'node_id\":\"0[23][0-9a-f]{6}' | sort -u | wc -l")
    return tr("nd_body") % (HOST, node_version(), up, peers)



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
    # --- валидатор: стейк, комиссия, членство в активном сете ---------------------
    # Сравниваем с прошлым тиком и сообщаем только об ИЗМЕНЕНИИ. Первое наблюдение всегда молчит:
    # иначе бот бы кричал про «новый стейк» на каждом своём перезапуске.
    # 'unknown' (коллектор молчит, прекомпайл не ответил) состояние НЕ трогает и алертов не даёт —
    # это «не смогли посмотреть», а не «стало ноль».
    v_state, v_info = validator_lookup()
    if v_state == "validator":
        prev_id = st.get("val_id")
        if prev_id is not None and v_info["id"] != prev_id:
            alerts.append(tr("a_val_id") % (prev_id, v_info["id"]))

        prev_stake = st.get("val_stake")
        if prev_stake is not None and v_info["stake"] != prev_stake:
            delta = v_info["stake"] - prev_stake
            alerts.append(tr("a_val_stake") % (
                "+" if delta > 0 else "−", format(abs(delta), ",.0f"),
                format(prev_stake, ",.0f"), format(v_info["stake"], ",.0f")))

        prev_comm = st.get("val_commission")
        # Комиссию меняем только мы. Изменение, которого мы не делали, — повод посмотреть, кто.
        if prev_comm is not None and abs(v_info["commission"] - prev_comm) > 1e-9:
            alerts.append(tr("a_val_comm") % (prev_comm, v_info["commission"]))

        if v_info["bps_ok"]:
            in_set = v_info["bps"] > 0
            prev_in = st.get("val_in_set")
            if prev_in is not None and in_set != prev_in:
                alerts.append((tr("a_val_in") % (v_info["bps"] / 100.0)) if in_set else tr("a_val_out"))
            st["val_in_set"] = in_set

        st["val_id"] = v_info["id"]
        st["val_stake"] = v_info["stake"]
        st["val_commission"] = v_info["commission"]
    elif v_state == "none" and st.get("val_id") is not None:
        # Были в реестре, а теперь ключа там нет — это событие, а не тишина.
        alerts.append(tr("a_val_gone") % st["val_id"])
        for k in ("val_id", "val_stake", "val_commission", "val_in_set"):
            st.pop(k, None)

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
            alerts.append(tr("a_svc_down") % s)
        elif prev_active is False and active is True:
            alerts.append(tr("a_svc_up") % s)
        # crash-loop: NRestarts растёт, даже если фаза опроса всё время «здоровая»
        nr = svc_restarts(s)
        prev_nr = st.get("nrestarts_" + s)
        if nr is not None:
            if prev_nr is not None and nr > prev_nr:
                delta = nr - prev_nr
                if delta >= CRASHLOOP_RESTARTS:
                    alerts.append(tr("a_crashloop")
                                  % (s, delta, nr))
                else:
                    alerts.append(tr("a_sysrest") % (s, nr))
            st["nrestarts_" + s] = nr
        if prev_start and start and start != prev_start and active:
            wd = recent_watchdog_action()
            tag = tr("a_expected") if (s == "monad-bft" and wd and "restart" in wd.lower()) else ""
            alerts.append(tr("a_restart") % (s, tag, start))
        st["active_" + s] = active
        st["start_" + s]  = start
    # sync lag
    s = get_sync()
    rpc_dead = s["height"] is None
    if rpc_dead and not st.get("rpc_dead"):
        alerts.append(tr("a_rpc_down"))
    elif not rpc_dead and st.get("rpc_dead"):
        alerts.append(tr("a_rpc_up"))
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
            alerts.append(tr("a_lag") % (s["lag"], s["height"]))
        elif not behind and st.get("behind"):
            alerts.append(tr("a_lag_ok") % s["lag"])
        st["behind"] = behind
    # БЛОК НЕ РАСТЁТ (урок инцидента 2026-07-19: 5ч фриза с одним тихим алертом)
    h = s["height"]
    if h is not None:
        if st.get("last_height") == h:
            st["stall_ticks"] = st.get("stall_ticks", 0) + 1
        else:
            if st.get("stalled"):
                alerts.append(tr("a_stuck_ok") % h)
            st["stall_ticks"] = 0
            st["stalled"] = False
        if st.get("stall_ticks", 0) >= STALL_TICKS and not st.get("stalled"):
            alerts.append(tr("a_stuck")
                          % (h, st["stall_ticks"] * CHECK_INTERVAL // 60))
            st["stalled"] = True
        st["last_height"] = h
    # РЕ-АЛЕРТ критических состояний каждые REALERT_SEC (не молчать часами!)
    now = time.time()
    crit = []
    if st.get("stalled"):  crit.append(tr("a_stuck_still") % st.get("last_height"))
    if st.get("rpc_dead"): crit.append(tr("a_rpc_still"))
    for svc in SERVICES:
        if st.get("active_" + svc) is False:
            crit.append(tr("a_svc_still") % svc)
    if crit:
        if now - st.get("last_realert", 0) >= REALERT_SEC:
            alerts.append(tr("a_reminder") % (REALERT_SEC // 60)
                          + "\n".join(crit))
            st["last_realert"] = now
    else:
        st["last_realert"] = 0
    # Флуд waltrace. Проверка существовала только в /status — то есть срабатывала, лишь если
    # человек сам спросит. 2026-07-31 регрессия на 0.15.2 (сообщение «waltrace thread stopped»,
    # ~220 строк/с) шла 4.5 часа, и ни один канал о ней не сообщил: правила в Prometheus нет,
    # метрики нет, в monitor() счётчик не заглядывал. Пассивный индикатор — не мониторинг.
    ok_wt, wt = sh_try("journalctl -u monad-bft --since '5 min ago' --no-pager 2>/dev/null "
                       "| grep -c 'waltrace thread stopped'", timeout=45)
    if ok_wt and wt.isdigit():
        _n = int(wt)
        flooding = _n >= WALTRACE_FLOOD_5MIN
        if flooding and not st.get("waltrace_flood"):
            # a_flood carries the %d, a_flood2 is the explanation appended to it.
            alerts.append((tr("a_flood") + tr("a_flood2")) % _n)
        elif not flooding and st.get("waltrace_flood"):
            alerts.append(tr("a_flood_ok") % _n)
        st["waltrace_flood"] = flooding

    # disk
    d = get_disk()
    if d["pct"] is not None:
        warn = d["pct"] >= DISK_WARN_PCT
        if warn and not st.get("disk_warn"):
            alerts.append(tr("a_disk")
                          % (d.get("mount", "/"), d["pct"], d["avail_gb"]))
        elif not warn and st.get("disk_warn"):
            alerts.append(tr("a_disk_ok") % d["pct"])
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
            alerts.append(tr("a_prom_clr") % k)
        st["prom_alerts"] = sorted(cur)
        if st.get("prom_down"):
            alerts.append(tr("a_prom_up"))
            st["prom_down"] = False
    elif not st.get("prom_down"):
        # Недоступный Prometheus = мы ослепли по половине сигналов. Раньше об этом никто
        # не узнавал, потому что бот про Prometheus вообще не знал.
        alerts.append(tr("a_prom_down"))
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
            alerts.append(tr("a_wd_rest")
                          % (cnt, last_watchdog_action()))
        st["wd_count"] = cnt

    # Самое тяжёлое сообщение watchdog'а: нода зависла ПОВТОРНО после авто-рестарта и
    # автоматика сдалась. Раньше оно вообще не покидало лог.
    need = watchdog_needs_attention()
    if need and st.get("wd_alert_line") != need:
        alerts.append(tr("a_wd_gave") % need)
        st["wd_alert_line"] = need

    # Мёртвый watchdog = нода без авто-восстановления. Раньше его остановка была невидима.
    alive, wd_age = watchdog_alive()
    if alive is None:
        # Watchdog на этом хосте не установлен — тишина ожидаема, состояние не трогаем.
        pass
    elif not alive and not st.get("wd_dead"):
        alerts.append(tr("a_wd_quiet")
                      % (tr("t_never") if wd_age is None else tr("t_mins") % int(wd_age // 60)))
        st["wd_dead"] = True
    elif alive and st.get("wd_dead"):
        alerts.append(tr("a_wd_back"))
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
    elif cmd in ("validator", "val", "stake"): send(cid, fmt_validator(), kb=True)
    elif cmd == "help":   send(cid, tr("help") % HOST, kb=True)
    elif cmd == "lang":
        # Cycle EN -> RU -> DE. Show the full help right away: Telegram does not re-render
        # earlier messages, so a bare confirmation looks like nothing changed.
        global _lang
        order = ["en", "ru", "de"]
        _lang = order[(order.index(_lang) + 1) % len(order)] if _lang in order else "en"
        st = load_state(); st["lang"] = _lang; save_state(st)
        send(cid, tr("m_lang") + "\n\n" + tr("help") % HOST, kb=True)
    else: send(cid, tr("m_unknown"), kb=True)

def handle(msg):
    chat = msg.get("chat", {})
    cid = str(chat.get("id", ""))
    text = (msg.get("text") or "").strip()
    if not text.startswith("/"):
        return
    cmd = text.split()[0].lstrip("/").split("@")[0].lower()
    if cmd in ("id", "start"):
        send(cid, tr("m_chatid") % (cid, tr("help") % HOST if cid in ALLOWED else ""), kb=cid in ALLOWED)
        return
    if cid not in ALLOWED:
        send(cid, tr("m_unauth") % cid)
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
        send(cid, tr("m_unauth") % cid)
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
        # Printed before the language setting is read, so it stays English.
        sys.stderr.write("BOT_TOKEN is not set in %s — exiting\n" % CFG_PATH); sys.exit(1)
    st = load_state()
    # Restore the language chosen with /lang across restarts; config LANG is the fallback.
    global _lang
    _lang = st.get("lang", DEFAULT_LANG)
    offset = st.get("offset", 0)
    # init service baselines so first tick doesn't false-alarm
    monitor(st)
    broadcast(tr("m_started") % HOST)
    last_check = time.time()
    net_fail = 0
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
            net_fail = 0
        elif upd is not None:
            net_fail = 0
            time.sleep(2)  # HTTP-ошибка API (401/409/429): не долбим в busy-loop
        else:
            # upd is None — не-HTTP сбой: DNS, обрыв, таймаут чтения. Паузы тут не было вовсе,
            # и при быстром отказе (сеть легла) цикл крутился бы вплотную, забивая journald
            # рядом с боевой нодой. Нарастающая пауза, как в provenance-tg-bot.
            net_fail += 1
            time.sleep(min(2 * (2 ** min(net_fail - 1, 4)), 60))  # 2,4,8,16, далее 32 с
        if time.time() - last_check >= CHECK_INTERVAL:
            try: monitor(st)
            except Exception as e:
                sys.stderr.write("monitor error: %s\n" % e)
            last_check = time.time()

if __name__ == "__main__":
    main()
