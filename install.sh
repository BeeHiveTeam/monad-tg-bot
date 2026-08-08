#!/usr/bin/env bash
#
# monad-tg-bot installer.
#
#   curl -fsSL https://raw.githubusercontent.com/BeeHiveTeam/monad-tg-bot/main/install.sh | bash
#
# Run this ON the Monad node: the bot reads systemd unit state, disk usage and the local RPC,
# none of which are visible from elsewhere. Tokens are read without echo and never reach the
# shell history. Re-running keeps an existing config and never touches state.
#
# Unattended:
#   BOT_TOKEN=... ALLOWED_CHAT_IDS=123456 bash install.sh

set -euo pipefail

REPO_RAW="https://raw.githubusercontent.com/BeeHiveTeam/monad-tg-bot/main"
DEST="${MONAD_BOT_DIR:-/opt/monad-tg-bot}"
SERVICE="monad-tg-bot"

if [ -t 1 ]; then B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'
else B=""; G=""; Y=""; R=""; N=""; fi
say()  { printf '%s\n' "$*"; }
ok()   { printf '%s✓%s %s\n' "$G" "$N" "$*"; }
warn() { printf '%s!%s %s\n' "$Y" "$N" "$*"; }
die()  { printf '%s✗%s %s\n' "$R" "$N" "$*" >&2; exit 1; }

# Piped into bash, stdin is the script — prompts must come from the terminal. Probe by opening
# /dev/tty: it can exist and still be unopenable (container, cron), where a -r test passes and
# every read then returns empty, which would write a config with no token in it.
if { : < /dev/tty; } 2>/dev/null; then HAVE_TTY=1; else HAVE_TTY=0; fi
ask() { local p="$1" d="${2:-}" a=""
        if [ "$HAVE_TTY" -eq 1 ]; then printf '%s' "$p" > /dev/tty; read -r a < /dev/tty || a=""; fi
        printf '%s' "${a:-$d}"; }
ask_secret() { local p="$1" a=""
        if [ "$HAVE_TTY" -eq 1 ]; then printf '%s' "$p" > /dev/tty; read -rs a < /dev/tty || a=""
        printf '\n' > /dev/tty; fi
        printf '%s' "$a"; }

ENV_TOKEN="${BOT_TOKEN:-}"; ENV_CHATS="${ALLOWED_CHAT_IDS:-}"

say ""
say "${B}monad-tg-bot — Monad node watcher${N}"
say "Alerts on service, sync, disk and waltrace state; answers /status /sync /disk /node."
say ""

# ── preflight ────────────────────────────────────────────────────────────────
command -v python3 >/dev/null 2>&1 || die "python3 not found. Install Python 3.8+ and re-run."
python3 - <<'EOF' || die "Python is too old — 3.8+ required."
import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)
EOF
ok "Python $(python3 -c 'import sys;print("%d.%d"%sys.version_info[:2])')"
command -v curl >/dev/null 2>&1 || die "curl not found."

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  command -v sudo >/dev/null 2>&1 || die "Not root and sudo not found. Re-run as root or set MONAD_BOT_DIR to a writable path."
  SUDO="sudo"
fi

# This bot is only useful on the node itself — say so rather than installing a blind watcher.
if ! systemctl list-unit-files 2>/dev/null | grep -q '^monad-bft'; then
  warn "No monad-bft service found on this machine."
  warn "This bot reads unit state and the local RPC — it is meant to run ON the node."
  a=$(ask "Continue anyway? [y/N]: " "n")
  case "$a" in y|Y|yes) ;; *) die "Aborted. Run this on the Monad node." ;; esac
else
  ok "Found monad-bft on this machine"
fi

HAVE_SYSTEMD=0
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then HAVE_SYSTEMD=1; fi

# DEST reaches `chown -R` as root, so validate before anything touches the filesystem. An
# unset variable, a relative path or a system directory would otherwise recurse over it.
case "$DEST" in
  /*) ;;
  *) die "MONAD_BOT_DIR must be an absolute path (got: $DEST)" ;;
esac
case "$DEST" in
  *[!A-Za-z0-9/._-]*) die "MONAD_BOT_DIR must not contain spaces or shell metacharacters (got: $DEST)" ;;
esac
case "$DEST" in
  / | /etc | /etc/* | /usr | /usr/* | /var | /var/* | /bin | /bin/* | /sbin | /sbin/* | /lib | /lib/* | /boot | /boot/* | /dev | /dev/* | /proc | /proc/* | /sys | /sys/*)
    die "Refusing to install into a system directory: $DEST" ;;
esac

say ""
say "Install directory: ${B}${DEST}${N}"

# ── fetch ────────────────────────────────────────────────────────────────────
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
for f in bot.py config.env.example monad-tg-bot.service monad-stall-watchdog; do
  curl -fsSL "$REPO_RAW/$f" -o "$TMP/$f" || die "Could not download $f"
done
python3 -m py_compile "$TMP/bot.py" || die "Downloaded bot.py does not compile — not installing it."
ok "Downloaded and verified bot.py"

$SUDO mkdir -p "$DEST"
$SUDO cp "$TMP/bot.py" "$DEST/bot.py"
$SUDO cp "$TMP/config.env.example" "$DEST/config.env.example"
$SUDO cp "$TMP/monad-stall-watchdog" "$DEST/monad-stall-watchdog"
$SUDO chmod +x "$DEST/monad-stall-watchdog"

# ── config ───────────────────────────────────────────────────────────────────
CFG="$DEST/config.env"
WRITE_CFG=1
if $SUDO test -f "$CFG"; then
  say ""; warn "$CFG already exists."
  if [ "${MONAD_FORCE:-}" = "1" ]; then
    ok "Overwriting it (MONAD_FORCE=1)."
  else
    a=$(ask "Overwrite it? Existing tokens will be lost. [y/N]: " "n")
    case "$a" in y|Y|yes) ;; *) WRITE_CFG=0; ok "Keeping the existing config. Use MONAD_FORCE=1 to replace it." ;; esac
  fi
fi

if [ "$WRITE_CFG" -eq 1 ]; then
  if [ "$HAVE_TTY" -eq 0 ] && [ -z "$ENV_TOKEN" ]; then
    say ""
    die "No terminal available for the prompts.
   Either download and run it directly:
     curl -fsSLO $REPO_RAW/install.sh && bash install.sh
   or supply the values up front:
     BOT_TOKEN=... ALLOWED_CHAT_IDS=123456 bash install.sh"
  fi

  say ""
  say "${B}Configuration${N} — values are not echoed."
  say ""
  TOKEN="$ENV_TOKEN"
  if [ -z "$TOKEN" ]; then
    say "1. Telegram bot token, from @BotFather."
    TOKEN=$(ask_secret "   token: ")
  else
    ok "1. Token taken from the environment."
  fi
  [ -n "$TOKEN" ] || die "A Telegram token is required."

  say ""
  say "2. Chat ids allowed to talk to the bot, comma-separated."
  say "   ${Y}Do not know yours?${N} Message the bot once, then press Enter to detect it."
  CHATS="$ENV_CHATS"
  [ -n "$CHATS" ] || CHATS=$(ask "   chat ids (Enter to detect): ")
  if [ -z "$CHATS" ]; then
    say "   asking Telegram…"
    # Not in argv: /proc/PID/cmdline is world-readable and `ps` would show the token.
    CHATS=$(printf 'url = "https://api.telegram.org/bot%s/getUpdates"\n' "$TOKEN" \
      | curl -fsS --config - 2>/dev/null \
      | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
if not d.get("ok"): sys.exit(0)
ids=[]
for u in d.get("result", []):
    m = u.get("message") or u.get("channel_post") or {}
    c = (m.get("chat") or {}).get("id")
    if c is not None and c not in ids: ids.append(c)
print(",".join(str(i) for i in ids))' || true)
    if [ -n "$CHATS" ]; then ok "   detected: $CHATS"
    else die "Could not detect a chat id. Message the bot first, then re-run."; fi
  fi

  TMPCFG="$TMP/config.env"; (umask 077; : > "$TMPCFG")
  {
    echo "BOT_TOKEN=$TOKEN"
    echo "ALLOWED_CHAT_IDS=$CHATS"
  } >> "$TMPCFG"
  # Carry over the tunables from the shipped example so the file is self-documenting.
  grep -E '^#|^(CHECK_INTERVAL|DISK_WARN_PCT|SYNC_LAG_WARN_SEC)=' "$TMP/config.env.example" >> "$TMPCFG" 2>/dev/null || true
  $SUDO install -m 600 "$TMPCFG" "$CFG"
  ok "Wrote $CFG (mode 600)"
fi

# ── run ──────────────────────────────────────────────────────────────────────
say ""
if [ "$HAVE_SYSTEMD" -eq 1 ]; then
  if [ "$HAVE_TTY" -eq 1 ]; then
    a=$(ask "Install as a systemd service and start it now? [Y/n]: " "y")
    case "$a" in n|N|no) HAVE_SYSTEMD=0 ;; esac
  elif [ "${INSTALL_SERVICE:-}" = "1" ]; then
    ok "Installing the systemd service (INSTALL_SERVICE=1)."
  else
    # Writing a root-owned unit is the most intrusive step here; with nobody to ask, do not.
    HAVE_SYSTEMD=0
    warn "No terminal: skipping the systemd service. Re-run with INSTALL_SERVICE=1 to install it."
  fi
fi

if [ "$HAVE_SYSTEMD" -eq 1 ]; then
  # The unit hardens itself with PrivateTmp/ProtectHome, which hide /tmp and /home inside its
  # namespace — installing there crash-loops with 226/NAMESPACE. Relax only what is needed.
  EXTRA_SED=""
  case "$DEST" in
    /tmp/*|/var/tmp/*) EXTRA_SED="-e s|^PrivateTmp=.*|PrivateTmp=false|" ;;
    /home/*|/root/*)   EXTRA_SED="-e s|^ProtectHome=.*|ProtectHome=false|" ;;
  esac
  [ -n "$EXTRA_SED" ] && warn "Relaxing unit sandboxing so $DEST is visible to the service."

  RUN_USER="${SUDO_USER:-$(id -un)}"
  $SUDO chown -R "$RUN_USER" "$DEST"
  sed -e "s|^User=.*|User=$RUN_USER|" \
      -e "s|^WorkingDirectory=.*|WorkingDirectory=$DEST|" \
      -e "s|^ExecStart=.*|ExecStart=$(command -v python3) $DEST/bot.py|" \
      -e "s|^ReadWritePaths=.*|ReadWritePaths=$DEST|" \
      $EXTRA_SED \
      "$TMP/monad-tg-bot.service" > "$TMP/unit"
  UNIT="/etc/systemd/system/${SERVICE}.service"
  if $SUDO test -f "$UNIT" 2>/dev/null; then
    OLD_WD=$($SUDO grep -m1 '^WorkingDirectory=' "$UNIT" 2>/dev/null | cut -d= -f2- || true)
    if [ -n "$OLD_WD" ] && [ "$OLD_WD" != "$DEST" ]; then
      warn "An existing $SERVICE unit points at $OLD_WD, not $DEST."
      a=$(ask "Replace it? The old unit is backed up. [y/N]: " "n")
      case "$a" in y|Y|yes) ;; *) die "Left the existing service alone. Nothing was changed." ;; esac
    fi
    $SUDO cp "$UNIT" "${UNIT}.bak-$(date +%Y%m%d%H%M%S)" 2>/dev/null || true
  fi
  $SUDO cp "$TMP/unit" "$UNIT"
  $SUDO systemctl daemon-reload
  $SUDO systemctl enable --now "$SERVICE" >/dev/null 2>&1 || die "systemctl enable failed."
  sleep 3
  if $SUDO systemctl is-active --quiet "$SERVICE"; then
    ok "Service ${SERVICE} is running as ${RUN_USER}."
  else
    warn "Service is not active. Recent log:"
    $SUDO journalctl -u "$SERVICE" -n 15 --no-pager || true
    die "Start failed — fix the above and run: sudo systemctl restart $SERVICE"
  fi
  say ""
  say "  logs:    ${B}sudo journalctl -u $SERVICE -f${N}"
  say "  restart: ${B}sudo systemctl restart $SERVICE${N}"
else
  say "Run it with:"
  say "  ${B}cd $DEST && python3 bot.py${N}"
fi

say ""
say "Optional: ${B}$DEST/monad-stall-watchdog${N} restarts monad-bft if blocks stop advancing."
say "Add it to root's crontab if you want that safety net."
say ""
ok "Done. Send the bot ${B}/status${N} to check it answers."
say ""
