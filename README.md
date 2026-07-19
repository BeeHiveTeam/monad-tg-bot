# monad-tg-bot

A tiny, dependency-free Telegram bot for monitoring a [Monad](https://monad.xyz) node:
push alerts on incidents + on-demand status commands. Pure Python stdlib
(`urllib`/`json`/`subprocess`), one file, runs as a systemd service.

Companion to [monad-tools](https://github.com/BeeHiveTeam/monad-tools) and
[monad-grafana](https://github.com/BeeHiveTeam/monad-grafana).

## Features

**Push alerts** (sent only on state change, no spam):
- 🔴 a node service (`monad-bft` / `monad-execution` / `monad-rpc`) goes down — and ✅ recovery
- 🔄 a service restarts (tagged as expected when it matches a waltrace-watchdog action)
- ⚠️ waltrace auto-restart detected (counter from the watchdog log)
- 🟡 sync falls behind (block lag over threshold) / 🔴 RPC unreachable — and ✅ recovery
- 🟡 disk `/` usage crosses the warning threshold

**Commands** (answered only for authorized chat IDs):

| Command | What |
|---|---|
| `/status` | one-shot summary: services, sync+block+lag, disk, version, waltrace restarts |
| `/sync` | sync status, block height, lag |
| `/disk` | disk usage of `/` |
| `/waltrace` | watchdog auto-restart count, recent flood, last action |
| `/node` | node version, uptime, peer count |
| `/id` | reply with your chat id (for first-time setup) |
| `/help` | command list |

Every reply and alert comes with inline buttons for the same commands — tap instead of typing.

## Requirements

- Python 3.8+
- A Monad node with JSON-RPC on `http://localhost:8080` (adjust `RPC` in `bot.py` if different)
- systemd, and `systemctl` / `journalctl` / `df` available (the bot shells out to them)
- A Telegram bot token from [@BotFather](https://t.me/BotFather)

The bot runs as **root** because it reads systemd state, the journal, and the
root-owned watchdog log. It performs **read-only** monitoring — it does not
restart or modify the node.

## Install

```bash
sudo mkdir -p /opt/monad-tg-bot
sudo install -m 0755 bot.py            /opt/monad-tg-bot/bot.py
sudo install -m 0644 monad-tg-bot.service /etc/systemd/system/monad-tg-bot.service
sudo install -m 0600 config.env.example /opt/monad-tg-bot/config.env   # then edit

# 1) create a bot via @BotFather, put the token in config.env:
sudo sed -i 's|^BOT_TOKEN=.*|BOT_TOKEN=<your-token>|' /opt/monad-tg-bot/config.env

# 2) start it
sudo systemctl daemon-reload
sudo systemctl enable --now monad-tg-bot

# 3) message the bot /id to learn your chat id, then authorize it:
sudo sed -i 's|^ALLOWED_CHAT_IDS=.*|ALLOWED_CHAT_IDS=<your-chat-id>|' /opt/monad-tg-bot/config.env
sudo systemctl restart monad-tg-bot
```

## Configuration (`config.env`)

| Key | Default | Meaning |
|---|---|---|
| `BOT_TOKEN` | — | bot token from @BotFather |
| `ALLOWED_CHAT_IDS` | — | comma-separated authorized chat ids |
| `CHECK_INTERVAL` | `60` | background check interval, seconds |
| `DISK_WARN_PCT` | `85` | disk `/` usage warning threshold, percent |
| `SYNC_LAG_WARN_SEC` | `30` | sync lag warning threshold, seconds |

State (Telegram update offset + alert de-dup baseline) is kept in
`/opt/monad-tg-bot/state.json`.

## Security notes

- `config.env` holds the bot token — it is `chmod 600` and **gitignored**. Never commit it.
- Only `ALLOWED_CHAT_IDS` get data; everyone else gets nothing but their own chat id.
- Only one process may long-poll a bot token. Running a manual
  `getUpdates` against the same token while the service is up returns HTTP 409
  and disrupts the bot for one cycle — don't.
- The bot is read-only; it never restarts or reconfigures the node.

## License

MIT
