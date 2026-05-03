"""
config.py  —  Credentials & settings loaded from environment variables.

In GitHub Actions: set these as repository Secrets
  (Settings → Secrets and variables → Actions → New repository secret)

  Secret name             Maps to
  ──────────────────────  ──────────────────────────────────────────
  UPSTOX_ACCESS_TOKEN     Upstox API access token
  TELEGRAM_BOT_TOKEN      Telegram bot token from @BotFather
  TELEGRAM_CHAT_ID        Your Telegram chat / channel ID

For local development create a `.env` file (never commit it):
  UPSTOX_ACCESS_TOKEN=eyJ...
  TELEGRAM_BOT_TOKEN=123456:ABC...
  TELEGRAM_CHAT_ID=987654321

Then run:  python -m dotenv run python websocket.py
  OR just export the variables in your shell before running.
"""

import os
import sys

# ── Load .env file for local development (optional) ──────────────────────────
# If python-dotenv is installed and a .env file exists, load it silently.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # python-dotenv not installed — rely on shell env vars

# ── Read credentials from environment ────────────────────────────────────────

access_token       = os.environ.get("UPSTOX_ACCESS_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Validate at startup ───────────────────────────────────────────────────────

_missing = []

if not access_token:
    _missing.append("UPSTOX_ACCESS_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    _missing.append("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHAT_ID:
    _missing.append("TELEGRAM_CHAT_ID")

if _missing:
    print(
        f"[CONFIG] ⚠️  Missing environment variable(s): {', '.join(_missing)}\n"
        f"[CONFIG]    Set them as GitHub Secrets or export them in your shell.\n"
        f"[CONFIG]    Bot may not function correctly without these."
    )