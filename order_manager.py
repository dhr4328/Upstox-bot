"""
order_manager.py  —  Virtual Option Order Manager for Live SBT Bot

VIRTUAL MODE: No real orders are placed via the broker API.
  - Entry price is taken from the live option LTP at signal time.
  - P&L is tracked using 1-minute intraday candles of the option.
  - To switch to LIVE trading, set VIRTUAL_MODE = False.

Signal → Option mapping:
  LONG  (BUY signal)  →  CALL option (CE)  →  Virtual BUY CE
  SHORT (SELL signal) →  PUT  option (PE)  →  Virtual BUY PE

Telegram Alerts:
  Alert 1  —  When a signal is generated and option is selected:
               Shows signal type, option instrument, entry price, lot size.
  Alert 2  —  When the trade is closed (target or SL hit):
               Shows option, entry, exit, P&L, result (PROFIT / LOSS).

Monitor:
  - Fetches 1-minute intraday candles for the option every 62 seconds.
  - Evaluates the latest closed candle's close price for P&L.
  - TARGET_PNL = +₹600  →  close trade (TARGET)
  - SL_PNL     = -₹300  →  close trade (STOP LOSS)

Constants:
  LOT_SIZE   = 65     (NIFTY lot size)
  TARGET_PNL = 600    (₹ profit target per trade)
  SL_PNL     = -300   (₹ stop-loss per trade)
  NIFTY_STEP = 50     (NIFTY strike step in index points)
"""

import os
import datetime
import threading
import time

import requests
import upstox_client
from upstox_client.rest import ApiException

from config import access_token, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# ══════════════════════════════════════════════════════════════════════════════
# MODE SWITCH  —  controlled by VIRTUAL_MODE environment variable
#   GitHub Actions: set via workflow_dispatch input (default: "true")
#   Local dev     : export VIRTUAL_MODE=true   (or false for live)
# ══════════════════════════════════════════════════════════════════════════════

VIRTUAL_MODE = os.environ.get("VIRTUAL_MODE", "true").lower() != "false"

print(
    f"[ORDER_MGR] Mode = {'🔵 VIRTUAL (paper trade)' if VIRTUAL_MODE else '🟢 LIVE (real orders)'}"
)

# ══════════════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════════════

LOT_SIZE             = 65      # NIFTY lot size (shares per lot)
TARGET_PNL           = 600     # ₹ profit target per trade
SL_PNL               = -300    # ₹ stop-loss per trade (negative)
NIFTY_STEP           = 50      # NIFTY strike step in index points
OPTION_POLL_INTERVAL = 62      # seconds between 1-min candle polls

# ══════════════════════════════════════════════════════════════════════════════
# Upstox SDK client  (used for market data queries only in virtual mode)
# ══════════════════════════════════════════════════════════════════════════════

_cfg              = upstox_client.Configuration()
_cfg.access_token = access_token
_api_client       = upstox_client.ApiClient(_cfg)

_options_api      = upstox_client.OptionsApi(_api_client)
_market_quote_api = upstox_client.MarketQuoteApi(_api_client)
_history_api      = upstox_client.HistoryV3Api(_api_client)

# Only instantiated when VIRTUAL_MODE is False
_order_api = upstox_client.OrderApiV3(_api_client)

# ══════════════════════════════════════════════════════════════════════════════
# One-trade-at-a-time guard
# ══════════════════════════════════════════════════════════════════════════════

_trade_lock   = threading.Lock()
_active_trade = None   # None when no position is open

# ══════════════════════════════════════════════════════════════════════════════
# Telegram
# ══════════════════════════════════════════════════════════════════════════════

def _send_telegram(text: str):
    """Post a message to the configured Telegram chat (best-effort)."""
    if (TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE"
            or TELEGRAM_CHAT_ID == "YOUR_CHAT_ID_HERE"):
        print("[TELEGRAM] Credentials not configured — skipping.")
        return
    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=data, timeout=10)
        if r.status_code == 200:
            print("[TELEGRAM] ✓ Alert sent.")
        else:
            print(f"[TELEGRAM] HTTP {r.status_code}: {r.text[:200]}")
    except Exception as exc:
        print(f"[TELEGRAM] Error: {exc}")


def _alert_signal_entry(signal: str, option_type: str, strike: int,
                        instr_key: str, entry_price: float,
                        expiry: str, signal_time: str):
    """
    Telegram Alert 1 — Signal generated + option instrument selected.
    """
    arrow  = "📈" if signal == "LONG" else "📉"
    mode   = "🔵 VIRTUAL" if VIRTUAL_MODE else "🟢 LIVE"
    target = entry_price + (TARGET_PNL / LOT_SIZE)
    sl_px  = entry_price + (SL_PNL    / LOT_SIZE)

    text = (
        f"{arrow} *SBT Signal — Option Selected* {mode}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Signal      : *{signal}*\n"
        f"🎯 Option      : *NIFTY {strike} {option_type}*\n"
        f"📅 Expiry      : `{expiry}`\n"
        f"🔑 Instrument  : `{instr_key}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Entry Price : `₹{entry_price:.2f}`\n"
        f"📦 Lot Size    : `{LOT_SIZE} shares (1 lot)`\n"
        f"🎯 Target      : `₹{target:.2f}` (+₹{TARGET_PNL})\n"
        f"🛑 Stop Loss   : `₹{sl_px:.2f}` (₹{SL_PNL})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Signal Time : `{signal_time}`"
    )
    print(f"\n[TELEGRAM] Sending Alert 1 — Signal Entry")
    _send_telegram(text)


def _alert_trade_result(signal: str, option_type: str, strike: int,
                        entry_price: float, exit_price: float,
                        pnl: float, exit_reason: str,
                        exit_ts: str, signal_time: str):
    """
    Telegram Alert 2 — Trade closed with result.
    """
    is_profit  = pnl >= 0
    result_tag = "✅ PROFIT" if is_profit else "❌ LOSS"
    emoji      = "🎯" if exit_reason == "TARGET" else "🛑"
    mode       = "🔵 VIRTUAL" if VIRTUAL_MODE else "🟢 LIVE"

    text = (
        f"{emoji} *Trade Closed — {exit_reason} HIT* {mode}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Signal      : *{signal}*\n"
        f"🎯 Option      : *NIFTY {strike} {option_type}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Entry Price : `₹{entry_price:.2f}`\n"
        f"💸 Exit Price  : `₹{exit_price:.2f}`\n"
        f"📈 P&L / share : `₹{exit_price - entry_price:+.2f}`\n"
        f"💼 Net P&L     : *₹{pnl:+.2f}*  ({LOT_SIZE} shares)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 Result      : *{result_tag}*\n"
        f"⏰ Exit Time   : `{exit_ts}`\n"
        f"⏰ Signal Time : `{signal_time}`"
    )
    print(f"\n[TELEGRAM] Sending Alert 2 — Trade Result  ({result_tag})")
    _send_telegram(text)


# ══════════════════════════════════════════════════════════════════════════════
# Expiry date helper
# ══════════════════════════════════════════════════════════════════════════════

def _nearest_thursday(base: datetime.date) -> datetime.date:
    """Return the nearest upcoming Thursday (inclusive of base)."""
    days = (3 - base.weekday()) % 7
    return base + datetime.timedelta(days=days)


def get_expiry_date() -> str:
    """Nearest NIFTY weekly expiry as 'YYYY-MM-DD'."""
    now    = datetime.datetime.now()
    today  = now.date()
    expiry = _nearest_thursday(today)
    # If today is Thursday and market is closed, roll to next Thursday
    if expiry == today and now.hour >= 15 and now.minute >= 30:
        expiry = _nearest_thursday(today + datetime.timedelta(days=1))
    return expiry.strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════════════════════
# Option chain — resolve instrument key
# ══════════════════════════════════════════════════════════════════════════════

def get_option_instrument_key(
    nifty_ltp: float, option_type: str
) -> tuple[str, int, float, str]:
    """
    Finds the ATM option contract for the nearest expiry.

    Returns
    -------
    (instrument_key, strike, chain_ltp, expiry_date)
    """
    expiry_date = get_expiry_date()
    atm_strike  = round(nifty_ltp / NIFTY_STEP) * NIFTY_STEP

    print(
        f"[ORDER] NIFTY LTP={nifty_ltp:.2f}  ATM={atm_strike}  "
        f"Type={option_type}  Expiry={expiry_date}"
    )

    try:
        resp = _options_api.get_put_call_option_chain(
            instrument_key="NSE_INDEX|Nifty 50",
            expiry_date=expiry_date,
        )
    except ApiException as exc:
        raise RuntimeError(f"Option chain API error: {exc}") from exc

    if not resp or not resp.data:
        raise RuntimeError("Option chain response is empty.")

    # Build strike → {CE_key, CE_ltp, PE_key, PE_ltp}
    chain: dict[int, dict] = {}
    for row in resp.data:
        s = int(row.strike_price)
        e = chain.setdefault(s, {})
        if row.call_options and row.call_options.market_data:
            e["CE_key"] = row.call_options.instrument_key
            e["CE_ltp"] = float(row.call_options.market_data.ltp or 0)
        if row.put_options and row.put_options.market_data:
            e["PE_key"] = row.put_options.instrument_key
            e["PE_ltp"] = float(row.put_options.market_data.ltp or 0)

    # Try ATM, then ±50 fallbacks
    for s in [atm_strike, atm_strike + NIFTY_STEP, atm_strike - NIFTY_STEP]:
        if s in chain and f"{option_type}_key" in chain[s]:
            return (
                chain[s][f"{option_type}_key"],
                s,
                chain[s].get(f"{option_type}_ltp", 0.0),
                expiry_date,
            )

    raise RuntimeError(
        f"No {option_type} found near strike {atm_strike} "
        f"for expiry {expiry_date}. "
        f"Available: {sorted(chain)[:10]}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Live LTP for entry price reference
# ══════════════════════════════════════════════════════════════════════════════

def get_option_ltp(instrument_key: str) -> float:
    """Fetch live LTP of an option via Market Quote API."""
    try:
        resp = _market_quote_api.ltp(instrument_key)
        if resp and resp.data:
            val = next(iter(resp.data.values()))
            return float(val.last_price)
    except Exception as exc:
        print(f"[ORDER] LTP fetch error: {exc}")
    return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Broker order placement  (REAL — only called when VIRTUAL_MODE = False)
# ══════════════════════════════════════════════════════════════════════════════

def _place_real_order(instrument_key: str, transaction_type: str = "BUY") -> str:
    """
    Place a MARKET intraday order via Upstox OrderApiV3.
    Only called when VIRTUAL_MODE = False.
    Returns order_id string.
    """
    order_body = upstox_client.PlaceOrderV3Request(
        quantity           = LOT_SIZE,
        product            = "I",          # Intraday / MIS
        validity           = "DAY",
        price              = 0.0,
        instrument_token   = instrument_key,
        order_type         = "MARKET",
        transaction_type   = transaction_type,
        disclosed_quantity = 0,
        trigger_price      = 0.0,
        is_amo             = False,
        slice              = False,
    )
    try:
        resp     = _order_api.place_order(body=order_body)
        order_id = resp.data.order_id
        print(f"[ORDER] REAL {transaction_type} placed → order_id={order_id}")
        return order_id
    except ApiException as exc:
        raise RuntimeError(f"place_order() failed: {exc}") from exc


# ══════════════════════════════════════════════════════════════════════════════
# 1-min option candle fetcher
# ══════════════════════════════════════════════════════════════════════════════

def _get_option_1min_candles(instrument_key: str) -> list[dict]:
    """
    Fetch today's 1-minute intraday candles for the option instrument.
    Returns a list of dicts sorted oldest → newest.
    Keys: datetime, open, high, low, close, volume.
    Returns [] on any error.
    """
    try:
        import pandas as pd
        resp    = _history_api.get_intra_day_candle_data(instrument_key, "1minute")
        candles = resp.data.candles if (resp and resp.data) else []
        if not candles:
            return []

        df = pd.DataFrame(
            candles,
            columns=["datetime", "open", "high", "low", "close", "volume", "OI"],
        )
        df["datetime"] = (
            pd.to_datetime(df["datetime"], utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )
        df = df.sort_values("datetime").reset_index(drop=True)
        return df.to_dict("records")

    except Exception as exc:
        print(f"[MONITOR] 1-min candle fetch error: {exc}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Background monitor — checks 1-min option candles for TP / SL
# ══════════════════════════════════════════════════════════════════════════════

def _monitor_trade(trade: dict):
    """
    Runs in a background daemon thread.

    Every OPTION_POLL_INTERVAL seconds:
      1. Fetch the latest 1-min intraday candles for the option.
      2. Use the most-recent closed candle's CLOSE as the current price.
      3. P&L = (close − entry_price) × LOT_SIZE
      4. TARGET_PNL hit → send Alert 2 (PROFIT) and close.
         SL_PNL     hit → send Alert 2 (LOSS)   and close.

    trade keys:
        instrument_key, strike, option_type, entry_price,
        expiry_date, signal, signal_time
    """
    global _active_trade

    instr      = trade["instrument_key"]
    entry      = trade["entry_price"]
    opt_type   = trade["option_type"]
    strike     = trade["strike"]
    sig_t      = trade["signal_time"]
    signal     = trade["signal"]

    print(
        f"[MONITOR] Started  —  {opt_type} {strike}  "
        f"Entry=₹{entry:.2f}  Target=+₹{TARGET_PNL}  SL=₹{SL_PNL}\n"
        f"[MONITOR] Polling 1-min option candles every {OPTION_POLL_INTERVAL}s …"
    )

    last_candle_ts = None

    while True:
        time.sleep(OPTION_POLL_INTERVAL)

        candles = _get_option_1min_candles(instr)
        if not candles:
            print("[MONITOR] No candle data yet — will retry next interval.")
            continue

        latest = candles[-1]
        ts     = latest["datetime"]

        if ts == last_candle_ts:
            print(f"[MONITOR] No new candle since {ts} — waiting …")
            continue

        last_candle_ts = ts
        cur_close      = float(latest["close"])
        pnl            = (cur_close - entry) * LOT_SIZE

        print(
            f"[MONITOR] 1-min candle @ {ts}  "
            f"O={latest['open']:.2f}  H={latest['high']:.2f}  "
            f"L={latest['low']:.2f}  C={cur_close:.2f}  "
            f"→ P&L = ₹{pnl:+.2f}"
        )

        exit_reason = None
        if pnl >= TARGET_PNL:
            exit_reason = "TARGET"
        elif pnl <= SL_PNL:
            exit_reason = "SL"

        if exit_reason:
            exit_ts = str(ts)

            # ── Square-off ──────────────────────────────────────────────────
            if VIRTUAL_MODE:
                print(
                    f"[MONITOR] [{exit_reason}] VIRTUAL square-off  "
                    f"Exit={cur_close:.2f}  P&L=₹{pnl:+.2f}"
                )
            else:
                try:
                    sq_id = _place_real_order(instr, transaction_type="SELL")
                    print(f"[MONITOR] REAL square-off order_id={sq_id}")
                except Exception as exc:
                    print(f"[MONITOR] ⚠️  Square-off FAILED: {exc}")
                    _send_telegram(
                        f"⚠️ *Square-off FAILED*\n"
                        f"Option : `{opt_type} {strike}`\n"
                        f"Error  : `{exc}`"
                    )

            # ── Alert 2 — Trade result ───────────────────────────────────────
            _alert_trade_result(
                signal      = signal,
                option_type = opt_type,
                strike      = strike,
                entry_price = entry,
                exit_price  = cur_close,
                pnl         = pnl,
                exit_reason = exit_reason,
                exit_ts     = exit_ts,
                signal_time = sig_t,
            )
            break   # exit monitor loop

    # Release trade slot
    with _trade_lock:
        _active_trade = None
    print("[MONITOR] Trade closed — ready for next signal.\n")


# ══════════════════════════════════════════════════════════════════════════════
# Public entry point  (called by websocket.py on every fresh SBT signal)
# ══════════════════════════════════════════════════════════════════════════════

def place_option_order(signal: str, nifty_ltp: float, signal_time: str):
    """
    Called by websocket.py when the SBT strategy fires a new signal
    on the latest closed 5-min NIFTY candle.

    Parameters
    ----------
    signal      : 'LONG' or 'SHORT'
    nifty_ltp   : NIFTY close price on the signal candle
    signal_time : human-readable timestamp string

    Behaviour
    ---------
    LONG  →  Virtual/Real BUY  CE (Call)  at ATM strike
    SHORT →  Virtual/Real BUY  PE (Put)   at ATM strike

    Only one active trade at a time; new signals are skipped until
    the running trade hits its TARGET or SL.
    """
    global _active_trade

    # ── Guard: skip if a trade is already running ──────────────────────────
    with _trade_lock:
        if _active_trade is not None:
            print(
                f"[ORDER] Signal '{signal}' ignored — trade already active: "
                f"{_active_trade['option_type']} {_active_trade['strike']}"
            )
            return

    option_type = "CE" if signal == "LONG" else "PE"
    mode_tag    = "[VIRTUAL]" if VIRTUAL_MODE else "[LIVE]"

    print(f"\n{'='*62}")
    print(f"  {mode_tag}  NEW SIGNAL : {signal}  →  {option_type}  |  {signal_time}")
    print(f"{'='*62}")

    try:
        # 1. Resolve option contract
        instr_key, strike, chain_ltp, expiry = get_option_instrument_key(
            nifty_ltp, option_type
        )
        print(
            f"[ORDER] Contract → NIFTY {strike} {option_type}  "
            f"Expiry={expiry}  Chain LTP=₹{chain_ltp:.2f}\n"
            f"[ORDER] Instrument key: {instr_key}"
        )

        # 2. Fetch live LTP for entry price
        live_ltp    = get_option_ltp(instr_key)
        entry_price = live_ltp if live_ltp > 0 else chain_ltp
        print(f"[ORDER] Entry price = ₹{entry_price:.2f}")

        # 3. Place order (virtual or real)
        if VIRTUAL_MODE:
            order_id = f"VIRTUAL-{datetime.datetime.now().strftime('%H%M%S')}"
            print(f"[ORDER] {mode_tag} BUY  NIFTY {strike} {option_type}  "
                  f"Qty={LOT_SIZE}  Price=₹{entry_price:.2f}  "
                  f"ref={order_id}")
        else:
            order_id = _place_real_order(instr_key, transaction_type="BUY")

        # 4. Store trade record
        trade = {
            "instrument_key": instr_key,
            "strike":         strike,
            "option_type":    option_type,
            "expiry_date":    expiry,
            "entry_price":    entry_price,
            "order_id":       order_id,
            "signal":         signal,
            "signal_time":    signal_time,
        }
        with _trade_lock:
            _active_trade = trade

        # 5. Alert 1 — Signal + entry details
        _alert_signal_entry(
            signal      = signal,
            option_type = option_type,
            strike      = strike,
            instr_key   = instr_key,
            entry_price = entry_price,
            expiry      = expiry,
            signal_time = signal_time,
        )

        # 6. Start background monitor (1-min option candles)
        t = threading.Thread(
            target = _monitor_trade,
            args   = (trade,),
            daemon = True,
            name   = f"monitor-{option_type}{strike}",
        )
        t.start()
        print(f"[ORDER] Monitor thread '{t.name}' started.\n")

    except Exception as exc:
        print(f"[ORDER] ❌ Failed: {exc}")
        _send_telegram(
            f"❌ *Order Setup FAILED* {mode_tag}\n"
            f"Signal : `{signal}`\n"
            f"NIFTY  : `{nifty_ltp:.2f}`\n"
            f"Error  : `{exc}`"
        )
