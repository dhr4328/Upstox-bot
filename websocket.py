"""
websocket.py  —  Upstox Live WebSocket Feed (Nifty 50)

Flow:
  1. Load historical 5-min OHLC candles from buffer_data.py  (seed)
  2. Connect to Upstox WebSocket, subscribe to Nifty 50 LTP ticks
  3. Continuously build live 5-min candles and APPEND them after the
     historical data so the full dataset is always contiguous.
  4. On every 5-min candle close, run Super Bollinger Trend strategy.
  5. If a fresh signal fires → place an option order via order_manager.
     The option P&L is then monitored on a 1-min candle feed.
"""

import asyncio
import json
import ssl
import time
import datetime

import numpy as np
import pandas as pd
import requests
import websockets

from config import access_token, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
# order_manager is imported lazily inside run_strategy_and_print()
# to avoid circular-import issues at module load time.

# ── Settings ──────────────────────────────────────────────────────────────────

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
AUTH_URL       = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
CANDLE_MINUTES = 5   # NIFTY live candle timeframe (5 minutes)

IST_TZ         = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def _ist_now_naive() -> datetime.datetime:
    """
    Return current IST wall-clock time as a timezone-naive datetime.
    Candle timestamps in this bot are stored as naive IST values.
    """
    return datetime.datetime.now(IST_TZ).replace(tzinfo=None)


def _to_ist_naive_datetime(value):
    """Normalize datetime-like values to naive IST datetime for formatting."""
    if value is None:
        return None

    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()

    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(IST_TZ).replace(tzinfo=None)

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    if getattr(parsed, "tzinfo", None) is not None:
        parsed = parsed.tz_convert(IST_TZ).tz_localize(None)
    return parsed.to_pydatetime()


def _format_ist_timestamp(value, fmt: str = "%d-%b-%Y %H:%M:%S IST") -> str:
    dt = _to_ist_naive_datetime(value)
    if dt is None:
        return str(value)
    return dt.strftime(fmt)

# ── Telegram ──────────────────────────────────────────────────────────────────

async def send_telegram_alert(signal: str, close: float, sbt: float, ts_str: str):
    """
    Send a formatted signal alert to the configured Telegram chat.
    Silently swallows errors so the main loop is never interrupted.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] Credentials not set — skipping alert.")
        return

    arrow  = "\U0001F7E2 \U000025B2" if signal == "LONG" else "\U0001F534 \U000025BC"   # 🟢▲ / 🔴▼
    text = (
        f"{arrow}  *Nifty 50 — SBT Signal*\n"
        f"Signal  : *{signal}*\n"
        f"Close   : `{close:.2f}`\n"
        f"SBT     : `{sbt:.2f}`\n"
        f"Time    : `{ts_str}`\n"
        f"#NiftyAlert #SuperBollingerTrend"
    )
    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}

    try:
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(
            None,
            lambda: requests.post(url, data=data, timeout=10)
        )
        if resp.status_code == 200:
            print(f"[TELEGRAM] Alert sent  ✓  ({signal}  {ts_str})")
        else:
            print(f"[TELEGRAM] Failed — HTTP {resp.status_code}: {resp.text}")
    except Exception as exc:
        print(f"[TELEGRAM] Error sending alert: {exc}")

# ── Step 1 : Seed from historical buffer ──────────────────────────────────────

def load_historical_buffer():
    global seed_open_candle
    seed_open_candle = {}
    """
    Build the seed candle list for the live feed:

    1. Multi-day historical candles from buffer_data.py  (past sessions)
    2. Today's intraday 5-min candles from intraday_data.py
       — fetched ONLY when the bot starts after 09:15 IST;
         returns [] automatically if started at/before 09:15.

    The two sources are merged and de-duplicated by timestamp so the
    WebSocket always has a complete, gap-free buffer regardless of
    what time the bot is launched.

    Returns a list of dicts sorted oldest → newest, each with keys:
        time, open, high, low, close, volume, source
    """
    # ── Part 1 : Multi-day historical candles ─────────────────────────────────
    hist_candles = []
    try:
        from buffer_data import df as hist_df   # triggers the API fetch
        if not hist_df.empty:
            for _, row in hist_df.iterrows():
                hist_candles.append({
                    "time":   row["datetime"],
                    "open":   float(row["open"]),
                    "high":   float(row["high"]),
                    "low":    float(row["low"]),
                    "close":  float(row["close"]),
                    "volume": float(row["volume"]),
                    "source": "historical",
                })
            print(f"[BUFFER] Historical : {len(hist_candles)} candles loaded.")
        else:
            print("[BUFFER] Historical data is empty — starting from scratch.")
    except Exception as exc:
        print(f"[BUFFER] Could not load historical data: {exc}")

    # ── Part 2 : Today's intraday candles (only when started after 09:15) ─────
    intraday_candles = []
    try:
        from intraday_data import get_intraday_candles
        intraday_candles = get_intraday_candles()   # [] if started at/before 09:15
    except Exception as exc:
        print(f"[INTRADAY] Could not load intraday data: {exc}")

    # ── Merge + de-duplicate by timestamp ─────────────────────────────────────
    all_candles = hist_candles + intraday_candles
    if not all_candles:
        return []

    seen      = set()
    unique    = []
    for c in sorted(all_candles, key=lambda x: x["time"]):
        ts = c["time"]
        if ts not in seen:
            seen.add(ts)
            unique.append(c)

    # ── Drop the last candle if it belongs to the current open 5-min bucket ───
    # The Upstox intraday API often returns the still-forming candle as its
    # last entry.  Using an incomplete candle as a "closed" seed produces wrong
    # OHLC values, which shifts the SBT line and causes missed / false signals.
    # We strip it here; the WebSocket live feed will build that candle properly.
    now        = _ist_now_naive()
    minute_mod = now.minute - (now.minute % 5)
    open_bucket_ts = now.replace(minute=minute_mod, second=0, microsecond=0)
    if unique and unique[-1]["time"] >= open_bucket_ts:
        dropped = unique.pop()
        seed_open_candle = dropped.copy()
        print(
            f"[BUFFER] Dropped incomplete seed candle @ "
            f"{dropped['time'].strftime('%d-%b %H:%M')} "
            f"(current open bucket: {open_bucket_ts.strftime('%H:%M')})"
        )

    if not unique:
        print("[BUFFER] No closed seed candles available — starting from scratch.")
        return []

    print(
        f"[BUFFER] Seeded {len(unique)} closed candles into live feed  "
        f"({unique[0]['time'].strftime('%d-%b %H:%M')}  →  "
        f"{unique[-1]['time'].strftime('%d-%b %H:%M')})\n"
    )
    return unique

# ── Strategy ─────────────────────────────────────────────────────────────────

def superBoilingerTrend(df, period=12, mult=2.0):
    """
    Super Bollinger Trend indicator — matches TradingView's Pine Script exactly.

    Key implementation notes vs TradingView:
      • ddof=0  : TradingView ta.stdev() uses population std (÷N), NOT sample
                  std (÷N-1).  pandas rolling().std() defaults to ddof=1, which
                  makes bands ~4 % wider and shifts the SBT line away from TV.
      • sbt[0] initialised to first Close (not 0).  TV uses `var float trend = close`
                  which seeds the SBT at the very first bar's close.
      • Crossover: `prev_close <= prev_sbt` / `prev_close >= prev_sbt` matches
                  TV's  `close[1] <= trend[1]` / `close[1] >= trend[1]`.

    Expects a DataFrame with columns: High, Low, Close.
    Adds columns: bb_up, bb_dn, SBT, Signal.
    Signal values: 'LONG', 'SHORT', or None.
    """
    df = df.copy()

    # ── Bollinger bands — ddof=0 to match TradingView ta.stdev() ─────────────
    df["bb_up"] = (
        df["High"].rolling(period).mean()
        + df["High"].rolling(period).std(ddof=0) * mult
    )
    df["bb_dn"] = (
        df["Low"].rolling(period).mean()
        - df["Low"].rolling(period).std(ddof=0) * mult
    )

    sbt    = np.zeros(len(df))
    signal = [None] * len(df)

    # ── Seed SBT at first bar's close (mirrors TV `var float trend = close`) ──
    sbt[0] = float(df["Close"].iloc[0])

    for i in range(1, len(df)):
        close      = float(df["Close"].iloc[i])
        prev_close = float(df["Close"].iloc[i - 1])
        prev_sbt   = sbt[i - 1]
        bb_up      = df["bb_up"].iloc[i]
        bb_dn      = df["bb_dn"].iloc[i]

        # Before the rolling window fills, just carry forward the SBT
        if np.isnan(bb_up) or np.isnan(bb_dn):
            sbt[i] = prev_sbt
            continue

        # ── Update SBT level (uptrend = lower band, downtrend = upper band) ───
        if close > prev_sbt:
            current_sbt = max(prev_sbt, float(bb_dn))
        else:
            current_sbt = min(prev_sbt, float(bb_up))

        # ── Crossover detection — matches TV `close[1] <= trend[1]` logic ─────
        # LONG  : previous close was at or below SBT AND current close is above
        # SHORT : previous close was at or above SBT AND current close is below
        if close > prev_sbt and prev_close <= prev_sbt:
            signal[i]   = "LONG"
            current_sbt = float(bb_dn)
        elif close < prev_sbt and prev_close >= prev_sbt:
            signal[i]   = "SHORT"
            current_sbt = float(bb_up)

        sbt[i] = current_sbt

    df["SBT"]    = sbt
    df["Signal"] = signal
    return df


async def run_strategy_and_print(label: str = "", closed_only: bool = False,
                                  fire_order: bool = True):
    """
    Build the OHLC DataFrame, run SuperBoilingerTrend on it,
    print the latest signal, and send a Telegram alert if the
    freshest closed candle itself generated the signal.

    closed_only=True  → use only completed candles (candles_5m), no open candle.
                         Used on every live candle-close so the just-closed candle
                         is always df.index[-1] and signal detection is correct.
    closed_only=False → use get_full_df() which includes the in-progress candle.
                         Used for the startup historical scan.
    fire_order=False  → print/alert only; do NOT place an option order.
                         Used during the startup historical scan to avoid placing
                         stale orders on old candles.

    Returns the strategy-applied DataFrame.
    """
    if closed_only:
        # Build df from completed candles only — avoids the in-progress open_candle
        # being appended as the last row and breaking the last_idx check.
        rows = list(candles_5m)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df.sort_values("time").reset_index(drop=True)
    else:
        df = get_full_df()

    if df.empty or len(df) < 13:   # need at least period+1 rows
        return df

    # Rename to title-case so the strategy function works unchanged
    df = df.rename(columns={"open": "Open", "high": "High",
                             "low": "Low",  "close": "Close",
                             "volume": "Volume", "time": "Datetime"})

    df = superBoilingerTrend(df)

    # ── Print the last signal in the series ───────────────────────────────────
    sig_rows = df[df["Signal"].notna()]
    if not sig_rows.empty:
        last = sig_rows.iloc[-1]
        ts   = last["Datetime"]
        ts_s = _format_ist_timestamp(ts, "%d-%b-%Y %H:%M IST")
        tag  = f"  [{label}]" if label else ""
        print(
            f"[SBT]{tag} Last signal → {last['Signal']:5s}  "
            f"@ {ts_s}  Close={last['Close']:.2f}  SBT={last['SBT']:.2f}"
        )

        # ── Fire Telegram alert + option order only when the LAST ROW fired the signal
        # When closed_only=True, df.index[-1] is always the just-closed candle.
        last_idx = df.index[-1]
        if sig_rows.index[-1] == last_idx:
            sig   = last['Signal']
            close = float(last['Close'])
            arrow = "\u25b2" if sig == "LONG" else "\u25bc"
            print(
                f"  {'='*55}\n"
                f"  {arrow}  LIVE SIGNAL : {sig}  |  Close={close:.2f}"
                f"  |  {ts_s}\n"
                f"  {'='*55}"
            )
            # Send Telegram signal alert
            await send_telegram_alert(
                signal = sig,
                close  = close,
                sbt    = float(last['SBT']),
                ts_str = ts_s,
            )
            # ── Place option order only for live signals (not historical replay) ──
            if fire_order:
                import threading
                from order_manager import place_option_order   # lazy import
                threading.Thread(
                    target = place_option_order,
                    args   = (sig, close, ts_s),
                    daemon = True,
                ).start()
            else:
                print("[SBT] Historical signal — order placement suppressed.")
    else:
        print("[SBT] No signal yet.")

    return df


# ── Storage (populated at startup inside __main__) ───────────────────────────

candles_5m  = []   # completed candles (hist + live) — filled in __main__
open_candle = {}   # current in-progress live candle
seed_open_candle = {}

# ── Step 2 : Get the WebSocket URL ────────────────────────────────────────────

def get_ws_url():
    if not access_token:
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is not set or is empty. "
            "Add it as a GitHub Secret (Settings → Secrets and variables → Actions) "
            "or export it in your shell before running."
        )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    }
    resp = requests.get(AUTH_URL, headers=headers, allow_redirects=False, timeout=10)

    if resp.status_code in (301, 302, 307, 308):
        url = resp.headers.get("Location", "")
        if url:
            print("[WS] Got WebSocket URL (redirect)")
            return url

    data   = resp.json()
    ws_url = (
        data.get("data", {}).get("authorizedRedirectUri") or
        data.get("data", {}).get("uri") or
        data.get("data", {}).get("authenticated_redirect_uri") or ""
    )
    if ws_url:
        print("[WS] Got WebSocket URL (JSON body)")
        return ws_url

    raise RuntimeError(f"Could not get WebSocket URL. Status={resp.status_code}")

# ── Step 3 : Build the subscription message ───────────────────────────────────

def subscribe_message():
    payload = {
        "guid":   f"ws-{int(time.time())}",
        "method": "sub",
        "data": {
            "mode":           "ltpc",
            "instrumentKeys": [INSTRUMENT_KEY],
        },
    }
    return json.dumps(payload).encode("utf-8")

# ── Step 4 : Decode the binary Protobuf message ───────────────────────────────

def decode_message(raw: bytes):
    """Return (ltp, volume_traded) or (None, None) if decoding fails."""
    try:
        from upstox_client.feeder.proto import MarketDataFeedV3_pb2
        feed = MarketDataFeedV3_pb2.FeedResponse()
        feed.ParseFromString(raw)

        for key, val in feed.feeds.items():
            if key == INSTRUMENT_KEY and val.HasField("ltpc"):
                ltp = val.ltpc.ltp
                return ltp
    except Exception as exc:
        print(f"[WS] Decode error: {exc}")
    return None

# ── Step 5 : Update the live 5-minute candle ──────────────────────────────────

def update_candle(ltp: float):
    """
    Feed one LTP tick into the current 5-minute candle.
    When the minute-bucket rolls over, close the current candle, append it to
    candles_5m (right after the historical data), and open a fresh one.
    """
    global open_candle

    now        = _ist_now_naive()
    minute_mod = now.minute - (now.minute % CANDLE_MINUTES)
    candle_ts  = now.replace(minute=minute_mod, second=0, microsecond=0)

    # ── Guard: ignore ticks whose 5-min bucket is already in the buffer ──────
    if candles_5m:
        last_buffered_ts = candles_5m[-1]["time"]
        if candle_ts <= last_buffered_ts:
            return   # bucket already covered by hist or intraday seed; skip

    if not open_candle:
        open_candle = {
            "time": candle_ts, "open": ltp, "high": ltp,
            "low": ltp, "close": ltp, "volume": 0, "source": "live",
        }
        print(f"\n[LIVE CANDLE] {candle_ts.strftime('%d-%b %H:%M')}  "
              f"Opened  O={ltp}")
        return

    if candle_ts > open_candle["time"]:
        # ── Close the finished candle and append to the unified list ──────────
        candles_5m.append(open_candle.copy())
        c = open_candle
        print(
            f"[LIVE CANDLE] Closed {c['time'].strftime('%d-%b %H:%M')}  "
            f"O={c['open']}  H={c['high']}  L={c['low']}  C={c['close']}  "
            f"[Total: {len(candles_5m)}]"
        )

        # ── Run strategy BEFORE opening new candle so candles_5m[-1] is the
        #    just-closed row and closed_only=True makes it df.index[-1]. ─────
        asyncio.get_event_loop().create_task(
            run_strategy_and_print(
                label       = c['time'].strftime('%H:%M'),
                closed_only = True,   # ← ensures signal check works correctly
            )
        )

        # Open the next candle (after scheduling strategy so it doesn't appear
        # in the closed-only df used by the strategy task above)
        open_candle = {
            "time": candle_ts, "open": ltp, "high": ltp,
            "low": ltp, "close": ltp, "volume": 0, "source": "live",
        }
    else:
        # Same 5-min bucket — update high / low / close
        open_candle["high"]  = max(open_candle["high"],  ltp)
        open_candle["low"]   = min(open_candle["low"],   ltp)
        open_candle["close"] = ltp

# ── Utility: get the full combined DataFrame at any time ──────────────────────

def get_full_df() -> pd.DataFrame:
    """
    Returns a DataFrame of ALL candles (historical + completed live),
    sorted oldest → newest.
    """
    rows = list(candles_5m)
    if open_candle:
        rows.append(open_candle.copy())   # include the in-progress candle
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("time").reset_index(drop=True)
    return df

# ── Step 6 : Connect and listen ───────────────────────────────────────────────

async def run():
    ws_url  = get_ws_url()
    ssl_ctx = ssl.create_default_context()

    print("[WS] Connecting …")
    async with websockets.connect(ws_url, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(subscribe_message())
        print("[WS] Subscribed to Nifty 50. Waiting for ticks …\n")

        async for message in ws:
            if isinstance(message, bytes):
                ltp = decode_message(message)
                if ltp:
                    update_candle(ltp)

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # ── Initialise candle buffer (must happen before WebSocket starts) ────────
    candles_5m = load_historical_buffer()
    if seed_open_candle:
        open_candle = seed_open_candle.copy()
        print(
            f"[BUFFER] Live open candle seeded @ "
            f"{_format_ist_timestamp(open_candle['time'], '%d-%b %H:%M IST')}"
        )
    print("=" * 60)
    print("  Upstox  |  Nifty 50  |  1-Minute OHLC  (Live + Historical)")
    print("  Strategy : Super Bollinger Trend  (period=12, mult=2.0)")
    print("  Orders   : LONG→CE  SHORT→PE  | Target=+₹600  SL=-₹300")
    print("=" * 60)
    print(f"  Historical candles loaded : {len(candles_5m)}")
    if candles_5m:
        print(f"  Last historical candle    : {_format_ist_timestamp(candles_5m[-1]['time'])}")
    print()

    # ── Run strategy on the historical data once at startup ─────────────────
    # fire_order=False: prevents stale signals on old candles from triggering
    # real option orders at bot startup.
    print("[SBT] Running strategy on historical buffer …")
    asyncio.run(run_strategy_and_print(label="HISTORICAL", fire_order=False))
    print()
    print("  Press Ctrl+C to stop.\n")

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[WS] Stopped.")
        full = get_full_df()
        print(f"\nTotal candles in buffer (hist + live): {len(full)}")
        # Show last 10 candles with SBT column
        strat_df = asyncio.run(run_strategy_and_print(label="FINAL"))
        if not strat_df.empty:
            cols = ["Datetime", "Open", "High", "Low", "Close", "SBT", "Signal"]
            print(strat_df[[c for c in cols if c in strat_df.columns]].tail(10).to_string(index=False))
