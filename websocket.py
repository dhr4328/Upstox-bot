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
CANDLE_MINUTES = 5   # NIFTY live candle timeframe — always 5 min

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

    print(
        f"[BUFFER] Seeded {len(unique)} candles into live feed  "
        f"({unique[0]['time'].strftime('%d-%b %H:%M')}  →  "
        f"{unique[-1]['time'].strftime('%d-%b %H:%M')})\n"
    )
    return unique

# ── Strategy ─────────────────────────────────────────────────────────────────

def superBoilingerTrend(df, period=12, mult=2.0):
    """
    Super Bollinger Trend indicator.
    Expects a DataFrame with columns: High, Low, Close.
    Adds columns: bb_up, bb_dn, SBT, Signal.
    Signal values: 'LONG', 'SHORT', or None.
    """
    df = df.copy()
    df["bb_up"] = df["High"].rolling(period).mean() + df["High"].rolling(period).std() * mult
    df["bb_dn"] = df["Low"].rolling(period).mean()  - df["Low"].rolling(period).std()  * mult

    sbt    = np.zeros(len(df))
    signal = [None] * len(df)

    for i in range(1, len(df)):
        close      = float(df["Close"].iloc[i])
        prev_close = float(df["Close"].iloc[i - 1])
        prev_sbt   = sbt[i - 1]
        bb_up      = df["bb_up"].iloc[i]
        bb_dn      = df["bb_dn"].iloc[i]

        if np.isnan(bb_up) or np.isnan(bb_dn):
            sbt[i] = prev_sbt
            continue

        if close > prev_sbt:
            current_sbt = max(prev_sbt, float(bb_dn))
        else:
            current_sbt = min(prev_sbt, float(bb_up))

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


async def run_strategy_and_print(label: str = "", closed_only: bool = False):
    """
    Build the OHLC DataFrame, run SuperBoilingerTrend on it,
    print the latest signal, and send a Telegram alert if the
    freshest closed candle itself generated the signal.

    closed_only=True  → use only completed candles (candles_5m), no open candle.
                         Used on every live candle-close so the just-closed candle
                         is always df.index[-1] and signal detection is correct.
    closed_only=False → use get_full_df() which includes the in-progress candle.
                         Used for the startup historical scan.

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
        ts_s = ts.strftime("%d-%b %H:%M") if hasattr(ts, "strftime") else str(ts)
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
            # ── Place option order (non-blocking — runs in background thread) ──
            import threading
            from order_manager import place_option_order   # lazy import
            threading.Thread(
                target = place_option_order,
                args   = (sig, close, ts_s),
                daemon = True,
            ).start()
    else:
        print("[SBT] No signal yet.")

    return df


# ── Storage (populated at startup inside __main__) ───────────────────────────

candles_5m  = []   # completed candles (hist + live) — filled in __main__
open_candle = {}   # current in-progress live candle

# ── Step 2 : Get the WebSocket URL ────────────────────────────────────────────

def get_ws_url():
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

    now        = datetime.datetime.now()
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
            "low": ltp, "close": ltp, "source": "live",
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
            "low": ltp, "close": ltp, "source": "live",
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
    print("=" * 60)
    print("  Upstox  |  Nifty 50  |  1-Minute OHLC  (Live + Historical)")
    print("  Strategy : Super Bollinger Trend  (period=12, mult=2.0)")
    print("  Orders   : LONG→CE  SHORT→PE  | Target=+₹600  SL=-₹300")
    print("=" * 60)
    print(f"  Historical candles loaded : {len(candles_5m)}")
    if candles_5m:
        print(f"  Last historical candle    : {candles_5m[-1]['time']}")
    print()

    # ── Run strategy on the historical data once at startup ───────────────────
    print("[SBT] Running strategy on historical buffer …")
    asyncio.run(run_strategy_and_print(label="HISTORICAL"))
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
