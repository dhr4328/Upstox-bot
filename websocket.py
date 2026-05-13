"""
websocket.py - Upstox live market data feed for Nifty 50.

Flow:
1. Load historical + intraday 5-minute seed candles.
2. Stream Nifty ticks from websocket and build live 5-minute candles.
3. Run Super Bollinger Trend on every closed candle.
4. On fresh signal, trigger option trade flow in order_manager.
"""

import asyncio
import datetime
import json
import ssl
import threading
import time

import numpy as np
import pandas as pd
import requests
import websockets

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, access_token

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
CANDLE_MINUTES = 5
STRATEGY_PERIOD = 12
STRATEGY_MULT = 2.0
IST_TZ = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

candles_5m = []
open_candle = {}
seed_open_candle = {}


def _ist_now_naive() -> datetime.datetime:
    return datetime.datetime.now(IST_TZ).replace(tzinfo=None)


def _bucket_start(ts: datetime.datetime, minutes: int = CANDLE_MINUTES) -> datetime.datetime:
    bucket_minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=bucket_minute, second=0, microsecond=0)


def _to_ist_naive_datetime(value):
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
    return dt.strftime(fmt) if dt else str(value)


async def send_telegram_alert(signal: str, close: float, sbt: float, ts_str: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] Credentials missing; skipping signal alert.")
        return

    arrow = "LONG" if signal == "LONG" else "SHORT"
    text = (
        f"Nifty 50 SBT Signal ({CANDLE_MINUTES}m)\n"
        f"Signal: {arrow}\n"
        f"Close: {close:.2f}\n"
        f"SBT: {sbt:.2f}\n"
        f"Time: {ts_str}"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text}

    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, lambda: requests.post(url, data=data, timeout=10))
        if response.status_code == 200:
            print(f"[TELEGRAM] Signal alert sent ({signal} @ {ts_str}).")
        else:
            print(f"[TELEGRAM] Signal alert failed: HTTP {response.status_code}")
    except Exception as exc:
        print(f"[TELEGRAM] Signal alert error: {exc}")


def load_historical_buffer() -> list:
    global seed_open_candle
    seed_open_candle = {}

    merged = []

    try:
        from buffer_data import df as hist_df

        if not hist_df.empty:
            for _, row in hist_df.iterrows():
                merged.append(
                    {
                        "time": row["datetime"],
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": float(row["volume"]),
                        "source": "historical",
                    }
                )
            print(f"[BUFFER] Historical seed loaded: {len(hist_df)} candles.")
    except Exception as exc:
        print(f"[BUFFER] Failed to load historical seed: {exc}")

    try:
        from intraday_data import get_intraday_candles

        intraday = get_intraday_candles(candle_minutes=CANDLE_MINUTES)
        merged.extend(intraday)
    except Exception as exc:
        print(f"[BUFFER] Failed to load intraday seed: {exc}")

    if not merged:
        return []

    merged = sorted(merged, key=lambda candle: candle["time"])
    deduped = []
    seen = set()
    for candle in merged:
        ts = candle["time"]
        if ts in seen:
            continue
        seen.add(ts)
        deduped.append(candle)

    now_bucket = _bucket_start(_ist_now_naive())
    if deduped and deduped[-1]["time"] >= now_bucket:
        seed_open_candle = deduped.pop().copy()
        print(
            "[BUFFER] Dropped incomplete seed candle at "
            f"{_format_ist_timestamp(seed_open_candle['time'], '%d-%b %H:%M IST')}."
        )

    if not deduped:
        print("[BUFFER] No closed seed candles available.")
        return []

    print(
        f"[BUFFER] Seed ready with {len(deduped)} closed {CANDLE_MINUTES}-minute candles "
        f"({_format_ist_timestamp(deduped[0]['time'], '%d-%b %H:%M')} -> "
        f"{_format_ist_timestamp(deduped[-1]['time'], '%d-%b %H:%M')})."
    )
    return deduped


def super_bollinger_trend(df: pd.DataFrame, period: int = STRATEGY_PERIOD, mult: float = STRATEGY_MULT) -> pd.DataFrame:
    df = df.copy()

    df["bb_up"] = df["High"].rolling(period).mean() + df["High"].rolling(period).std(ddof=0) * mult
    df["bb_dn"] = df["Low"].rolling(period).mean() - df["Low"].rolling(period).std(ddof=0) * mult

    sbt = np.zeros(len(df))
    signal = [None] * len(df)
    sbt[0] = float(df["Close"].iloc[0])

    for idx in range(1, len(df)):
        close = float(df["Close"].iloc[idx])
        prev_close = float(df["Close"].iloc[idx - 1])
        prev_sbt = sbt[idx - 1]
        bb_up = df["bb_up"].iloc[idx]
        bb_dn = df["bb_dn"].iloc[idx]

        if np.isnan(bb_up) or np.isnan(bb_dn):
            sbt[idx] = prev_sbt
            continue

        if close > prev_sbt:
            current_sbt = max(prev_sbt, float(bb_dn))
        else:
            current_sbt = min(prev_sbt, float(bb_up))

        if close > prev_sbt and prev_close <= prev_sbt:
            signal[idx] = "LONG"
            current_sbt = float(bb_dn)
        elif close < prev_sbt and prev_close >= prev_sbt:
            signal[idx] = "SHORT"
            current_sbt = float(bb_up)

        sbt[idx] = current_sbt

    df["SBT"] = sbt
    df["Signal"] = signal
    return df


async def run_strategy_and_print(label: str = "", closed_only: bool = False, fire_order: bool = True) -> pd.DataFrame:
    if closed_only:
        rows = list(candles_5m)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows).sort_values("time").reset_index(drop=True)
    else:
        df = get_full_df()

    if df.empty or len(df) < STRATEGY_PERIOD + 1:
        return df

    df = df.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
            "time": "Datetime",
        }
    )
    df = super_bollinger_trend(df)

    signal_rows = df[df["Signal"].notna()]
    if signal_rows.empty:
        print("[SBT] No signal yet.")
        return df

    last = signal_rows.iloc[-1]
    ts = last["Datetime"]
    ts_str = _format_ist_timestamp(ts, "%d-%b-%Y %H:%M IST")
    tag = f" [{label}]" if label else ""
    print(
        f"[SBT]{tag} Last signal={last['Signal']} "
        f"Time={ts_str} Close={last['Close']:.2f} SBT={last['SBT']:.2f}"
    )

    # Only act when newest closed candle generated the signal.
    if signal_rows.index[-1] != df.index[-1]:
        return df

    signal = str(last["Signal"])
    close = float(last["Close"])
    print(f"[SBT] LIVE SIGNAL {signal} at {ts_str} (Close={close:.2f})")

    await send_telegram_alert(signal=signal, close=close, sbt=float(last["SBT"]), ts_str=ts_str)

    if fire_order:
        from order_manager import place_option_order

        threading.Thread(
            target=place_option_order,
            args=(signal, close, ts_str),
            daemon=True,
            name=f"order-{signal}-{int(time.time())}",
        ).start()
    else:
        print("[SBT] Historical signal detected; order fire suppressed.")

    return df


def get_ws_url() -> str:
    if not access_token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is missing.")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    response = requests.get(AUTH_URL, headers=headers, allow_redirects=False, timeout=10)

    if response.status_code in (301, 302, 307, 308):
        redirect = response.headers.get("Location", "")
        if redirect:
            return redirect

    payload = response.json()
    ws_url = (
        payload.get("data", {}).get("authorizedRedirectUri")
        or payload.get("data", {}).get("uri")
        or payload.get("data", {}).get("authenticated_redirect_uri")
        or ""
    )
    if ws_url:
        return ws_url

    raise RuntimeError(f"Unable to authorize websocket URL: HTTP {response.status_code}")


def subscribe_message(instrument_keys: list[str]) -> bytes:
    payload = {
        "guid": f"ws-{int(time.time())}",
        "method": "sub",
        "data": {"mode": "ltpc", "instrumentKeys": instrument_keys},
    }
    return json.dumps(payload).encode("utf-8")


def decode_ltp(raw: bytes, instrument_key: str = INSTRUMENT_KEY):
    try:
        from upstox_client.feeder.proto import MarketDataFeedV3_pb2

        feed = MarketDataFeedV3_pb2.FeedResponse()
        feed.ParseFromString(raw)

        tick = feed.feeds.get(instrument_key)
        if tick and tick.HasField("ltpc"):
            return float(tick.ltpc.ltp)
    except Exception as exc:
        print(f"[WS] Decode error: {exc}")
    return None


def update_candle(ltp: float):
    global open_candle

    now = _ist_now_naive()
    candle_ts = _bucket_start(now)

    if candles_5m:
        last_ts = candles_5m[-1]["time"]
        if candle_ts <= last_ts:
            return

    if not open_candle:
        open_candle = {
            "time": candle_ts,
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "volume": 0,
            "source": "live",
        }
        print(f"[LIVE CANDLE] Opened {_format_ist_timestamp(candle_ts, '%d-%b %H:%M IST')} O={ltp:.2f}")
        return

    if candle_ts > open_candle["time"]:
        closed = open_candle.copy()
        candles_5m.append(closed)

        print(
            f"[LIVE CANDLE] Closed {_format_ist_timestamp(closed['time'], '%d-%b %H:%M IST')} "
            f"O={closed['open']:.2f} H={closed['high']:.2f} "
            f"L={closed['low']:.2f} C={closed['close']:.2f} "
            f"[Total={len(candles_5m)}]"
        )

        loop = asyncio.get_running_loop()
        loop.create_task(
            run_strategy_and_print(
                label=closed["time"].strftime("%H:%M"),
                closed_only=True,
            )
        )

        open_candle = {
            "time": candle_ts,
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "volume": 0,
            "source": "live",
        }
        return

    open_candle["high"] = max(open_candle["high"], ltp)
    open_candle["low"] = min(open_candle["low"], ltp)
    open_candle["close"] = ltp


def get_full_df() -> pd.DataFrame:
    rows = list(candles_5m)
    if open_candle:
        rows.append(open_candle.copy())

    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values("time").reset_index(drop=True)
    return frame


async def run():
    ws_url = get_ws_url()
    ssl_ctx = ssl.create_default_context()

    print("[WS] Connecting to market feed...")
    async with websockets.connect(ws_url, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(subscribe_message([INSTRUMENT_KEY]))
        print(f"[WS] Subscribed to {INSTRUMENT_KEY}. Waiting for ticks...")

        async for message in ws:
            if isinstance(message, bytes):
                ltp = decode_ltp(message, INSTRUMENT_KEY)
                if ltp is not None:
                    update_candle(ltp)


if __name__ == "__main__":
    candles_5m = load_historical_buffer()
    if seed_open_candle:
        open_candle = seed_open_candle.copy()
        print(
            "[BUFFER] Seeded live open candle at "
            f"{_format_ist_timestamp(open_candle['time'], '%d-%b %H:%M IST')}"
        )

    print("=" * 64)
    print(f"  Upstox | Nifty 50 | {CANDLE_MINUTES}-Minute OHLC (Live + Seed)")
    print(f"  Strategy: Super Bollinger Trend (period={STRATEGY_PERIOD}, mult={STRATEGY_MULT})")
    print("  Orders: LONG->CE SHORT->PE | Virtual trade with live option websocket")
    print("=" * 64)
    print(f"  Seed candles loaded: {len(candles_5m)}")
    if candles_5m:
        print(f"  Last seed candle: {_format_ist_timestamp(candles_5m[-1]['time'])}")

    print("[SBT] Running historical scan...")
    asyncio.run(run_strategy_and_print(label="HISTORICAL", fire_order=False))
    print("[SBT] Startup scan complete. Press Ctrl+C to stop.\n")

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[WS] Stopped by user.")
        final_df = asyncio.run(run_strategy_and_print(label="FINAL"))
        if not final_df.empty:
            cols = ["Datetime", "Open", "High", "Low", "Close", "SBT", "Signal"]
            final_cols = [col for col in cols if col in final_df.columns]
            print(final_df[final_cols].tail(10).to_string(index=False))
