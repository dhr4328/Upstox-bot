"""
intraday_data.py  —  Fetch today's missed 5-min candles when bot starts late.

Logic
─────
• Market opens at 09:15 IST.
• If the bot is started AT or BEFORE 09:15, this module returns an empty list
  — the WebSocket will build all candles from scratch.
• If the bot is started AFTER 09:15 (e.g. 10:00 AM), this module fetches
  all 5-min intraday candles from 09:15 up to the current time so the live
  feed doesn't have to "catch up" from zero.

Exposes
───────
    get_intraday_candles() → list[dict]
        Each dict has keys: time, open, high, low, close, volume, source="intraday"
        Sorted oldest → newest (9:15 first).

Usage (from websocket.py or buffer_data.py)
────────────────────────────────────────────
    from intraday_data import get_intraday_candles
    candles = get_intraday_candles()   # [] if started at/before 9:15
"""

import datetime

import pandas as pd
import upstox_client

from config import access_token, data_token

# ── Settings ──────────────────────────────────────────────────────────────────

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
MARKET_OPEN    = datetime.time(9, 15)   # 09:15 IST
IST_TZ         = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def _ist_now_naive() -> datetime.datetime:
    """
    Return current IST wall-clock time as a timezone-naive datetime.
    Candle timestamps in this project are stored as naive IST values, so we
    keep that convention for safe comparisons.
    """
    return datetime.datetime.now(IST_TZ).replace(tzinfo=None)

# ── Upstox API client ─────────────────────────────────────────────────────────

_configuration             = upstox_client.Configuration()
_configuration.access_token = data_token if data_token else access_token
_client                    = upstox_client.ApiClient(_configuration)
_intraday_api              = upstox_client.HistoryV3Api(_client)

# ─────────────────────────────────────────────────────────────────────────────
# Internal helper
# ─────────────────────────────────────────────────────────────────────────────

def _candles_to_df(candles) -> pd.DataFrame:
    """Normalise a raw candle list (from Upstox) into a clean DataFrame."""
    if not candles:
        return pd.DataFrame()
    frame = pd.DataFrame(
        candles,
        columns=["datetime", "open", "high", "low", "close", "volume", "OI"],
    )
    frame.drop(columns=["OI"], inplace=True)
    frame["datetime"] = (
        pd.to_datetime(frame["datetime"], utc=True)
        .dt.tz_convert("Asia/Kolkata")
        .dt.tz_localize(None)
    )
    return frame


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_intraday_candles() -> list:
    """
    Return today's 5-min intraday candles as a list of dicts
    (sorted oldest → newest) **only** when the bot starts after 09:15 IST.

    Returns [] (empty list) if:
      • The current time is 09:15 or earlier  →  WebSocket starts fresh.
      • The API call fails.
      • No intraday data is available yet (pre-market / holiday).
    """
    now     = _ist_now_naive()
    now_t   = now.time()

    # ── Case: Bot started at/before market open — skip entirely ──────────────
    if now_t <= MARKET_OPEN:
        print(
            f"[INTRADAY] Bot started at {now_t.strftime('%H:%M')} "
            f"(≤ 09:15) — skipping intraday prefetch; WebSocket will build data live."
        )
        return []

    # ── Case: Bot started after market open — fetch missed candles ────────────
    minutes_elapsed = (now.hour * 60 + now.minute) - (9 * 60 + 15)
    candles_expected = minutes_elapsed // 5
    print(
        f"[INTRADAY] Bot started at {now_t.strftime('%H:%M')} — market has been open "
        f"~{minutes_elapsed} min  ({candles_expected} expected 5-min candles). Fetching …"
    )

    try:
        resp = _intraday_api.get_intra_day_candle_data(
            INSTRUMENT_KEY,
            "5minute",   # interval string accepted by Upstox HistoryV3Api
        )
        raw_df = _candles_to_df(resp.data.candles)

        if raw_df.empty:
            print("[INTRADAY] API returned no candles (holiday or pre-market?).")
            return []

        # Reverse so oldest candle is first (Upstox returns newest first)
        raw_df = raw_df.iloc[::-1].reset_index(drop=True)

        # Convert to list-of-dicts expected by websocket.py / buffer_data.py
        candle_list = []
        for _, row in raw_df.iterrows():
            candle_list.append({
                "time":   row["datetime"],
                "open":   float(row["open"]),
                "high":   float(row["high"]),
                "low":    float(row["low"]),
                "close":  float(row["close"]),
                "volume": float(row["volume"]),
                "source": "intraday",
            })

        print(
            f"[INTRADAY] Fetched {len(candle_list)} candle(s)  "
            f"({candle_list[0]['time'].strftime('%H:%M')}  →  "
            f"{candle_list[-1]['time'].strftime('%H:%M')})"
        )
        return candle_list

    except Exception as exc:
        print(f"[INTRADAY] API error — {exc}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Standalone test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    candles = get_intraday_candles()
    if candles:
        print(f"\n{'─'*55}")
        print(f"{'Time':>8}   {'Open':>10}  {'High':>10}  {'Low':>10}  {'Close':>10}")
        print(f"{'─'*55}")
        for c in candles[-10:]:   # show last 10
            print(
                f"{c['time'].strftime('%H:%M'):>8}   "
                f"{c['open']:>10.2f}  {c['high']:>10.2f}  "
                f"{c['low']:>10.2f}  {c['close']:>10.2f}"
            )
    else:
        print("\n[INTRADAY] No intraday candles returned.")
