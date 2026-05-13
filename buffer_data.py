"""
buffer_data.py  —  Fetch 1-minute OHLC candles for Nifty 50.

Two-pass fetch:
  1. Historical API  → last N trading days (9:15 → 3:30, closed sessions)
  2. Intraday API    → today's candles so far (9:15 → current time)

Exposes:
    df  — pandas DataFrame [datetime, open, high, low, close, volume]
          sorted oldest → newest, no gaps, ready to seed the live feed.
"""

import datetime

import pandas as pd
import upstox_client

from config import access_token, data_token

# ── Instrument (must match websocket.py) ──────────────────────────────────────

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
IST_TZ         = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def _ist_now() -> datetime.datetime:
    """Current timezone-aware IST datetime."""
    return datetime.datetime.now(IST_TZ)

# ── Upstox API client ─────────────────────────────────────────────────────────

configuration = upstox_client.Configuration()
configuration.access_token = data_token if data_token else access_token
_client = upstox_client.ApiClient(configuration)

hist_api     = upstox_client.HistoryV3Api(_client)
intraday_api = upstox_client.HistoryV3Api(_client)   # same class handles both

df = pd.DataFrame()   # will be populated below

# ─────────────────────────────────────────────────────────────────────────────
# Helper : normalise a raw candle list into a clean DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def _candles_to_df(candles) -> pd.DataFrame:
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
# Pass 1 : Historical closed sessions (last 5 calendar days → yesterday)
# ─────────────────────────────────────────────────────────────────────────────

_now_ist   = _ist_now()
today_str  = _now_ist.strftime("%Y-%m-%d")
from_str   = (_now_ist - datetime.timedelta(days=5)).strftime("%Y-%m-%d")

hist_df = pd.DataFrame()
try:
    resp     = hist_api.get_historical_candle_data1(
        INSTRUMENT_KEY, "minutes", "1", today_str, from_str
    )
    hist_df  = _candles_to_df(resp.data.candles)
    hist_df  = hist_df.iloc[::-1].reset_index(drop=True)   # oldest first
    if not hist_df.empty:
        print(
            f"[BUFFER] Historical : {len(hist_df)} candles  "
            f"({hist_df['datetime'].iloc[0]}  →  {hist_df['datetime'].iloc[-1]})"
        )
except Exception as exc:
    print(f"[BUFFER] Historical API error: {exc}")

# ─────────────────────────────────────────────────────────────────────────────
# Pass 2 : Today's intraday candles (9:15 AM → now)
# ─────────────────────────────────────────────────────────────────────────────

intra_df = pd.DataFrame()
try:
    resp     = intraday_api.get_intra_day_candle_data(INSTRUMENT_KEY, "1minute")
    intra_df = _candles_to_df(resp.data.candles)
    intra_df = intra_df.iloc[::-1].reset_index(drop=True)  # oldest first
    if not intra_df.empty:
        print(
            f"[BUFFER] Intraday   : {len(intra_df)} candles  "
            f"({intra_df['datetime'].iloc[0]}  →  {intra_df['datetime'].iloc[-1]})"
        )
    else:
        print("[BUFFER] Intraday   : no candles yet (pre-market or holiday)")
except Exception as exc:
    print(f"[BUFFER] Intraday API error: {exc}")

# ─────────────────────────────────────────────────────────────────────────────
# Merge : concat → sort by datetime → drop exact duplicates
# ─────────────────────────────────────────────────────────────────────────────

parts = [p for p in [hist_df, intra_df] if not p.empty]

if parts:
    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    print(
        f"[BUFFER] Combined   : {len(df)} candles  "
        f"({df['datetime'].iloc[0]}  →  {df['datetime'].iloc[-1]})"
    )
    print(df.tail())
else:
    print("[BUFFER] No data fetched — starting from scratch.")

# ─────────────────────────────────────────────────────────────────────────────
# Standalone sanity check
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nFull tail of combined buffer:")
    print(df.tail(15).to_string(index=False))
