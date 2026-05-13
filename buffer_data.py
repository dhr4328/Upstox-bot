"""
buffer_data.py - Fetch historical + intraday OHLC candles for Nifty 50.

Exposes:
  df - DataFrame[datetime, open, high, low, close, volume] in ascending order.
"""

import datetime

import pandas as pd
import upstox_client

from config import access_token, data_token

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
CANDLE_MINUTES = 5
IST_TZ = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def _ist_now() -> datetime.datetime:
    return datetime.datetime.now(IST_TZ)


def _interval_text(candle_minutes: int) -> str:
    return f"{candle_minutes}minute"


def _candles_to_df(candles) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame()

    frame = pd.DataFrame(
        candles,
        columns=["datetime", "open", "high", "low", "close", "volume", "OI"],
    )
    frame = frame.drop(columns=["OI"])
    frame["datetime"] = (
        pd.to_datetime(frame["datetime"], utc=True)
        .dt.tz_convert("Asia/Kolkata")
        .dt.tz_localize(None)
    )
    return frame


configuration = upstox_client.Configuration()
configuration.access_token = data_token if data_token else access_token
_client = upstox_client.ApiClient(configuration)
history_api = upstox_client.HistoryV3Api(_client)

df = pd.DataFrame()

_now_ist = _ist_now()
today_str = _now_ist.strftime("%Y-%m-%d")
from_str = (_now_ist - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

hist_df = pd.DataFrame()
try:
    hist_resp = history_api.get_historical_candle_data1(
        INSTRUMENT_KEY,
        "minutes",
        str(CANDLE_MINUTES),
        today_str,
        from_str,
    )
    hist_df = _candles_to_df(hist_resp.data.candles if (hist_resp and hist_resp.data) else [])
    hist_df = hist_df.sort_values("datetime").reset_index(drop=True)
    if not hist_df.empty:
        print(
            f"[BUFFER] Historical: {len(hist_df)} candles "
            f"({hist_df['datetime'].iloc[0]} -> {hist_df['datetime'].iloc[-1]})."
        )
except Exception as exc:
    print(f"[BUFFER] Historical API error: {exc}")

intra_df = pd.DataFrame()
try:
    intra_resp = history_api.get_intra_day_candle_data(
        INSTRUMENT_KEY,
        _interval_text(CANDLE_MINUTES),
    )
    intra_df = _candles_to_df(intra_resp.data.candles if (intra_resp and intra_resp.data) else [])
    intra_df = intra_df.sort_values("datetime").reset_index(drop=True)
    if not intra_df.empty:
        print(
            f"[BUFFER] Intraday: {len(intra_df)} candles "
            f"({intra_df['datetime'].iloc[0]} -> {intra_df['datetime'].iloc[-1]})."
        )
    else:
        print("[BUFFER] Intraday: no candles available yet.")
except Exception as exc:
    print(f"[BUFFER] Intraday API error: {exc}")

parts = [part for part in (hist_df, intra_df) if not part.empty]
if parts:
    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    print(
        f"[BUFFER] Combined: {len(df)} candles "
        f"({df['datetime'].iloc[0]} -> {df['datetime'].iloc[-1]})."
    )
else:
    print("[BUFFER] No candles fetched; buffer is empty.")


if __name__ == "__main__":
    print(df.tail(15).to_string(index=False) if not df.empty else "No data")
