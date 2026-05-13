"""
intraday_data.py - Fetch today's missed intraday candles when bot starts late.

Returns list[dict] in ascending time order with keys:
  time, open, high, low, close, volume, source
"""

import datetime

import pandas as pd
import upstox_client

from config import access_token, data_token

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
MARKET_OPEN = datetime.time(9, 15)
DEFAULT_CANDLE_MINUTES = 5
IST_TZ = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def _ist_now_naive() -> datetime.datetime:
    return datetime.datetime.now(IST_TZ).replace(tzinfo=None)


def _bucket_start(ts: datetime.datetime, minutes: int) -> datetime.datetime:
    bucket_minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=bucket_minute, second=0, microsecond=0)


def _interval_text(candle_minutes: int) -> str:
    return f"{candle_minutes}minute"


_config = upstox_client.Configuration()
_config.access_token = data_token if data_token else access_token
_client = upstox_client.ApiClient(_config)
_intraday_api = upstox_client.HistoryV3Api(_client)


def _candles_to_df(candles) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame()

    frame = pd.DataFrame(
        candles,
        columns=["datetime", "open", "high", "low", "close", "volume", "OI"],
    )
    frame = frame.drop(columns=["OI"])  # keep only OHLCV
    frame["datetime"] = (
        pd.to_datetime(frame["datetime"], utc=True)
        .dt.tz_convert("Asia/Kolkata")
        .dt.tz_localize(None)
    )
    return frame


def get_intraday_candles(candle_minutes: int = DEFAULT_CANDLE_MINUTES) -> list:
    """
    Return today's intraday candles in `candle_minutes` timeframe.

    Returns [] when started at/before market open, on holidays, or on API failure.
    """
    now = _ist_now_naive()
    if now.time() <= MARKET_OPEN:
        print(
            f"[INTRADAY] Bot started at {now.strftime('%H:%M')} "
            f"(<= {MARKET_OPEN.strftime('%H:%M')}) - skipping prefetch."
        )
        return []

    interval = _interval_text(candle_minutes)
    print(
        f"[INTRADAY] Fetching {candle_minutes}-minute candles "
        f"for {INSTRUMENT_KEY} ({interval})."
    )

    try:
        resp = _intraday_api.get_intra_day_candle_data(INSTRUMENT_KEY, interval)
        raw_df = _candles_to_df(resp.data.candles if (resp and resp.data) else [])
    except Exception as exc:
        print(f"[INTRADAY] API error: {exc}")
        return []

    if raw_df.empty:
        print("[INTRADAY] API returned no candles (holiday/pre-market).")
        return []

    raw_df = raw_df.sort_values("datetime").reset_index(drop=True)

    open_bucket = _bucket_start(now, candle_minutes)
    if raw_df["datetime"].iloc[-1] >= open_bucket:
        raw_df = raw_df.iloc[:-1].reset_index(drop=True)

    if raw_df.empty:
        print("[INTRADAY] No closed intraday candles available after bucket cleanup.")
        return []

    candles = []
    for _, row in raw_df.iterrows():
        candles.append(
            {
                "time": row["datetime"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "source": "intraday",
            }
        )

    print(
        f"[INTRADAY] Loaded {len(candles)} closed {candle_minutes}-minute candles "
        f"({candles[0]['time'].strftime('%H:%M')} -> {candles[-1]['time'].strftime('%H:%M')})."
    )
    return candles


if __name__ == "__main__":
    data = get_intraday_candles()
    print(f"Candles fetched: {len(data)}")
    if data:
        for candle in data[-10:]:
            print(candle)
