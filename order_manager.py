"""
order_manager.py - Option trade manager with live option websocket monitoring.

When a signal is generated:
1. Resolve ATM option contract (CE for LONG, PE for SHORT).
2. Open virtual/live buy trade.
3. Start a dedicated websocket for that option instrument.
4. Track tick-by-tick P&L and close on target/SL.
5. Send entry and exit trade reports to Telegram.
"""

import asyncio
import datetime
import json
import os
import ssl
import threading
import time

import requests
import upstox_client
import websockets
from upstox_client.rest import ApiException

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, access_token

VIRTUAL_MODE = os.environ.get("VIRTUAL_MODE", "true").lower() != "false"

LOT_SIZE = 65
TARGET_PNL = 600.0
SL_PNL = -300.0
NIFTY_STEP = 50
OPTION_LOG_INTERVAL_SEC = 5.0
DAILY_SUMMARY_TIME = datetime.time(15, 31)  # 3:31 PM IST
SUMMARY_CHECK_INTERVAL_SEC = 30
IST_TZ = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

print(f"[ORDER_MGR] Mode = {'VIRTUAL' if VIRTUAL_MODE else 'LIVE'}")

_cfg = upstox_client.Configuration()
_cfg.access_token = access_token
_api_client = upstox_client.ApiClient(_cfg)

_options_api = upstox_client.OptionsApi(_api_client)
_market_quote_api = upstox_client.MarketQuoteApi(_api_client)
_order_api = upstox_client.OrderApiV3(_api_client)

_trades_lock = threading.Lock()
_active_trades: list[dict] = []

_summary_lock = threading.Lock()
_closed_trades_by_day: dict[datetime.date, list[dict]] = {}
_summary_sent_days: set[datetime.date] = set()
_summary_worker_started = False


def _ist_now() -> datetime.datetime:
    return datetime.datetime.now(IST_TZ)


def _to_ist_naive_datetime(value):
    if value is None:
        return None

    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()

    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(IST_TZ).replace(tzinfo=None)

    try:
        parsed = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(IST_TZ).replace(tzinfo=None)


def _format_ist_timestamp(value, fmt: str = "%d-%b-%Y %H:%M:%S IST") -> str:
    dt = _to_ist_naive_datetime(value)
    if dt is None:
        return str(value)
    return dt.strftime(fmt)


def _send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] Missing credentials; skipping message.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}

    try:
        resp = requests.post(url, data=payload, timeout=10)
        if resp.status_code != 200:
            print(f"[TELEGRAM] Failed ({resp.status_code}): {resp.text[:200]}")
    except Exception as exc:
        print(f"[TELEGRAM] Error: {exc}")


def _send_trade_entry_alert(trade: dict):
    target_price = trade["entry_price"] + (TARGET_PNL / LOT_SIZE)
    sl_price = trade["entry_price"] + (SL_PNL / LOT_SIZE)

    message = (
        "Signal Triggered - Option Trade Started\n"
        f"Mode: {'VIRTUAL' if VIRTUAL_MODE else 'LIVE'}\n"
        f"Signal: {trade['signal']}\n"
        f"Instrument: NIFTY {trade['strike']} {trade['option_type']}\n"
        f"Expiry: {trade['expiry_date']}\n"
        f"Instrument Key: {trade['instrument_key']}\n"
        f"Entry: Rs {trade['entry_price']:.2f}\n"
        f"Lot Size: {LOT_SIZE}\n"
        f"Target Price: Rs {target_price:.2f} (P&L +Rs {TARGET_PNL:.2f})\n"
        f"Stop Price: Rs {sl_price:.2f} (P&L Rs {SL_PNL:.2f})\n"
        f"Signal Time: {trade['signal_time']}"
    )
    _send_telegram(message)


def _send_trade_exit_alert(trade: dict, exit_reason: str, exit_price: float, pnl: float, exit_time):
    result = "PROFIT" if pnl >= 0 else "LOSS"
    duration = _format_trade_duration(trade["entry_ts"], exit_time)

    message = (
        "Trade Closed - Report\n"
        f"Mode: {'VIRTUAL' if VIRTUAL_MODE else 'LIVE'}\n"
        f"Signal: {trade['signal']}\n"
        f"Instrument: NIFTY {trade['strike']} {trade['option_type']}\n"
        f"Entry: Rs {trade['entry_price']:.2f}\n"
        f"Exit: Rs {exit_price:.2f}\n"
        f"Exit Reason: {exit_reason}\n"
        f"P&L: Rs {pnl:+.2f}\n"
        f"Result: {result}\n"
        f"Signal Time: {trade['signal_time']}\n"
        f"Entry Time: {_format_ist_timestamp(trade['entry_ts'])}\n"
        f"Exit Time: {_format_ist_timestamp(exit_time)}\n"
        f"Duration: {duration}"
    )
    _send_telegram(message)


def _format_trade_duration(start_ts: datetime.datetime, end_ts: datetime.datetime) -> str:
    delta = end_ts - start_ts
    total_sec = int(max(delta.total_seconds(), 0))
    minutes, seconds = divmod(total_sec, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _record_closed_trade(trade: dict, exit_reason: str, exit_price: float, pnl: float, exit_time: datetime.datetime):
    trade_day = trade["entry_ts"].date()
    row = {
        "signal": trade["signal"],
        "strike": trade["strike"],
        "option_type": trade["option_type"],
        "entry_price": float(trade["entry_price"]),
        "exit_price": float(exit_price),
        "exit_reason": exit_reason,
        "pnl": float(pnl),
        "entry_ts": trade["entry_ts"],
        "exit_ts": exit_time,
    }
    with _summary_lock:
        _closed_trades_by_day.setdefault(trade_day, []).append(row)


def _has_open_trades_for_day(trade_day: datetime.date) -> bool:
    with _trades_lock:
        for trade in _active_trades:
            if trade["entry_ts"].date() == trade_day:
                return True
    return False


def _try_send_daily_summary_for_day(trade_day: datetime.date, force: bool = False) -> bool:
    now = _ist_now().replace(tzinfo=None)
    summary_time = datetime.datetime.combine(trade_day, DAILY_SUMMARY_TIME)
    if not force and now < summary_time:
        return False

    if not force and _has_open_trades_for_day(trade_day):
        return False

    with _summary_lock:
        if trade_day in _summary_sent_days:
            return False
        trades = list(_closed_trades_by_day.get(trade_day, []))

    if not trades:
        return False

    total = len(trades)
    wins = sum(1 for item in trades if item["pnl"] >= 0)
    losses = total - wins
    net_pnl = sum(item["pnl"] for item in trades)
    gross_profit = sum(item["pnl"] for item in trades if item["pnl"] > 0)
    gross_loss = sum(item["pnl"] for item in trades if item["pnl"] < 0)
    best_trade = max(item["pnl"] for item in trades)
    worst_trade = min(item["pnl"] for item in trades)
    avg_pnl = net_pnl / total if total else 0.0
    win_rate = (wins * 100.0 / total) if total else 0.0

    mode = "VIRTUAL" if VIRTUAL_MODE else "LIVE"
    message = (
        "Daily Trade Summary\n"
        f"Date: {trade_day.isoformat()} (IST)\n"
        f"Mode: {mode}\n"
        f"Total Trades: {total}\n"
        f"Wins: {wins}\n"
        f"Losses: {losses}\n"
        f"Win Rate: {win_rate:.2f}%\n"
        f"Net P&L: Rs {net_pnl:+.2f}\n"
        f"Gross Profit: Rs {gross_profit:+.2f}\n"
        f"Gross Loss: Rs {gross_loss:+.2f}\n"
        f"Avg P&L/Trade: Rs {avg_pnl:+.2f}\n"
        f"Best Trade: Rs {best_trade:+.2f}\n"
        f"Worst Trade: Rs {worst_trade:+.2f}"
    )
    _send_telegram(message)

    with _summary_lock:
        _summary_sent_days.add(trade_day)
    print(f"[SUMMARY] Daily summary sent for {trade_day} ({total} trades).")
    return True


def _daily_summary_worker():
    while True:
        try:
            now = _ist_now().replace(tzinfo=None)
            today = now.date()
            _try_send_daily_summary_for_day(today)
        except Exception as exc:
            print(f"[SUMMARY] Worker error: {exc}")
        time.sleep(SUMMARY_CHECK_INTERVAL_SEC)


def _ensure_daily_summary_worker_started():
    global _summary_worker_started
    with _summary_lock:
        if _summary_worker_started:
            return
        _summary_worker_started = True

    worker = threading.Thread(
        target=_daily_summary_worker,
        daemon=True,
        name="daily-summary-worker",
    )
    worker.start()
    print("[SUMMARY] Daily summary worker started.")


def _get_ws_url() -> str:
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


def _subscribe_message(instrument_keys: list[str]) -> bytes:
    payload = {
        "guid": f"opt-{int(time.time())}",
        "method": "sub",
        "data": {"mode": "ltpc", "instrumentKeys": instrument_keys},
    }
    return json.dumps(payload).encode("utf-8")


def _decode_ltp(raw: bytes, instrument_key: str):
    try:
        from upstox_client.feeder.proto import MarketDataFeedV3_pb2

        feed = MarketDataFeedV3_pb2.FeedResponse()
        feed.ParseFromString(raw)
        tick = feed.feeds.get(instrument_key)
        if tick and tick.HasField("ltpc"):
            return float(tick.ltpc.ltp)
    except Exception as exc:
        print(f"[OPTION_WS] Decode error: {exc}")
    return None


def get_expiry_date() -> str:
    try:
        response = _options_api.get_option_contracts("NSE_INDEX|Nifty 50")
    except ApiException as exc:
        raise RuntimeError(f"get_option_contracts failed: {exc}") from exc

    if not response or not response.data:
        raise RuntimeError("No option contracts returned by Upstox.")

    today = _ist_now().date()
    expiries = set()

    for contract in response.data:
        raw_expiry = getattr(contract, "expiry", None) or getattr(contract, "expiry_date", None)
        if not raw_expiry:
            continue

        if isinstance(raw_expiry, datetime.datetime):
            expiry_date = raw_expiry.date()
        elif isinstance(raw_expiry, datetime.date):
            expiry_date = raw_expiry
        else:
            try:
                expiry_date = datetime.datetime.strptime(str(raw_expiry)[:10], "%Y-%m-%d").date()
            except ValueError:
                continue

        if expiry_date >= today:
            expiries.add(expiry_date)

    if not expiries:
        raise RuntimeError("No upcoming expiry date found.")

    nearest = min(expiries)
    return nearest.strftime("%Y-%m-%d")


def get_option_instrument_key(nifty_ltp: float, option_type: str) -> tuple[str, int, float, str]:
    expiry_date = get_expiry_date()
    atm_strike = round(nifty_ltp / NIFTY_STEP) * NIFTY_STEP

    try:
        response = _options_api.get_put_call_option_chain("NSE_INDEX|Nifty 50", expiry_date)
    except ApiException as exc:
        raise RuntimeError(f"Option chain API error: {exc}") from exc

    if not response or not response.data:
        raise RuntimeError(f"Option chain is empty for expiry {expiry_date}.")

    chain: dict[int, dict] = {}
    for row in response.data:
        strike = int(row.strike_price)
        data = chain.setdefault(strike, {})

        if row.call_options and row.call_options.market_data:
            data["CE_key"] = row.call_options.instrument_key
            data["CE_ltp"] = float(row.call_options.market_data.ltp or 0.0)

        if row.put_options and row.put_options.market_data:
            data["PE_key"] = row.put_options.instrument_key
            data["PE_ltp"] = float(row.put_options.market_data.ltp or 0.0)

    preferred_strikes = [atm_strike, atm_strike + NIFTY_STEP, atm_strike - NIFTY_STEP]
    for strike in preferred_strikes:
        key_name = f"{option_type}_key"
        ltp_name = f"{option_type}_ltp"
        if strike in chain and key_name in chain[strike]:
            return chain[strike][key_name], strike, chain[strike].get(ltp_name, 0.0), expiry_date

    sorted_strikes = sorted(chain.keys(), key=lambda strike: abs(strike - atm_strike))
    key_name = f"{option_type}_key"
    ltp_name = f"{option_type}_ltp"
    for strike in sorted_strikes:
        if key_name in chain[strike]:
            return chain[strike][key_name], strike, chain[strike].get(ltp_name, 0.0), expiry_date

    raise RuntimeError(f"No {option_type} contract found near ATM {atm_strike}.")


def get_option_ltp(instrument_key: str) -> float:
    try:
        response = _market_quote_api.ltp(instrument_key)
        if response and response.data:
            row = next(iter(response.data.values()))
            return float(row.last_price)
    except Exception as exc:
        print(f"[ORDER] LTP fetch failed: {exc}")
    return 0.0


def _place_real_order(instrument_key: str, transaction_type: str) -> str:
    body = upstox_client.PlaceOrderV3Request(
        quantity=LOT_SIZE,
        product="I",
        validity="DAY",
        price=0.0,
        instrument_token=instrument_key,
        order_type="MARKET",
        transaction_type=transaction_type,
        disclosed_quantity=0,
        trigger_price=0.0,
        is_amo=False,
        slice=False,
    )

    try:
        response = _order_api.place_order(body=body)
        return response.data.order_id
    except ApiException as exc:
        raise RuntimeError(f"place_order failed: {exc}") from exc


async def _monitor_trade_via_option_ws(trade: dict) -> tuple[str, float, float, datetime.datetime]:
    instrument_key = trade["instrument_key"]
    entry_price = float(trade["entry_price"])

    target_price = entry_price + (TARGET_PNL / LOT_SIZE)
    sl_price = entry_price + (SL_PNL / LOT_SIZE)

    print(
        f"[OPTION_WS] Monitor started for {trade['option_type']}{trade['strike']} "
        f"Entry={entry_price:.2f} Target={target_price:.2f} SL={sl_price:.2f}"
    )

    last_log_time = 0.0

    while True:
        ws_url = _get_ws_url()

        try:
            async with websockets.connect(ws_url, ssl=ssl.create_default_context(), ping_interval=20) as ws:
                await ws.send(_subscribe_message([instrument_key]))
                print(f"[OPTION_WS] Subscribed to {instrument_key}")

                async for message in ws:
                    if not isinstance(message, bytes):
                        continue

                    ltp = _decode_ltp(message, instrument_key)
                    if ltp is None:
                        continue

                    exit_time = _ist_now().replace(tzinfo=None)
                    pnl = (ltp - entry_price) * LOT_SIZE

                    now_epoch = time.time()
                    if now_epoch - last_log_time >= OPTION_LOG_INTERVAL_SEC:
                        print(
                            f"[OPTION_WS] {trade['option_type']}{trade['strike']} "
                            f"LTP={ltp:.2f} P&L=Rs {pnl:+.2f}"
                        )
                        last_log_time = now_epoch

                    if ltp >= target_price:
                        return "TARGET", ltp, pnl, exit_time

                    if ltp <= sl_price:
                        return "SL", ltp, pnl, exit_time

        except Exception as exc:
            print(f"[OPTION_WS] Error for {instrument_key}: {exc}. Reconnecting in 2s...")
            await asyncio.sleep(2)


def _monitor_trade_worker(trade: dict):
    trade_closed = False
    trade_day = trade["entry_ts"].date()

    try:
        exit_reason, exit_price, pnl, exit_time = asyncio.run(_monitor_trade_via_option_ws(trade))

        if VIRTUAL_MODE:
            print(
                f"[ORDER] VIRTUAL exit {trade['option_type']}{trade['strike']} "
                f"Reason={exit_reason} Exit={exit_price:.2f} P&L=Rs {pnl:+.2f}"
            )
        else:
            sell_order_id = _place_real_order(trade["instrument_key"], transaction_type="SELL")
            print(f"[ORDER] LIVE square-off order placed: {sell_order_id}")

        _send_trade_exit_alert(
            trade=trade,
            exit_reason=exit_reason,
            exit_price=exit_price,
            pnl=pnl,
            exit_time=exit_time,
        )
        _record_closed_trade(
            trade=trade,
            exit_reason=exit_reason,
            exit_price=exit_price,
            pnl=pnl,
            exit_time=exit_time,
        )
        trade_closed = True

    except Exception as exc:
        print(f"[ORDER] Monitor crashed for {trade['instrument_key']}: {exc}")
        _send_telegram(
            "Trade monitor error\n"
            f"Instrument: {trade['instrument_key']}\n"
            f"Error: {exc}"
        )

    finally:
        with _trades_lock:
            try:
                _active_trades.remove(trade)
            except ValueError:
                pass

        if trade_closed:
            print(f"[ORDER] Trade completed and cleared: {trade['instrument_key']}")
            _try_send_daily_summary_for_day(trade_day)
        else:
            print(f"[ORDER] Trade removed after monitor failure: {trade['instrument_key']}")


def place_option_order(signal: str, nifty_ltp: float, signal_time: str):
    """
    Called by websocket.py on each fresh signal.

    This function resolves the option contract, creates the trade,
    and starts a dedicated option websocket monitor thread for that trade.
    """
    _ensure_daily_summary_worker_started()

    signal = signal.upper().strip()
    if signal not in {"LONG", "SHORT"}:
        print(f"[ORDER] Unsupported signal: {signal}")
        return

    option_type = "CE" if signal == "LONG" else "PE"
    signal_time_fmt = _format_ist_timestamp(signal_time)

    with _trades_lock:
        active_count = len(_active_trades)

    print("=" * 70)
    print(
        f"[ORDER] New signal={signal} option={option_type} "
        f"time={signal_time_fmt} active_trades={active_count}"
    )
    print("=" * 70)

    try:
        instrument_key, strike, chain_ltp, expiry_date = get_option_instrument_key(nifty_ltp, option_type)

        live_ltp = get_option_ltp(instrument_key)
        entry_price = live_ltp if live_ltp > 0 else chain_ltp
        if entry_price <= 0:
            raise RuntimeError("Entry price could not be determined from chain/live quote.")

        if VIRTUAL_MODE:
            order_id = f"VIRTUAL-{_ist_now().strftime('%H%M%S%f')[:13]}"
            print(
                f"[ORDER] VIRTUAL BUY {option_type} {strike} Qty={LOT_SIZE} "
                f"Entry=Rs {entry_price:.2f} Ref={order_id}"
            )
        else:
            order_id = _place_real_order(instrument_key, transaction_type="BUY")
            print(f"[ORDER] LIVE BUY order placed: {order_id}")

        trade = {
            "instrument_key": instrument_key,
            "strike": strike,
            "option_type": option_type,
            "expiry_date": expiry_date,
            "entry_price": float(entry_price),
            "order_id": order_id,
            "signal": signal,
            "signal_time": signal_time_fmt,
            "entry_ts": _ist_now().replace(tzinfo=None),
        }

        with _trades_lock:
            _active_trades.append(trade)

        _send_trade_entry_alert(trade)

        worker = threading.Thread(
            target=_monitor_trade_worker,
            args=(trade,),
            daemon=True,
            name=f"option-ws-{option_type}{strike}-{order_id[-6:]}",
        )
        worker.start()
        print(f"[ORDER] Started option websocket monitor thread: {worker.name}")

    except Exception as exc:
        print(f"[ORDER] Failed to start trade: {exc}")
        _send_telegram(
            "Order setup failed\n"
            f"Signal: {signal}\n"
            f"Nifty LTP: {nifty_ltp:.2f}\n"
            f"Error: {exc}"
        )


_ensure_daily_summary_worker_started()
