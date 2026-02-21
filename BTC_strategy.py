#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BTC_strategy.py

Virtual portfolio tracker (Daily, TradingView websocket):
- Portfolio 1: 100% USDC (constant)
- Portfolio 2: 100% BTC (buy-and-hold)
- Portfolio 3: 100% USDC, DCA into BTC (<=5% of USDC) each time BTC drops 5% from last buy.

Realism mode:
- DCA decisions are made on closed daily candles only
- DCA execution history is persisted in a JSON ledger
- Fees and slippage are applied on each buy

Outputs: console + Telegram.
"""

import json
import os
import random
import string
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from websocket import WebSocketConnectionClosedException, create_connection

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


TV_SYMBOL = "INDEX:BTCUSD"
TIMEFRAME = "D"
N_CANDLES = 400
START_DATE = "2026-02-06"
DROP_PCT = 5.0  # Buy trigger drop (%)
MAX_DCA_PCT = 5.0  # Max % of USDC to spend per buy
START_USD = 15000.0
FEE_PCT = 0.10  # Execution fee (%)
SLIPPAGE_PCT = 0.05  # Buy slippage (%) on candle close
STATE_PATH = os.path.join(os.path.dirname(__file__), "btc_dca_state.json")


def _gen_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def _create_msg(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"


def _parse_frames(raw):
    if raw in ("~h", "h"):
        return [raw]
    frames = []
    i = 0
    while raw.startswith("~m~", i):
        i += 3
        j = raw.find("~m~", i)
        if j == -1:
            break
        size = int(raw[i:j])
        i = j + 3
        frames.append(raw[i : i + size])
        i += size
    return frames if frames else [raw]


def fetch_tv_ohlc(symbol, interval, n_candles, sleep_s=0.05, timeout_s=20, retries=2):
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(
                "wss://prodata.tradingview.com/socket.io/websocket",
                header={"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"},
                timeout=timeout_s,
            )
            session_id = _gen_session_id()
            ws.send(_create_msg("chart_create_session", [session_id, ""]))
            ws.send(
                _create_msg(
                    "resolve_symbol",
                    [session_id, "sds_sym_1", f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}'],
                )
            )
            ws.send(_create_msg("create_series", [session_id, "sds_1", "s1", "sds_sym_1", interval, n_candles, ""]))

            points = []
            start_t = time.time()
            while time.time() - start_t < 12:
                try:
                    raw = ws.recv()
                except WebSocketConnectionClosedException:
                    break
                for frame in _parse_frames(raw):
                    if frame in ("~h", "h"):
                        ws.send("~h")
                        continue
                    if '"m":"timescale_update"' in frame:
                        payload = json.loads(frame)
                        series = payload.get("p", [None, {}])[1]
                        if isinstance(series, dict) and "sds_1" in series:
                            points = series["sds_1"].get("s", []) or points
                    if "series_completed" in frame:
                        break
                if "series_completed" in raw:
                    break

            if not points:
                continue

            rows = []
            for item in points:
                v = item.get("v", [])
                if len(v) < 5:
                    continue
                ts, o, h, l, c = v[:5]
                rows.append({"date": ts, "open": o, "high": h, "low": l, "close": c})
            if not rows:
                continue

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"], unit="s", utc=True)
            df.set_index("date", inplace=True)
            return df.sort_index()
        except Exception:
            if attempt >= retries:
                return None
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
            if sleep_s:
                time.sleep(sleep_s)
    return None


def _state_config():
    return {
        "symbol": TV_SYMBOL,
        "timeframe": TIMEFRAME,
        "start_date": START_DATE,
        "drop_pct": DROP_PCT,
        "max_dca_pct": MAX_DCA_PCT,
        "start_usd": START_USD,
        "fee_pct": FEE_PCT,
        "slippage_pct": SLIPPAGE_PCT,
    }


def _new_state():
    return {
        "version": 1,
        "config": _state_config(),
        "processed_until": None,
        "p3_usdc": START_USD,
        "p3_btc": 0.0,
        "buys": [],
    }


def _load_state():
    if not os.path.exists(STATE_PATH):
        return _new_state()
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            return _new_state()
        if state.get("config") != _state_config():
            return _new_state()
        if not isinstance(state.get("buys"), list):
            return _new_state()
        return state
    except Exception:
        return _new_state()


def _save_state(state):
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _parse_ts_utc(ts_str):
    if not ts_str:
        return None
    try:
        return pd.to_datetime(ts_str, utc=True)
    except Exception:
        return None


def _iso_ts(ts):
    return ts.to_pydatetime().astimezone(timezone.utc).isoformat()


def main():
    if load_dotenv:
        load_dotenv()

    df = fetch_tv_ohlc(TV_SYMBOL, TIMEFRAME, N_CANDLES)
    if df is None or df.empty:
        print("BTC strategy: no data")
        return

    start_dt = pd.to_datetime(START_DATE, utc=True)
    df_start = df.loc[df.index >= start_dt]
    if df_start.empty:
        print(f"BTC strategy: no data after START_DATE={START_DATE}")
        return

    close_now = float(df_start["close"].iloc[-1])
    date_now = df_start.index[-1].to_pydatetime().astimezone(timezone.utc)
    date_txt = date_now.strftime("%Y-%m-%d")

    # Closed-candle subset for DCA decisions/execution.
    df_closed = df_start.iloc[:-1]
    if df_closed.empty:
        print("BTC strategy: waiting for first closed candle after START_DATE")
        return

    # Portfolio 1: 100% USDC
    p1_value = START_USD

    # Portfolio 2: 100% BTC (buy-and-hold from first closed candle after START_DATE)
    first_close = float(df_closed["close"].iloc[0])
    p2_btc = START_USD / first_close if first_close > 0 else 0.0
    p2_value = p2_btc * close_now

    # Portfolio 3: persistent DCA ledger
    state = _load_state()
    p3_usdc = float(state.get("p3_usdc", START_USD))
    p3_btc = float(state.get("p3_btc", 0.0))
    dca_buys = state.get("buys", [])

    last_buy_price = None
    last_buy_date = None
    if dca_buys:
        last_buy = dca_buys[-1]
        last_buy_price = float(last_buy.get("exec_price", last_buy.get("price", 0.0)))
        last_buy_date = _parse_ts_utc(last_buy.get("date"))

    processed_until = _parse_ts_utc(state.get("processed_until"))
    if processed_until is None:
        to_process = df_closed
    else:
        to_process = df_closed.loc[df_closed.index > processed_until]

    buy_usd = 0.0
    buy_btc = 0.0
    fee_usd = 0.0
    triggered = False
    last_closed_idx = df_closed.index[-1]

    for ts, row in to_process.iterrows():
        signal_price = float(row["close"])

        should_buy = False
        if last_buy_price is None:
            should_buy = True
        else:
            drop_pct_signal = (signal_price - float(last_buy_price)) / float(last_buy_price) * 100.0
            should_buy = drop_pct_signal <= -DROP_PCT

        if not should_buy:
            continue

        buy_usd = p3_usdc * (MAX_DCA_PCT / 100.0)
        if buy_usd <= 0:
            continue

        exec_price = signal_price * (1.0 + SLIPPAGE_PCT / 100.0)
        fee_usd = buy_usd * (FEE_PCT / 100.0)
        net_usd = max(0.0, buy_usd - fee_usd)
        buy_btc = (net_usd / exec_price) if exec_price > 0 else 0.0

        p3_usdc -= buy_usd
        p3_btc += buy_btc
        last_buy_price = exec_price
        last_buy_date = ts

        dca_buys.append(
            {
                "date": _iso_ts(ts),
                "signal_price": signal_price,
                "exec_price": exec_price,
                "usd_spent": buy_usd,
                "fee_usd": fee_usd,
                "btc": buy_btc,
            }
        )
        if ts == last_closed_idx:
            triggered = True

    state["processed_until"] = _iso_ts(last_closed_idx)
    state["p3_usdc"] = p3_usdc
    state["p3_btc"] = p3_btc
    state["buys"] = dca_buys
    _save_state(state)

    buy_count = len(dca_buys)
    drop_pct = None
    if last_buy_price is not None:
        drop_pct = (close_now - float(last_buy_price)) / float(last_buy_price) * 100.0

    p3_btc_value = p3_btc * close_now
    p3_value = p3_usdc + p3_btc_value
    p3_invested_btc_usd = START_USD - p3_usdc
    p3_usdc_share_pct = (p3_usdc / p3_value * 100.0) if p3_value > 0 else 0.0
    p3_invested_pnl_usd = p3_btc_value - p3_invested_btc_usd
    p3_invested_pnl_pct = (p3_invested_pnl_usd / p3_invested_btc_usd * 100.0) if p3_invested_btc_usd > 0 else 0.0

    lines = [
        f"BTC STRATEGY ({date_txt})",
        f"Start date: {START_DATE}",
        f"Start price: {first_close:,.2f} USD",
        f"Current price: {close_now:,.2f} USD",
        f"P1 USDC: {p1_value:,.2f}",
        f"P2 BTC: {p2_value:,.2f}",
        f"P3 DCA: {p3_value:,.2f}",
        f"P3 USDC: {p3_usdc:,.2f} | BTC: {p3_btc:.6f}",
        f"P3 investi BTC: {p3_invested_btc_usd:,.2f}",
        f"P3 valeur BTC: {p3_btc_value:,.2f} ({p3_invested_pnl_pct:+.2f}%)",
        f"P3 part USDC: {p3_usdc_share_pct:.2f}% ({p3_usdc:,.2f})",
        f"Last buy: {last_buy_price:,.2f} ({last_buy_date.strftime('%Y-%m-%d') if last_buy_date is not None else 'n/a'}) | Buys: {buy_count}",
    ]
    if drop_pct is not None:
        lines.append(f"Drop vs last buy: {drop_pct:+.2f}%")
    if triggered:
        lines.append(f"DCA BUY: {buy_usd:,.2f} USD -> {buy_btc:.6f} BTC (fee {fee_usd:,.2f})")
    if dca_buys:
        lines.append("DCA Buys:")
        for i, b in enumerate(dca_buys, 1):
            b_date = _parse_ts_utc(b.get("date")).strftime("%Y-%m-%d")
            exec_price = float(b.get("exec_price", b.get("price", 0.0)))
            lines.append(f"  {i}. {b_date} @ {exec_price:,.2f} USD")

    print("\n".join(lines))

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            green = "🟢"
            red = "🔴"
            p1_dot = green if p1_value >= START_USD else red
            p2_dot = green if p2_value >= START_USD else red
            p3_dot = green if p3_value >= START_USD else red
            paris_now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
            p2_pct = (p2_value - START_USD) / START_USD * 100.0
            p3_pct = (p3_value - START_USD) / START_USD * 100.0
            rows = [
                ("P1 USDC", p1_value, p1_dot, None),
                ("P2 BTC", p2_value, p2_dot, p2_pct),
                ("P3 DCA", p3_value, p3_dot, p3_pct),
            ]
            rows.sort(key=lambda r: r[1], reverse=True)
            tg_lines = [
                f"<b>BTC STRATEGY ({date_txt})</b>",
                "",
                f"Start date: {START_DATE}",
                f"Start price: {first_close:,.2f} USD",
                f"Current price: {close_now:,.2f} USD",
                "",
            ]
            for name, val, dot, pct in rows:
                if pct is None:
                    tg_lines.append(f"{dot} {name}: {val:,.2f}")
                else:
                    tg_lines.append(f"{dot} {name}: {val:,.2f} ({pct:+.2f}%)")
            tg_lines += [
                "",
                f"P3 investi BTC: {p3_invested_btc_usd:,.2f}",
                f"P3 valeur BTC: {p3_btc_value:,.2f} ({p3_invested_pnl_pct:+.2f}%)",
                f"P3 part USDC: {p3_usdc_share_pct:.2f}% ({p3_usdc:,.2f})",
            ]
            if dca_buys:
                tg_lines += ["", "DCA Buys:"]
                for i, b in enumerate(dca_buys, 1):
                    b_date = _parse_ts_utc(b.get("date")).strftime("%Y-%m-%d")
                    exec_price = float(b.get("exec_price", b.get("price", 0.0)))
                    tg_lines.append(f"{i}. {b_date} @ {exec_price:,.2f} USD")
            tg_lines += [
                "",
                f"⏰ {paris_now} Paris",
            ]
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(
                url,
                json={"chat_id": chat_id, "text": "\n".join(tg_lines), "parse_mode": "HTML"},
                timeout=10,
            )
            print("Telegram: sent")
        except Exception as exc:
            print(f"Telegram: not sent ({exc})")
    else:
        print("Telegram: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")


if __name__ == "__main__":
    main()
