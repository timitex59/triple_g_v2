#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
px_renko_screener_v4.py

Index-driven FX pair screener.

Approach:
  1. Scan 8 currency indices for Px/Renko D1 bias (D1 + W1 + CHG%)
  2. Derive candidate pairs from BULL/BEAR index combinations:
     - BULL base + BEAR quote → pair BULL
     - BEAR base + BULL quote → pair BEAR
  3. Only fetch daily CHG% for candidate pairs to confirm direction
  4. Fallback: if only BEAR (no BULL), use MIXED indices with CHG%>0 as proxy-BULL
             if only BULL (no BEAR), use MIXED indices with CHG%<0 as proxy-BEAR

Much faster than V3: only 8 Renko scans instead of 36.
"""

import argparse
import json
import os
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime

import requests as http_requests
from dotenv import load_dotenv
from websocket import WebSocketConnectionClosedException, create_connection

from ichimoku_v4 import fetch_tv_ohlc

load_dotenv()

# ── constants ──────────────────────────────────────────────────────────
WS_URL = "wss://prodata.tradingview.com/socket.io/websocket"
WS_HEADERS = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MIN_CHG_PCT = 0.15

PAIR_LIST = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
]
PAIR_SET = set(PAIR_LIST)

INDICES = {
    "DXY": "TVC:DXY",
    "EXY": "TVC:EXY",
    "BXY": "TVC:BXY",
    "JXY": "TVC:JXY",
    "SXY": "TVC:SXY",
    "CXY": "TVC:CXY",
    "AXY": "TVC:AXY",
    "ZXY": "TVC:ZXY",
}

INDEX_TO_CCY = {
    "DXY": "USD", "EXY": "EUR", "BXY": "GBP", "JXY": "JPY",
    "SXY": "CHF", "CXY": "CAD", "AXY": "AUD", "ZXY": "NZD",
}
CCY_TO_INDEX = {v: k for k, v in INDEX_TO_CCY.items()}


# ── dataclasses ────────────────────────────────────────────────────────
@dataclass
class IndexResult:
    name: str           # e.g. "AXY"
    symbol: str         # e.g. "TVC:AXY"
    ccy: str            # e.g. "AUD"
    px_vs_d1: int       # 1=BULL, -1=BEAR, 0=Inside
    px_vs_w1: int
    daily_chg_pct: float
    final_bias: int     # 1=BULL, -1=BEAR, 0=MIXED
    role: str = ""      # "BULL", "BEAR", "proxy-BULL", "proxy-BEAR", ""


@dataclass
class PairCandidate:
    pair: str           # e.g. "GBPAUD"
    expected_bias: int  # 1=BULL, -1=BEAR
    bull_ccy: str       # strong currency
    bear_ccy: str       # weak currency
    bull_idx: str       # e.g. "BXY"
    bear_idx: str       # e.g. "AXY"
    is_proxy: bool      # True if one side is a proxy (fallback)
    daily_chg_pct: float = 0.0
    chg_confirmed: bool = False


# ── TradingView websocket helpers ─────────────────────────────────────
def _gen_sid():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def _msg(func, args):
    payload = json.dumps({"m": func, "p": args})
    return f"~m~{len(payload)}~m~{payload}"


def _frames(raw):
    if raw in ("~h", "h"):
        return [raw]
    out, i = [], 0
    while raw.startswith("~m~", i):
        i += 3
        j = raw.find("~m~", i)
        if j == -1:
            break
        size = int(raw[i:j])
        i = j + 3
        out.append(raw[i : i + size])
        i += size
    return out if out else [raw]


def fetch_tv_renko_ohlc(symbol: str, interval: str, atr_length: int = 14,
                        n_bricks: int = 5, timeout_s: int = 15, retries: int = 2):
    """Return last Renko brick as dict {open, high, low, close}, or None."""
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(WS_URL, header=WS_HEADERS, timeout=timeout_s)
            sid = _gen_sid()
            ws.send(_msg("chart_create_session", [sid, ""]))
            sym_payload = json.dumps({
                "symbol": {"symbol": symbol, "adjustment": "splits", "session": "regular"},
                "type": "BarSetRenko@tv-prostudies-40!",
                "inputs": {
                    "source": "close", "sources": "Close",
                    "boxSize": atr_length, "style": "ATR",
                    "atrLength": atr_length, "wicks": True,
                },
            })
            ws.send(_msg("resolve_symbol", [sid, "sds_sym_1", f"={sym_payload}"]))
            ws.send(_msg("create_series", [sid, "sds_1", "s1", "sds_sym_1", interval, n_bricks, ""]))

            points = []
            t0 = time.time()
            while time.time() - t0 < 12:
                try:
                    raw = ws.recv()
                except WebSocketConnectionClosedException:
                    break
                for frame in _frames(raw):
                    if frame in ("~h", "h"):
                        ws.send("~h")
                        continue
                    if '"m":"symbol_error"' in frame:
                        return None
                    if '"m":"timescale_update"' in frame:
                        payload = json.loads(frame)
                        series = payload.get("p", [None, {}])[1]
                        if isinstance(series, dict) and "sds_1" in series:
                            points = series["sds_1"].get("s", []) or points
                    if "series_completed" in frame:
                        break
                if "series_completed" in raw:
                    break

            if points:
                for item in reversed(points):
                    v = item.get("v", [])
                    if len(v) >= 5:
                        return {"open": v[1], "high": v[2], "low": v[3], "close": v[4]}
        except Exception:
            if attempt >= retries:
                return None
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
    return None


# ── Phase 1: scan indices ─────────────────────────────────────────────
def compute_px_vs_renko(price: float, renko_open: float, renko_close: float) -> int:
    renko_high = max(renko_open, renko_close)
    renko_low = min(renko_open, renko_close)
    if price > renko_high:
        return 1
    elif price < renko_low:
        return -1
    return 0


def _scan_index(name: str, symbol: str, atr_length: int, min_chg: float) -> IndexResult | None:
    """Scan one index: Renko D1 + W1, current price, daily CHG%."""
    df = fetch_tv_ohlc(symbol, "60", 5)
    if df is None or df.empty:
        return None
    current_price = float(df["close"].iloc[-1])

    df_d = fetch_tv_ohlc(symbol, "D", 5)
    if df_d is None or len(df_d) < 2:
        return None
    daily_close = float(df_d["close"].iloc[-1])
    daily_prev = float(df_d["close"].iloc[-2])
    chg_pct = ((daily_close - daily_prev) / daily_prev * 100) if daily_prev != 0 else 0.0

    brick_d1 = fetch_tv_renko_ohlc(symbol, "D", atr_length=atr_length)
    if not brick_d1:
        return None
    px_d1 = compute_px_vs_renko(current_price, brick_d1["open"], brick_d1["close"])

    brick_w1 = fetch_tv_renko_ohlc(symbol, "W", atr_length=atr_length)
    if not brick_w1:
        return None
    px_w1 = compute_px_vs_renko(current_price, brick_w1["open"], brick_w1["close"])

    chg_valid = abs(chg_pct) > min_chg
    chg_same_dir = (px_d1 == 1 and chg_pct > 0) or (px_d1 == -1 and chg_pct < 0)
    weekly_opposed = (px_d1 == 1 and px_w1 == -1) or (px_d1 == -1 and px_w1 == 1)
    final_bias = px_d1 if (chg_valid and chg_same_dir and not weekly_opposed) else 0

    ccy = INDEX_TO_CCY[name]
    return IndexResult(
        name=name, symbol=symbol, ccy=ccy,
        px_vs_d1=px_d1, px_vs_w1=px_w1,
        daily_chg_pct=chg_pct, final_bias=final_bias,
    )


def scan_indices(atr_length: int, min_chg: float, workers: int = 5) -> list[IndexResult]:
    total = len(INDICES)
    results: list[IndexResult] = []
    done = 0

    print(f"\n  Phase 1: Scanning {total} indices ...")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_scan_index, name, sym, atr_length, min_chg): name
            for name, sym in INDICES.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            done += 1
            try:
                r = future.result()
                if r:
                    results.append(r)
                    tag = "BULL" if r.final_bias == 1 else "BEAR" if r.final_bias == -1 else "MIX"
                    print(f"\r  [{done}/{total}] {name:<4} {tag}", end="", flush=True)
                else:
                    print(f"\r  [{done}/{total}] {name:<4} FAIL", end="", flush=True)
            except Exception as e:
                print(f"\r  [{done}/{total}] {name:<4} ERR", end="", flush=True)

    print("\r" + " " * 50 + "\r", end="")
    return results


# ── Phase 2: derive candidate pairs ───────────────────────────────────
def derive_candidates(indices: list[IndexResult], min_chg: float) -> list[PairCandidate]:
    """Build candidate pairs from BULL/BEAR index combinations."""
    bulls = [r for r in indices if r.final_bias == 1]
    bears = [r for r in indices if r.final_bias == -1]
    mixed = [r for r in indices if r.final_bias == 0]

    bull_ccys: list[tuple[str, str, bool]] = []  # (ccy, index_name, is_proxy)
    bear_ccys: list[tuple[str, str, bool]] = []

    if bulls and bears:
        # Case A: both BULL and BEAR indices exist
        bull_ccys = [(r.ccy, r.name, False) for r in bulls]
        bear_ccys = [(r.ccy, r.name, False) for r in bears]
    elif bears and not bulls:
        # Case B: only BEAR — use MIXED with positive CHG% as proxy-BULL
        proxy_bulls = [r for r in mixed if r.daily_chg_pct > 0]
        if not proxy_bulls:
            return []
        bull_ccys = [(r.ccy, r.name, True) for r in proxy_bulls]
        bear_ccys = [(r.ccy, r.name, False) for r in bears]
    elif bulls and not bears:
        # Case C: only BULL — use MIXED with negative CHG% as proxy-BEAR
        proxy_bears = [r for r in mixed if r.daily_chg_pct < 0]
        if not proxy_bears:
            return []
        bull_ccys = [(r.ccy, r.name, False) for r in bulls]
        bear_ccys = [(r.ccy, r.name, True) for r in proxy_bears]
    else:
        return []

    # Generate pairs
    candidates: list[PairCandidate] = []
    for b_ccy, b_idx, b_proxy in bull_ccys:
        for e_ccy, e_idx, e_proxy in bear_ccys:
            if b_ccy == e_ccy:
                continue
            is_proxy = b_proxy or e_proxy
            # Try BULL_BASE + BEAR_QUOTE → BULL pair
            pair_bull = b_ccy + e_ccy
            if pair_bull in PAIR_SET:
                candidates.append(PairCandidate(
                    pair=pair_bull, expected_bias=1,
                    bull_ccy=b_ccy, bear_ccy=e_ccy,
                    bull_idx=b_idx, bear_idx=e_idx,
                    is_proxy=is_proxy,
                ))
            # Try BEAR_BASE + BULL_QUOTE → BEAR pair
            pair_bear = e_ccy + b_ccy
            if pair_bear in PAIR_SET:
                candidates.append(PairCandidate(
                    pair=pair_bear, expected_bias=-1,
                    bull_ccy=b_ccy, bear_ccy=e_ccy,
                    bull_idx=b_idx, bear_idx=e_idx,
                    is_proxy=is_proxy,
                ))

    # Deduplicate (same pair could come from multiple combos — keep first)
    seen = set()
    unique = []
    for c in candidates:
        if c.pair not in seen:
            seen.add(c.pair)
            unique.append(c)
    return unique


# ── Phase 3: fetch CHG% for candidate pairs ───────────────────────────
def _fetch_pair_chg(pair: str) -> tuple[str, float | None]:
    """Fetch daily CHG% close-to-close for one pair."""
    symbol = f"OANDA:{pair}"
    df_d = fetch_tv_ohlc(symbol, "D", 5)
    if df_d is None or len(df_d) < 2:
        return pair, None
    close = float(df_d["close"].iloc[-1])
    prev = float(df_d["close"].iloc[-2])
    chg = ((close - prev) / prev * 100) if prev != 0 else 0.0
    return pair, chg


def fetch_candidate_chg(candidates: list[PairCandidate], min_chg: float,
                        workers: int = 5) -> list[PairCandidate]:
    """Fetch CHG% for each candidate pair, filter by direction + threshold."""
    if not candidates:
        return []

    pairs_to_fetch = list({c.pair for c in candidates})
    total = len(pairs_to_fetch)
    print(f"\n  Phase 3: Fetching CHG% for {total} candidate pairs ...")

    chg_map: dict[str, float] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_pair_chg, p): p for p in pairs_to_fetch}
        for future in as_completed(futures):
            pair = futures[future]
            done += 1
            try:
                _, chg = future.result()
                if chg is not None:
                    chg_map[pair] = chg
                print(f"\r  [{done}/{total}] {pair}", end="", flush=True)
            except Exception:
                print(f"\r  [{done}/{total}] {pair} ERR", end="", flush=True)

    print("\r" + " " * 50 + "\r", end="")

    # Apply CHG% to candidates and filter
    confirmed = []
    for c in candidates:
        if c.pair not in chg_map:
            continue
        c.daily_chg_pct = chg_map[c.pair]
        chg_ok = abs(c.daily_chg_pct) > min_chg
        dir_ok = (c.expected_bias == 1 and c.daily_chg_pct > 0) or \
                 (c.expected_bias == -1 and c.daily_chg_pct < 0)
        c.chg_confirmed = chg_ok and dir_ok
        confirmed.append(c)

    return confirmed


# ── display ────────────────────────────────────────────────────────────
BIAS_LABEL = {1: "BULL", -1: "BEAR", 0: "MIXED"}
PX_LABEL = {1: "> Close", -1: "< Open", 0: "Inside"}


def print_indices(indices: list[IndexResult]) -> None:
    print(f"\n  --- INDICES ---")
    header = f"  {'IDX':<5} {'CCY':<5} {'D1':<10} {'W1':<10} {'CHG%':<8} {'BIAS':<6} {'ROLE':<12}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for r in sorted(indices, key=lambda x: x.name):
        chg = f"{r.daily_chg_pct:+.2f}%"
        print(f"  {r.name:<5} {r.ccy:<5} {PX_LABEL[r.px_vs_d1]:<10} {PX_LABEL[r.px_vs_w1]:<10} {chg:<8} {BIAS_LABEL[r.final_bias]:<6} {r.role:<12}")


def print_candidates(candidates: list[PairCandidate], show_all: bool) -> None:
    confirmed = [c for c in candidates if c.chg_confirmed]
    rejected = [c for c in candidates if not c.chg_confirmed]

    print(f"\n  --- CONFIRMED PAIRS ---")
    if not confirmed:
        print("  (none)")
    else:
        header = f"  {'PAIR':<8} {'BIAS':<6} {'CHG%':<8} {'STRONG':<10} {'WEAK':<10} {'TYPE'}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for c in sorted(confirmed, key=lambda x: abs(x.daily_chg_pct), reverse=True):
            chg = f"{c.daily_chg_pct:+.2f}%"
            bias = "BULL" if c.expected_bias == 1 else "BEAR"
            strong = f"{c.bull_ccy}({c.bull_idx})"
            weak = f"{c.bear_ccy}({c.bear_idx})"
            tag = "proxy" if c.is_proxy else "direct"
            print(f"  {c.pair:<8} {bias:<6} {chg:<8} {strong:<10} {weak:<10} {tag}")

    if show_all and rejected:
        print(f"\n  --- REJECTED (CHG% not confirmed) ---")
        for c in sorted(rejected, key=lambda x: x.pair):
            chg = f"{c.daily_chg_pct:+.2f}%" if c.daily_chg_pct != 0 else "N/A"
            bias = "BULL" if c.expected_bias == 1 else "BEAR"
            print(f"  {c.pair:<8} {bias:<6} {chg:<8} expected but CHG% wrong direction")


def build_telegram_message(indices: list[IndexResult],
                           candidates: list[PairCandidate]) -> str | None:
    confirmed = [c for c in candidates if c.chg_confirmed]
    if not confirmed:
        return None

    lines = ["PX_RENKO_V4", ""]

    # Indices & proxies with a role
    active = [r for r in indices if r.role]
    active_bears = sorted([r for r in active if "BEAR" in r.role],
                          key=lambda x: x.daily_chg_pct)
    active_bulls = sorted([r for r in active if "BULL" in r.role],
                          key=lambda x: x.daily_chg_pct, reverse=True)

    lines.append("\U0001f4ca INDICES et PROXY VALIDE")
    for r in active_bears:
        lines.append(f"\U0001f534 {r.ccy} ({r.daily_chg_pct:+.2f}%)")
    for r in active_bulls:
        lines.append(f"\U0001f7e2 {r.ccy} ({r.daily_chg_pct:+.2f}%)")

    lines.append("")
    lines.append("\U0001f3c6 A TRADER")
    pair_bears = sorted([c for c in confirmed if c.expected_bias == -1],
                        key=lambda x: x.daily_chg_pct)
    pair_bulls = sorted([c for c in confirmed if c.expected_bias == 1],
                        key=lambda x: x.daily_chg_pct, reverse=True)
    for c in pair_bears:
        lines.append(f"\U0001f534 {c.pair} ({c.daily_chg_pct:+.2f}%)")
    for c in pair_bulls:
        lines.append(f"\U0001f7e2 {c.pair} ({c.daily_chg_pct:+.2f}%)")

    lines.append("")
    now_paris = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append(f"\u23f0 {now_paris} Paris")
    return "\n".join(lines)


# ── Telegram ───────────────────────────────────────────────────────────
def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [WARN] Telegram credentials not configured (.env)")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = http_requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        ok = resp.json().get("ok", False)
        if ok:
            print("  Telegram: sent")
        else:
            print(f"  Telegram: failed ({resp.text[:100]})")
        return ok
    except Exception as e:
        print(f"  Telegram: error ({e})")
        return False


# ── main ───────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(
        description="Px/Renko V4 Index-Driven Screener – derives pairs from index combinations."
    )
    parser.add_argument("--length", type=int, default=14, help="ATR length for Renko (default: 14).")
    parser.add_argument("--min-chg", type=float, default=MIN_CHG_PCT, help="Min |CHG%%| daily (default: 0.15).")
    parser.add_argument("--show-all", action="store_true", help="Show rejected candidates too.")
    parser.add_argument("--no-telegram", action="store_true", help="Skip Telegram, console only.")
    parser.add_argument("--workers", type=int, default=5, help="Parallel threads (default: 5).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    t_start = time.time()

    # Phase 1: scan indices
    indices = scan_indices(args.length, args.min_chg, workers=args.workers)
    t_phase1 = time.time() - t_start
    print(f"  Phase 1 done in {t_phase1:.1f}s ({len(indices)} indices)\n")

    # Classify indices
    bulls = [r for r in indices if r.final_bias == 1]
    bears = [r for r in indices if r.final_bias == -1]
    mixed = [r for r in indices if r.final_bias == 0]

    for r in bulls:
        r.role = "BULL"
    for r in bears:
        r.role = "BEAR"

    # Assign proxy roles if needed
    if bears and not bulls:
        for r in mixed:
            if r.daily_chg_pct > 0:
                r.role = "proxy-BULL"
    elif bulls and not bears:
        for r in mixed:
            if r.daily_chg_pct < 0:
                r.role = "proxy-BEAR"

    print_indices(indices)

    # Phase 2: derive candidates
    print(f"\n  Phase 2: Deriving candidate pairs ...")
    candidates = derive_candidates(indices, args.min_chg)
    n_cand = len(candidates)
    if n_cand == 0:
        print("  No candidate pairs found (need at least 1 BULL + 1 BEAR index).")
        return 0
    print(f"  → {n_cand} candidate pairs from index combinations")

    # Phase 3: fetch CHG% and confirm
    candidates = fetch_candidate_chg(candidates, args.min_chg, workers=args.workers)
    elapsed = time.time() - t_start

    confirmed = [c for c in candidates if c.chg_confirmed]
    print(f"\n  Scan complete in {elapsed:.1f}s")

    # Display
    print_candidates(candidates, args.show_all)

    n_bulls = sum(1 for c in confirmed if c.expected_bias == 1)
    n_bears = sum(1 for c in confirmed if c.expected_bias == -1)
    print(f"\n  Result: {n_bulls} BULL, {n_bears} BEAR / {n_cand} candidates\n")

    # Telegram
    if not args.no_telegram:
        msg = build_telegram_message(indices, candidates)
        if msg:
            print(msg)
            print("")
            send_telegram(msg)
        else:
            print("  No confirmed pairs — no Telegram sent.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
