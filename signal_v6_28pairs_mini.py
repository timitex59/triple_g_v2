#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_v6_28pairs_mini.py
=========================
Signal detector v6 MINI -- 28 paires LIVE via WebSocket TradingView
Version multi-timeframe: 15m (execution) + 1H + Daily.

Usage :
  python signal_v6_28pairs_mini.py                    # scan complet 28 paires
  python signal_v6_28pairs_mini.py --pair EURJPY      # une paire
  python signal_v6_28pairs_mini.py --loop             # boucle
  python signal_v6_28pairs_mini.py --loop --interval 1800
  python signal_v6_28pairs_mini.py --filter ROBUST    # ROBUST/MOYEN/FRAGILE
"""

import json, os, random, string, time, argparse, warnings
warnings.filterwarnings("ignore")
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pandas_ta as pta
import requests
from websocket import WebSocketConnectionClosedException, create_connection

try:
    from dotenv import load_dotenv; load_dotenv()
except Exception:
    pass

# ================================================================
# PARAMETRES OPTIMAUX v6 -- 28 paires
# ================================================================
PAIRS_CONFIG = {
    # ---- ROBUST (>= 60%) ----
    'AUDJPY': (30,  6, 21, False, 0.05, 0.05, 0.2, 100),
    'GBPNZD': (20,  6, 21, True,  0.02, 0.02, 0.2, 100),
    'NZDUSD': ( 0,  0, 24, True,  0.02, 0.02, 0.2, 100),
    'EURJPY': ( 0,  6, 21, True,  0.02, 0.02, 0.2,  86),
    'NZDCAD': (20,  4, 22, True,  0.10, 0.10, 0.2,  83),
    'XAUUSD': (20,  4, 22, True,  0.10, 0.10, 0.2,  83),
    # ---- MOYEN (35-60%) ----
    'CADCHF': ( 0,  4, 22, False, 0.02, 0.02, 0.2,  48),
    'CHFJPY': ( 0,  0, 24, False, 0.10, 0.10, 0.2,  37),
    # ---- FRAGILE (<35%) ----
    'EURAUD': (25,  0, 24, True,  0.10, 0.10, 0.2,  20),
    'AUDNZD': (25,  8, 20, True,  0.10, 0.10, 0.2,  14),
    'AUDUSD': (20,  8, 20, True,  0.02, 0.02, 0.2,  14),
    'USDCHF': (35,  8, 20, True,  0.10, 0.10, 0.2,  11),
    'USDJPY': (25,  8, 20, True,  0.05, 0.05, 0.2,  10),
    'EURUSD': (35,  0, 24, False, 0.10, 0.10, 0.2,   9),
    'CADJPY': (35,  6, 21, False, 0.05, 0.05, 0.2,   9),
    'USDCAD': ( 0,  6, 21, True,  0.02, 0.02, 0.2,   7),
    'GBPUSD': (25,  8, 20, True,  0.10, 0.10, 0.2,   2),
    'EURCHF': (35,  0, 24, True,  0.05, 0.05, 0.2,   2),
    'GBPCHF': (20,  0, 24, True,  0.05, 0.05, 0.2,   1),
    'AUDCHF': (20,  0, 24, True,  0.02, 0.02, 0.2,   0),
    'AUDCAD': ( 0,  0, 24, False, 0.05, 0.05, 0.2,   0),
    'EURGBP': (30,  0, 24, True,  0.02, 0.02, 0.2,   0),
    'EURCAD': (20,  6, 21, True,  0.05, 0.05, 0.2,   0),
    'EURNZD': ( 0,  0, 24, True,  0.02, 0.02, 0.2,   0),
    'GBPAUD': ( 0,  0, 24, False, 0.10, 0.10, 0.2,   0),
    'GBPJPY': (20,  8, 20, False, 0.02, 0.02, 0.2,   0),
    'GBPCAD': (35,  8, 20, True,  0.02, 0.02, 0.2,   0),
    'NZDJPY': (35,  0, 24, True,  0.02, 0.02, 0.2,   0),
    'NZDCHF': ( 0,  8, 20, True,  0.02, 0.02, 0.2,   0),
}

def robustness_label(r):
    if r >= 60: return 'ROBUST'
    if r >= 35: return 'MOYEN'
    return 'FRAGILE'

JPY_PAIRS = {'AUDJPY','CADJPY','CHFJPY','EURJPY','GBPJPY','NZDJPY','USDJPY'}
EMA_LENS  = [20, 25, 30, 35, 40, 45, 50, 55]
PYRAMIDING = 5
LOT_SIZE   = 0.1
CONTRACT   = 100_000
N_CANDLES  = {"15": 2200, "60": 1000, "D": 400}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# ================================================================
# WEBSOCKET TRADINGVIEW
# ================================================================
def _gen_sid():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))

def _msg(func, args):
    c = json.dumps({"m": func, "p": args})
    return f"~m~{len(c)}~m~{c}"

def _frames(raw):
    if raw in ("~h", "h"): return [raw]
    frames, i = [], 0
    while raw.startswith("~m~", i):
        i += 3; j = raw.find("~m~", i)
        if j == -1: break
        size = int(raw[i:j]); i = j + 3
        frames.append(raw[i: i + size]); i += size
    return frames or [raw]

def fetch_ohlc(symbol, interval, n, timeout=20, retries=2):
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(
                "wss://prodata.tradingview.com/socket.io/websocket",
                header={"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"},
                timeout=timeout,
            )
            sid = _gen_sid()
            ws.send(_msg("chart_create_session", [sid, ""]))
            ws.send(_msg("resolve_symbol", [
                sid, "sds_sym_1",
                f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}'
            ]))
            ws.send(_msg("create_series", [sid, "sds_1", "s1", "sds_sym_1", interval, n, ""]))

            points = []; t0 = time.time()
            while time.time() - t0 < 15:
                try: raw = ws.recv()
                except WebSocketConnectionClosedException: break
                for f in _frames(raw):
                    if f in ("~h", "h"): ws.send("~h"); continue
                    if '"m":"timescale_update"' in f:
                        payload = json.loads(f)
                        s = payload.get("p", [None, {}])[1]
                        if isinstance(s, dict) and "sds_1" in s:
                            points = s["sds_1"].get("s", []) or points
                    if "series_completed" in f: break
                if "series_completed" in raw: break

            if not points: continue
            rows = []
            for item in points:
                v = item.get("v", [])
                if len(v) < 5: continue
                ts, o, h, l, c = v[:5]
                rows.append({"Datetime": ts, "Open": o, "High": h, "Low": l, "Close": c})
            if not rows: continue
            df = pd.DataFrame(rows)
            df["Datetime"] = pd.to_datetime(df["Datetime"], unit="s", utc=True)
            return df.set_index("Datetime").sort_index()
        except Exception:
            if attempt >= retries: return None
        finally:
            if ws:
                try: ws.close()
                except Exception: pass
    return None

def fetch_pair(pair):
    sym = f"OANDA:{pair}"
    m15 = fetch_ohlc(sym, "15", N_CANDLES["15"])
    h1  = fetch_ohlc(sym, "60", N_CANDLES["60"])
    d1  = fetch_ohlc(sym, "D",  N_CANDLES["D"])
    return m15, h1, d1

# ================================================================
# INDICATEURS (Pine-fideles)
# ================================================================
def ema_pine(s, n):
    result = s.copy().astype(float); alpha = 2.0 / (n + 1)
    valid = s.dropna()
    if len(valid) < n: return pd.Series(np.nan, index=s.index)
    sl = s.index.get_loc(valid.index[n - 1])
    result.iloc[:sl] = np.nan; result.iloc[sl] = valid.iloc[:n].mean()
    for i in range(sl + 1, len(result)):
        result.iloc[i] = alpha * s.iloc[i] + (1 - alpha) * result.iloc[i - 1] \
                         if not np.isnan(s.iloc[i]) else result.iloc[i - 1]
    return result

def psar(h, l, af0=0.1, af=0.1, max_af=0.2):
    r = pta.psar(h, l, af0=af0, af=af, max_af=max_af)
    cl = [c for c in r.columns if 'PSARl' in c][0]
    cs = [c for c in r.columns if 'PSARs' in c][0]
    return r[cl].combine_first(r[cs])

def adx14(h, l, c):
    res = pta.adx(h, l, c, length=14)
    return res[[x for x in res.columns if x.startswith('ADX_')][0]]

def xo(a, b): return (a > b) & (a.shift(1) <= b.shift(1))
def xu(a, b): return (a < b) & (a.shift(1) >= b.shift(1))
def vwhen(cond, s): return s.where(cond).ffill()
def ri(df):
    df = df.reset_index()
    return df.rename(columns={'index': 'Datetime'}) if 'Datetime' not in df.columns else df

# ================================================================
# BUILD DF COMPLET (MINI: 15m + 1H + D)
# ================================================================
def build_df(pair, m15, h1, d1, af0, af, maf):
    if m15 is None or h1 is None or d1 is None: return None

    # EMA / ADX sur 1H
    for n in EMA_LENS:
        h1[f'e{n}'] = ema_pine(h1['Close'], n)
        d1[f'e{n}'] = ema_pine(d1['Close'], n)

    h1['adx']      = adx14(h1['High'], h1['Low'], h1['Close'])
    h1['ema_bull'] = np.logical_and.reduce([h1[f'e{EMA_LENS[i]}'] > h1[f'e{EMA_LENS[i+1]}'] for i in range(7)])
    h1['ema_bear'] = np.logical_and.reduce([h1[f'e{EMA_LENS[i]}'] < h1[f'e{EMA_LENS[i+1]}'] for i in range(7)])

    # Filtre structurel sur Daily
    d1['w_above'] = np.logical_and.reduce([d1['Close'] > d1[f'e{n}'] for n in EMA_LENS])
    d1['w_below'] = np.logical_and.reduce([d1['Close'] < d1[f'e{n}'] for n in EMA_LENS])
    d1['w_close'] = d1['Close']

    h1['last_bear_open'] = h1['Open'].where(h1['Close'] < h1['Open']).ffill()
    h1['last_bull_open'] = h1['Open'].where(h1['Close'] > h1['Open']).ffill()

    DC = ['ema_bull','ema_bear','adx','last_bear_open','last_bull_open']
    WC = ['w_above','w_below','w_close']
    ds = h1[DC].shift(1).rename(columns={c: 'd_'+c for c in DC})
    ws = d1[WC].shift(1).rename(columns={c: 'w_'+c for c in WC})

    df = m15.copy()
    df = pd.merge_asof(ri(df), ri(ds), on='Datetime', direction='backward').set_index('Datetime')
    df = pd.merge_asof(ri(df), ri(ws), on='Datetime', direction='backward').set_index('Datetime')
    df['hour'] = df.index.hour; df['month'] = df.index.month; df['day'] = df.index.day

    # SAR 1H + SAR Daily
    dsar = psar(h1['High'], h1['Low'], af0, af, maf).rename('d_sar').shift(1)
    wsar = psar(d1['High'], d1['Low'], af0, af, maf).rename('w_sar').shift(1)
    df = pd.merge_asof(ri(df), ri(dsar.reset_index().rename(columns={'index':'Datetime','Datetime':'Datetime'})),
                       on='Datetime', direction='backward').set_index('Datetime')
    df = pd.merge_asof(ri(df), ri(wsar.reset_index().rename(columns={'index':'Datetime','Datetime':'Datetime'})),
                       on='Datetime', direction='backward').set_index('Datetime')

    wa = df['w_w_above'].fillna(False) if 'w_w_above' in df.columns else df['w_above'].fillna(False)
    wb = df['w_w_below'].fillna(False) if 'w_w_below' in df.columns else df['w_below'].fillna(False)
    wc = df['w_w_close'] if 'w_w_close' in df.columns else df['w_close']

    df['bias_bull'] = df['d_ema_bull'].fillna(False) & wa & (wc > df['w_sar']).fillna(False)
    df['bias_bear'] = df['d_ema_bear'].fillna(False) & wb & (wc < df['w_sar']).fillna(False)
    df['ref_level'] = np.where(df['bias_bull'], df['d_last_bear_open'],
                      np.where(df['bias_bear'], df['d_last_bull_open'], np.nan))

    # SAR execution sur 15m
    df['sar']      = psar(m15['High'], m15['Low'], af0, af, maf)
    df['bull']     = xo(df['Close'], df['sar'])
    df['bear']     = xu(df['Close'], df['sar'])
    df['bull_vw0'] = vwhen(df['bull'], df['sar'])
    df['bear_vw0'] = vwhen(df['bear'], df['sar'])
    df['cab']      = xo(df['Close'], df['bear_vw0'])
    df['cbb']      = xu(df['Close'], df['bull_vw0'])
    return df

# ================================================================
# SIMULATION COMPLETE
# ================================================================
def simulate(df, pair, adx_t, ss, se, hol):
    sp  = 0.025 if pair in JPY_PAIRS else 0.00015
    aok = (df['d_adx'].fillna(0) > adx_t).values if adx_t > 0 else np.ones(len(df), bool)
    sok = ((df['hour'] >= ss) & (df['hour'] < se)).values
    hok = (~(((df['month']==12)&(df['day']>=20))|((df['month']==1)&(df['day']<=5)))).values if hol else np.ones(len(df), bool)
    FLT = aok & sok & hok

    N=len(df); C=df['Close'].values.astype(float); SAR=df['sar'].values.astype(float)
    BUL=df['bull'].values.astype(bool); BEA=df['bear'].values.astype(bool)
    BV=df['bull_vw0'].values.astype(float); BRV=df['bear_vw0'].values.astype(float)
    BB=df['bias_bull'].values.astype(bool); BBR=df['bias_bear'].values.astype(bool)
    RL=df['ref_level'].values.astype(float)
    CAB=df['cab'].values.astype(bool); CBB=df['cbb'].values.astype(bool)

    pos=[]; trades=[]; lt=st=False; lvls=lvss=np.nan
    plr=psr=np.nan; nla=nsa=False; prl=np.nan
    lsb=None; lst=None

    nl = lambda: sum(1 for p in pos if p[0]=='L')
    ns = lambda: sum(1 for p in pos if p[0]=='S')

    def ca(side, ep, bar):
        for p in list(pos):
            if p[0]==side:
                d=1 if side=='L' else -1
                trades.append((ep-p[1])*d*LOT_SIZE*CONTRACT - sp*LOT_SIZE*CONTRACT > 0)
                pos.remove(p)

    for i in range(1, N):
        c=C[i]; s=SAR[i]; r=RL[i]; bb=BB[i]; bbr=BBR[i]; f=FLT[i]
        bv=BV[i]; brv=BRV[i]; _nl=nl(); _ns=ns()
        rn=np.isnan(r); pn=np.isnan(prl)
        rc=(rn!=pn) or (not rn and not pn and r!=prl); prl=r
        if rc: lvls=lvss=np.nan
        if rc or not bb:  plr=np.nan; nla=False
        if rc or not bbr: psr=np.nan; nsa=False

        lsig=(bb and BUL[i] and not rn and c>r and s>r and (np.isnan(lvls) or s<lvls))
        ssig=(bbr and BEA[i] and not rn and c<r and s<r and (np.isnan(lvss) or s>lvss))
        if lsig: lvls=s
        if ssig: lvss=s
        if _nl>0 and lsig: lt=True
        if _ns>0 and ssig: st=True

        elt=(C[i]<BV[i]) and (C[i-1]>=BV[i-1]) if not np.isnan(BV[i]) and not np.isnan(BV[i-1]) else False
        est=(C[i]>BRV[i]) and (C[i-1]<=BRV[i-1]) if not np.isnan(BRV[i]) and not np.isnan(BRV[i-1]) else False
        if _nl>0 and lt and elt: ca('L',c,i); lt=False; _nl=nl()
        if _ns>0 and st and est: ca('S',c,i); st=False; _ns=ns()

        plb=(f and bb  and not rn and not np.isnan(brv) and CAB[i] and brv<r)
        psb=(f and bbr and not rn and not np.isnan(bv)  and CBB[i] and bv>r)
        if plb:   plr=bv; nla=False
        elif not np.isnan(plr) and c<plr: nla=True
        if psb:   psr=brv; nsa=False
        elif not np.isnan(psr) and c>psr: nsa=True

        nls=(f and bb  and nla and not rn and not np.isnan(plr)  and not np.isnan(brv) and CAB[i] and brv<r and brv<plr)
        nss=(f and bbr and nsa and not rn and not np.isnan(psr)  and not np.isnan(bv)  and CBB[i] and bv>r  and bv>psr)
        if nls: plr=bv; nla=False
        if nss: psr=brv; nsa=False

        if (plb or nls) and _nl+1<=PYRAMIDING and _ns==0: pos.append(('L',c+sp/2,i)); st=False; lsb=i; lst='LONG'
        if (psb or nss) and _ns+1<=PYRAMIDING and _nl==0: pos.append(('S',c-sp/2,i)); lt=False; lsb=i; lst='SHORT'
        if nl()==0: lt=False
        if ns()==0: st=False

    last = N - 1
    return {
        'pos': pos, 'lt': lt, 'st': st, 'plr': plr, 'psr': psr,
        'nla': nla, 'nsa': nsa,
        'bb': BB[last], 'bbr': BBR[last], 'rl': RL[last],
        'sar': SAR[last], 'c': C[last], 'bv': BV[last], 'brv': BRV[last],
        'aok': aok[last], 'sok': sok[last], 'hok': hok[last], 'fok': FLT[last],
        'ts': df.index[last], 'trades': trades,
        'lsb': lsb, 'lst': lst, 'idx': df.index,
    }

# ================================================================
# ANALYSE PAIRE
# ================================================================
def fp(p, pair):
    if np.isnan(p): return "n/a"
    return f"{p:.3f}" if pair in JPY_PAIRS else f"{p:.5f}"

def analyse(pair):
    cfg = PAIRS_CONFIG[pair]
    adx, ss, se, hol, af0, af, maf, rob = cfg
    label = robustness_label(rob)

    m15, h1, d1 = fetch_pair(pair)
    if m15 is None or h1 is None or d1 is None:
        return {'pair': pair, 'error': True, 'label': label, 'rob': rob}

    df = build_df(pair, m15, h1, d1, af0, af, maf)
    if df is None:
        return {'pair': pair, 'error': True, 'label': label, 'rob': rob}

    st = simulate(df, pair, adx, ss, se, hol)
    chg_cc_daily = np.nan
    try:
        if d1 is not None and len(d1) >= 2:
            c0 = float(d1['Close'].iloc[-1])
            c1 = float(d1['Close'].iloc[-2])
            if c1 != 0:
                chg_cc_daily = (c0 - c1) / c1 * 100.0
    except Exception:
        chg_cc_daily = np.nan
    th = st['trades']
    nt = len(th); wr = sum(th)/nt*100 if nt else 0.0

    longs  = [p for p in st['pos'] if p[0]=='L']
    shorts = [p for p in st['pos'] if p[0]=='S']

    sig = 'AUCUN'
    if st['lsb'] == len(df)-1:
        sig = f"ENTREE {st['lst']}"

    watch = []
    bv=st['bv']; brv=st['brv']; r=st['rl']
    if st['bb'] and not np.isnan(r):
        if not np.isnan(brv) and brv < r:
            watch.append(f"Pre_LONG si close > bear_vw0 ({fp(brv,pair)})")
        if st['nla'] and not np.isnan(st['plr']):
            watch.append(f"Neo_LONG arme (ref={fp(st['plr'],pair)})")
        if longs and st['lt']:
            watch.append(f"Trailing LONG actif -- exit si close < bull_vw0 ({fp(bv,pair)})")
    if st['bbr'] and not np.isnan(r):
        if not np.isnan(bv) and bv > r:
            watch.append(f"Pre_SHORT si close < bull_vw0 ({fp(bv,pair)})")
        if st['nsa'] and not np.isnan(st['psr']):
            watch.append(f"Neo_SHORT arme (ref={fp(st['psr'],pair)})")
        if shorts and st['st']:
            watch.append(f"Trailing SHORT actif -- exit si close > bear_vw0 ({fp(brv,pair)})")

    return {
        'pair': pair, 'label': label, 'rob': rob,
        'biais': 'BULL' if st['bb'] else ('BEAR' if st['bbr'] else 'NEUTRE'),
        'fok': st['fok'], 'aok': st['aok'], 'sok': st['sok'], 'hok': st['hok'],
        'close': st['c'], 'sar': st['sar'], 'ref': st['rl'],
        'bv': st['bv'], 'brv': st['brv'],
        'longs': longs, 'shorts': shorts,
        'lt': st['lt'], 'st_arm': st['st'],
        'nla': st['nla'], 'nsa': st['nsa'],
        'plr': st['plr'], 'psr': st['psr'],
        'sig': sig, 'watch': watch,
        'nt': nt, 'wr': wr,
        'ts': st['ts'], 'idx': st['idx'],
        'chg_cc_daily': chg_cc_daily,
        'error': False,
    }

# ================================================================
# AFFICHAGE RESUME TABULAIRE
# ================================================================
BIAIS_SYM = {'BULL': 'BULL ^', 'BEAR': 'BEAR v', 'NEUTRE': '  --  '}

def print_summary(results):
    now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*90}")
    print(f"  SIGNAL v6 MINI LIVE -- 28 paires -- {now} Paris")
    print(f"{'='*90}")

    order = {'ROBUST': 0, 'MOYEN': 1, 'FRAGILE': 2}
    def sort_key(r):
        b = 0 if r.get('biais') in ('BULL','BEAR') else 1
        return (order.get(r.get('label','FRAGILE'), 2), b, r.get('pair',''))
    results = sorted(results, key=sort_key)

    hdr = f"  {'Paire':<8} {'Rob':>4}% {'Label':<8} {'Biais':<8} {'Filtre':<8} {'Close':>10} {'SAR 15m':>10} {'Ref':>10} {'Pos':>6} {'WR%':>6} {'Signal / A surveiller'}"
    sep = "  " + "-"*88
    print(hdr); print(sep)

    current_label = None
    for r in results:
        if r.get('error'):
            print(f"  {r['pair']:<8} {'?':>4}  {r['label']:<8}  ERREUR (donnees manquantes)")
            continue
        if r['label'] != current_label:
            print(f"\n  --- {r['label']} ---")
            current_label = r['label']
        b    = BIAIS_SYM.get(r['biais'], '  --  ')
        flt  = "OK    " if r['fok'] else ("ADX   " if not r['aok'] else ("SES   " if not r['sok'] else "HOL   "))
        pos  = ""
        if r['longs']:  pos += f"+{len(r['longs'])}L"
        if r['shorts']: pos += f"-{len(r['shorts'])}S"
        if not pos:     pos = " none"
        sig_watch = r['sig'] if r['sig'] != 'AUCUN' else (r['watch'][0][:40] if r['watch'] else '-')
        trail = ""
        if r['longs'] and r['lt']:  trail = "[TRL]"
        if r['shorts'] and r['st_arm']: trail = "[TRL]"
        print(f"  {r['pair']:<8} {r['rob']:>4}  {r['label']:<8} {b:<8} {flt:<8}"
              f" {fp(r['close'],r['pair']):>10} {fp(r['sar'],r['pair']):>10}"
              f" {fp(r['ref'],r['pair']):>10} {pos:>6}{trail:<5}  {r['wr']:>5.1f}%  {sig_watch}")

    print(sep)

def print_active_signals(results):
    actives = [r for r in results if not r.get('error') and
               (r['sig'] != 'AUCUN' or r['longs'] or r['shorts'])]
    if not actives:
        print("\n  Aucune position ouverte ni signal d'entree en ce moment.")
        return

    print(f"\n{'='*90}")
    print(f"  DETAIL -- POSITIONS OUVERTES & SIGNAUX")
    print(f"{'='*90}")
    for r in actives:
        pair = r['pair']
        b = BIAIS_SYM.get(r['biais'])
        print(f"\n  {pair} [{r['label']} {r['rob']}%]  Biais:{b}  Close:{fp(r['close'],pair)}  SAR15m:{fp(r['sar'],pair)}")
        c = r['close']
        for p in r['longs']:
            ts  = r['idx'][p[2]]
            pnl = (c - p[1]) * LOT_SIZE * CONTRACT
            trl = " [TRAILING ACTIF]" if r['lt'] else ""
            print(f"    LONG  entree {str(ts)[:16]}  px={fp(p[1],pair)}  PnL~{pnl:+.0f} pts{trl}")
        for p in r['shorts']:
            ts  = r['idx'][p[2]]
            pnl = (p[1] - c) * LOT_SIZE * CONTRACT
            trl = " [TRAILING ACTIF]" if r['st_arm'] else ""
            print(f"    SHORT entree {str(ts)[:16]}  px={fp(p[1],pair)}  PnL~{pnl:+.0f} pts{trl}")
        if r['sig'] != 'AUCUN':
            print(f"    *** {r['sig']} ***")
        for w in r['watch']:
            print(f"    -> {w}")
        if r['nla']: print(f"    Neo_LONG arme (ref={fp(r['plr'],pair)})")
        if r['nsa']: print(f"    Neo_SHORT arme (ref={fp(r['psr'],pair)})")

# ================================================================
# TELEGRAM
# ================================================================
def telegram_text(results):
    now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines = []
    def fmt_chg(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "n/a"
        return f"{v:+.2f}%"

    seen_open  = {}
    seen_trail = {}

    for r in results:
        if r.get('error') or (not r['longs'] and not r['shorts']):
            continue
        icon      = "🟢" if r['longs'] else "🔴"
        has_trail = (r['longs'] and r['lt']) or (r['shorts'] and r['st_arm'])
        if has_trail:
            seen_trail[r['pair']] = icon
        else:
            seen_open[r['pair']] = icon

    new_entries = [r for r in results if not r.get('error') and r['sig'] != 'AUCUN']

    if seen_trail:
        lines.append("TRAILING")
    elif new_entries:
        lines.append("ENTREE")

    if new_entries:
        if seen_trail:
            lines.append("")
            lines.append("Entree")
        for r in new_entries:
            icon = "🟢" if "LONG" in r['sig'] else "🔴"
            lines.append(f"  {icon}{r['pair']} ({fmt_chg(r.get('chg_cc_daily'))})")

    if seen_open:
        lines.append("")
        lines.append("Open")
        for pair, icon in seen_open.items():
            rr = next((x for x in results if x.get('pair') == pair), None)
            lines.append(f"  {icon}{pair} ({fmt_chg(rr.get('chg_cc_daily') if rr else np.nan)})")

    if seen_trail:
        lines.append("")
        lines.append("Trailing")
        for pair, icon in seen_trail.items():
            rr = next((x for x in results if x.get('pair') == pair), None)
            lines.append(f"  {icon}{pair} ({fmt_chg(rr.get('chg_cc_daily') if rr else np.nan)})")

    if not new_entries and not seen_open and not seen_trail:
        lines.append("⚪ Aucune position active")

    lines.append("")
    lines.append(f"⏰ {now} Paris")
    return "\n".join(lines)

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        print("  Telegram: envoye")
    except Exception as e:
        print(f"  Telegram: erreur ({e})")

# ================================================================
# EXPORT CSV
# ================================================================
def export_csv(results, fname='signal_v6_28pairs_mini_current.csv'):
    rows = []
    for r in results:
        if r.get('error'):
            rows.append({'pair': r['pair'], 'label': r['label'], 'rob': r['rob'], 'error': True})
            continue
        rows.append({
            'pair': r['pair'], 'label': r['label'], 'rob': r['rob'],
            'timestamp_utc': str(r['ts'])[:16],
            'biais': r['biais'], 'filters_ok': r['fok'],
            'close': round(r['close'], 5),
            'sar_15m': round(r['sar'], 5),
            'ref_level': round(r['ref'], 5) if not np.isnan(r['ref']) else '',
            'bull_vw0': round(r['bv'], 5) if not np.isnan(r['bv']) else '',
            'bear_vw0': round(r['brv'], 5) if not np.isnan(r['brv']) else '',
            'open_longs': len(r['longs']), 'open_shorts': len(r['shorts']),
            'trailing_long': r['lt'], 'trailing_short': r['st_arm'],
            'neo_long_armed': r['nla'], 'neo_short_armed': r['nsa'],
            'signal_last_bar': r['sig'],
            'wr_hist_pct': round(r['wr'], 1), 'n_trades_hist': r['nt'],
            'watch': ' | '.join(r['watch']),
        })
    pd.DataFrame(rows).to_csv(fname, index=False)
    return fname

# ================================================================
# MAIN
# ================================================================
def run_scan(target_pairs):
    t0   = time.time()
    now  = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n  Scan {len(target_pairs)} paires -- {now}")
    print(f"  Source : TradingView WebSocket (OANDA)")
    print("  Timeframes : 15m + 1H + Daily")

    results = []
    for i, pair in enumerate(target_pairs, 1):
        print(f"  [{i:>2}/{len(target_pairs)}] {pair}...", end=' ', flush=True)
        try:
            r = analyse(pair)
            if r.get('error'):
                print("ERREUR")
            else:
                biais = r['biais']
                pos_s = ""
                if r['longs']:  pos_s += f"+{len(r['longs'])}L"
                if r['shorts']: pos_s += f"-{len(r['shorts'])}S"
                sig_s = f" *** {r['sig']} ***" if r['sig'] != 'AUCUN' else ""
                print(f"OK  {biais:<6} {pos_s}{sig_s}")
            results.append(r)
        except Exception as e:
            print(f"ERREUR ({e})")
            results.append({'pair': pair, 'error': True,
                            'label': robustness_label(PAIRS_CONFIG[pair][7]),
                            'rob': PAIRS_CONFIG[pair][7]})

    print_summary(results)
    print_active_signals(results)

    csv_f = export_csv(results)
    tg    = telegram_text(results)
    send_telegram(tg)

    elapsed = time.time() - t0
    print(f"\n  -> {csv_f}")
    print(f"  Temps total : {elapsed:.1f}s  ({elapsed/len(target_pairs):.1f}s/paire)")
    print(f"{'='*90}\n")
    return results

def main():
    parser = argparse.ArgumentParser(description="Signal v6 MINI LIVE -- 28 paires TradingView")
    parser.add_argument('--pair',     default=None, help='Paire unique (ex: EURJPY)')
    parser.add_argument('--filter',   default=None, help='Filtrer par label: ROBUST / MOYEN / FRAGILE')
    parser.add_argument('--loop',     action='store_true', help='Mode boucle')
    parser.add_argument('--interval', type=int, default=3600, help='Intervalle boucle (secondes)')
    args = parser.parse_args()

    if args.pair:
        p = args.pair.upper()
        if p not in PAIRS_CONFIG:
            print(f"Paire {p} inconnue. Disponibles: {', '.join(PAIRS_CONFIG)}")
            return
        target = [p]
    elif args.filter:
        lbl = args.filter.upper()
        target = [p for p, cfg in PAIRS_CONFIG.items() if robustness_label(cfg[7]) == lbl]
        if not target:
            print(f"Aucune paire avec label '{lbl}'")
            return
        print(f"Filtre '{lbl}' -> {len(target)} paires : {', '.join(target)}")
    else:
        target = list(PAIRS_CONFIG.keys())

    if args.loop:
        print(f"Mode LOOP -- scan toutes les {args.interval}s  (Ctrl+C pour arreter)")
        while True:
            try:
                run_scan(target)
            except KeyboardInterrupt:
                print("\nArret."); break
            except Exception as e:
                print(f"Erreur scan: {e}")
            print(f"Prochain scan dans {args.interval}s...")
            time.sleep(args.interval)
    else:
        run_scan(target)

if __name__ == "__main__":
    main()
