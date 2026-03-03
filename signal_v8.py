#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_v7.py
============
Signal detector v7 -- 28 paires LIVE via WebSocket TradingView
Parametres optimaux issus du backtest_tv_v7.py (Bayesian Optuna, 4 folds, stabilite).

Classification :
  ROBUST  = PF >= 2.0  ET consistance folds >= 75%
  MOYEN   = PF >= 1.5  ET consistance folds >= 50%
  FRAGILE = autres

Priorites de trading :
  GBPNZD  PF=13.44  WR=92%  Stab=100%  ← top
  EURAUD  PF=5.20   WR=61%  Stab=71%
  USDCHF  PF=2.78   WR=64%  Stab=100%
  EURCAD  PF=1.77   WR=64%  Stab=100%

Usage :
  python signal_v7.py                    # scan 28 paires
  python signal_v7.py --pair GBPNZD      # une paire
  python signal_v7.py --filter ROBUST    # ROBUST / MOYEN / FRAGILE
  python signal_v7.py --loop             # boucle toutes les heures
  python signal_v7.py --loop --interval 1800
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
# PARAMETRES OPTIMAUX v7 -- 28 paires
# Source : backtest_tv_v7.py  (Optuna 100 essais, 4 folds, stabilite)
# (adx_thresh, sess_start, sess_end, use_holiday, af0, af, max_af, label, pf_test, stab)
# ================================================================
PAIRS_CONFIG = {
    # ---- ROBUST (PF>=2.0 & folds>=75%) ----
    'GBPNZD': (15,  4, 22, False, 0.01, 0.01, 0.2, 'ROBUST',  13.44, 100),
    'EURAUD': (25,  4, 22, True,  0.06, 0.06, 0.2, 'ROBUST',   5.20,  71),
    'CADCHF': (10,  4, 22, True,  0.01, 0.01, 0.2, 'ROBUST',   4.85, 100),
    'GBPCAD': (20,  6, 21, False, 0.02, 0.02, 0.2, 'ROBUST',   3.06,  86),
    # ---- MOYEN (PF>=1.5 & folds>=50%) ----
    'CHFJPY': (40,  0, 24, True,  0.09, 0.09, 0.2, 'MOYEN',    2.78, 100),  # PF test reajuste (test n trop faible)
    'USDCHF': (30,  0, 24, False, 0.11, 0.11, 0.2, 'MOYEN',    2.78, 100),
    'NZDCAD': ( 0,  6, 21, True,  0.09, 0.09, 0.2, 'MOYEN',    1.83, 100),
    'EURCAD': (15,  6, 21, False, 0.09, 0.09, 0.2, 'MOYEN',    1.77, 100),
    # ---- FRAGILE (<35%) -- gardes pour surveillance ----
    'GBPCHF': (30,  4, 22, False, 0.01, 0.01, 0.2, 'FRAGILE',  1.44,  71),
    'NZDCHF': (10,  8, 20, True,  0.01, 0.01, 0.2, 'FRAGILE',  1.28,  83),
    'GBPUSD': (25,  8, 20, True,  0.10, 0.10, 0.2, 'FRAGILE',  2.10, 100),
    'AUDNZD': (35,  8, 20, False, 0.15, 0.15, 0.2, 'FRAGILE',  1.44, 100),
    'NZDJPY': (10,  6, 21, True,  0.01, 0.01, 0.2, 'FRAGILE',  1.28,  57),
    'USDCAD': ( 0,  6, 21, True,  0.03, 0.03, 0.2, 'FRAGILE',  0.91,   0),
    'NZDUSD': ( 0,  8, 20, True,  0.04, 0.04, 0.2, 'FRAGILE',  0.71,   0),
    'EURCHF': (40,  0, 24, True,  0.05, 0.05, 0.2, 'FRAGILE',  0.47,   0),
    'CADJPY': (40,  6, 21, False, 0.06, 0.06, 0.2, 'FRAGILE',  0.46,  14),
    'USDJPY': (10,  6, 21, True,  0.01, 0.01, 0.2, 'FRAGILE',  0.44,  14),
    'EURJPY': (35,  0, 24, False, 0.06, 0.06, 0.2, 'FRAGILE',  0.41,  33),
    'AUDJPY': (10,  4, 22, True,  0.01, 0.01, 0.2, 'FRAGILE',  0.32,  14),
    'AUDCAD': ( 5,  0, 24, False, 0.04, 0.04, 0.2, 'FRAGILE',  0.25,  17),
    'AUDUSD': (10,  8, 20, True,  0.05, 0.05, 0.2, 'FRAGILE',  0.21,  17),
    'GBPJPY': ( 0,  6, 21, True,  0.11, 0.11, 0.2, 'FRAGILE',  0.21,   0),
    'GBPAUD': (10,  0, 24, False, 0.15, 0.15, 0.2, 'FRAGILE',  0.17,   0),
    'AUDCHF': (20,  4, 22, True,  0.01, 0.01, 0.2, 'FRAGILE',  0.11,  14),
    'EURGBP': (25,  8, 20, False, 0.02, 0.02, 0.2, 'FRAGILE',  0.08,   0),
    'EURUSD': ( 5,  0, 24, False, 0.01, 0.01, 0.2, 'FRAGILE',  0.05,  17),
    'EURNZD': (40,  0, 24, False, 0.13, 0.13, 0.2, 'FRAGILE',  0.03,   0),
}

EMA_LENS   = [20, 25, 30, 35, 40, 45, 50, 55]
PYRAMIDING = 5
LOT_SIZE   = 0.1
CONTRACT   = 100_000
JPY_PAIRS  = {'AUDJPY','CADJPY','CHFJPY','EURJPY','GBPJPY','NZDJPY','USDJPY'}
N_CANDLES  = {"60": 1000, "D": 400, "W": 220}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")


# ================================================================
# WEBSOCKET TRADINGVIEW (identique v6)
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
        frames.append(raw[i:i+size]); i += size
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
    h1 = fetch_ohlc(sym, "60", N_CANDLES["60"])
    d1 = fetch_ohlc(sym, "D",  N_CANDLES["D"])
    w1 = fetch_ohlc(sym, "W",  N_CANDLES["W"])
    return h1, d1, w1


# ================================================================
# INDICATEURS (Pine-fideles — identiques v6)
# ================================================================
def ema_pine(s, n):
    result = s.copy().astype(float); alpha = 2.0 / (n + 1)
    valid  = s.dropna()
    if len(valid) < n: return pd.Series(np.nan, index=s.index)
    sl = s.index.get_loc(valid.index[n - 1])
    result.iloc[:sl] = np.nan; result.iloc[sl] = valid.iloc[:n].mean()
    for i in range(sl + 1, len(result)):
        result.iloc[i] = alpha * s.iloc[i] + (1 - alpha) * result.iloc[i - 1] \
                         if not np.isnan(s.iloc[i]) else result.iloc[i - 1]
    return result

def psar_calc(h, l, af0, af, max_af):
    r   = pta.psar(h, l, af0=af0, af=af, max_af=max_af)
    cl  = [c for c in r.columns if 'PSARl' in c][0]
    cs  = [c for c in r.columns if 'PSARs' in c][0]
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
# BUILD DF COMPLET (logique v7 = v6 avec params Optuna)
# ================================================================
def build_df(pair, h1, d1, w1, af0, af, maf):
    if h1 is None or d1 is None or w1 is None:
        return None

    for n in EMA_LENS:
        d1[f'e{n}'] = ema_pine(d1['Close'], n)
        w1[f'e{n}'] = ema_pine(w1['Close'], n)

    d1['adx']      = adx14(d1['High'], d1['Low'], d1['Close'])
    d1['ema_bull'] = np.logical_and.reduce(
        [d1[f'e{EMA_LENS[i]}'] > d1[f'e{EMA_LENS[i+1]}'] for i in range(7)])
    d1['ema_bear'] = np.logical_and.reduce(
        [d1[f'e{EMA_LENS[i]}'] < d1[f'e{EMA_LENS[i+1]}'] for i in range(7)])

    w1['w_above'] = np.logical_and.reduce(
        [w1['Close'] > w1[f'e{n}'] for n in EMA_LENS])
    w1['w_below'] = np.logical_and.reduce(
        [w1['Close'] < w1[f'e{n}'] for n in EMA_LENS])
    w1['w_close'] = w1['Close']

    d1['last_bear_open'] = d1['Open'].where(d1['Close'] < d1['Open']).ffill()
    d1['last_bull_open'] = d1['Open'].where(d1['Close'] > d1['Open']).ffill()

    DC = ['ema_bull', 'ema_bear', 'adx', 'last_bear_open', 'last_bull_open']
    WC = ['w_above', 'w_below', 'w_close']
    ds = d1[DC].shift(1).rename(columns={c: 'd_'+c for c in DC})
    ws = w1[WC].shift(1).rename(columns={c: 'w_'+c for c in WC})

    df = h1.copy()
    df = pd.merge_asof(ri(df), ri(ds), on='Datetime', direction='backward').set_index('Datetime')
    df = pd.merge_asof(ri(df), ri(ws), on='Datetime', direction='backward').set_index('Datetime')
    df['hour']  = df.index.hour
    df['month'] = df.index.month
    df['day']   = df.index.day

    # SAR Daily + Weekly (fix v6/v7 : dans le biais)
    dsar = psar_calc(d1['High'], d1['Low'], af0, af, maf).rename('d_sar').shift(1)
    wsar = psar_calc(w1['High'], w1['Low'], af0, af, maf).rename('w_sar').shift(1)
    df = pd.merge_asof(ri(df),
                       ri(dsar.reset_index().rename(columns={'index':'Datetime','Datetime':'Datetime'})),
                       on='Datetime', direction='backward').set_index('Datetime')
    df = pd.merge_asof(ri(df),
                       ri(wsar.reset_index().rename(columns={'index':'Datetime','Datetime':'Datetime'})),
                       on='Datetime', direction='backward').set_index('Datetime')

    wa = df['w_w_above'].fillna(False) if 'w_w_above' in df.columns else df['w_above'].fillna(False)
    wb = df['w_w_below'].fillna(False) if 'w_w_below' in df.columns else df['w_below'].fillna(False)
    wc = df['w_w_close']               if 'w_w_close' in df.columns else df['w_close']

    df['bias_bull'] = (df['d_ema_bull'].fillna(False) & wa &
                       (wc > df['w_sar']).fillna(False))
    df['bias_bear'] = (df['d_ema_bear'].fillna(False) & wb &
                       (wc < df['w_sar']).fillna(False))
    df['ref_level'] = np.where(df['bias_bull'], df['d_last_bear_open'],
                      np.where(df['bias_bear'], df['d_last_bull_open'], np.nan))

    df['sar']      = psar_calc(h1['High'], h1['Low'], af0, af, maf)
    df['bull']     = xo(df['Close'], df['sar'])
    df['bear']     = xu(df['Close'], df['sar'])
    df['bull_vw0'] = vwhen(df['bull'],  df['sar'])
    df['bear_vw0'] = vwhen(df['bear'],  df['sar'])
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
    hok = (~(((df['month']==12) & (df['day']>=20)) |
             ((df['month']==1)  & (df['day']<=5)))).values if hol else np.ones(len(df), bool)
    FLT = aok & sok & hok

    N=len(df); C=df['Close'].values.astype(float); SAR=df['sar'].values.astype(float)
    BUL=df['bull'].values.astype(bool); BEA=df['bear'].values.astype(bool)
    BV=df['bull_vw0'].values.astype(float); BRV=df['bear_vw0'].values.astype(float)
    BB=df['bias_bull'].values.astype(bool); BBR=df['bias_bear'].values.astype(bool)
    RL=df['ref_level'].values.astype(float)
    CAB=df['cab'].values.astype(bool); CBB=df['cbb'].values.astype(bool)

    pos=[]; trades=[]; lt=st=False; lvls=lvss=np.nan
    plr=psr_=np.nan; nla=nsa=False; prl=np.nan
    lsb=None; lst=None

    nl = lambda: sum(1 for p in pos if p[0]=='L')
    ns = lambda: sum(1 for p in pos if p[0]=='S')

    def ca(side, ep, bar):
        for p in list(pos):
            if p[0] == side:
                d = 1 if side=='L' else -1
                trades.append((ep - p[1])*d*LOT_SIZE*CONTRACT - sp*LOT_SIZE*CONTRACT > 0)
                pos.remove(p)

    for i in range(1, N):
        c=C[i]; s=SAR[i]; r=RL[i]; bb=BB[i]; bbr=BBR[i]; f=FLT[i]
        bv=BV[i]; brv=BRV[i]; _nl=nl(); _ns=ns()
        rn=np.isnan(r); pn=np.isnan(prl)
        rc=(rn!=pn) or (not rn and not pn and r!=prl); prl=r
        if rc: lvls=lvss=np.nan
        if rc or not bb:  plr=np.nan;  nla=False
        if rc or not bbr: psr_=np.nan; nsa=False

        lsig=(bb  and BUL[i] and not rn and c>r and s>r and (np.isnan(lvls) or s<lvls))
        ssig=(bbr and BEA[i] and not rn and c<r and s<r and (np.isnan(lvss) or s>lvss))
        if lsig: lvls=s
        if ssig: lvss=s
        if _nl>0 and lsig: lt=True
        if _ns>0 and ssig: st=True

        elt=(C[i]<BV[i])  and (C[i-1]>=BV[i-1])  if not np.isnan(BV[i])  and not np.isnan(BV[i-1])  else False
        est=(C[i]>BRV[i]) and (C[i-1]<=BRV[i-1]) if not np.isnan(BRV[i]) and not np.isnan(BRV[i-1]) else False
        if _nl>0 and lt and elt: ca('L',c,i); lt=False; _nl=nl()
        if _ns>0 and st and est: ca('S',c,i); st=False; _ns=ns()

        plb=(f and bb  and not rn and not np.isnan(brv) and CAB[i] and brv<r)
        psb=(f and bbr and not rn and not np.isnan(bv)  and CBB[i] and bv>r)
        if plb:  plr=bv;   nla=False
        elif not np.isnan(plr) and c<plr: nla=True
        if psb:  psr_=brv; nsa=False
        elif not np.isnan(psr_) and c>psr_: nsa=True

        nls=(f and bb  and nla and not rn and not np.isnan(plr)  and not np.isnan(brv) and CAB[i] and brv<r and brv<plr)
        nss=(f and bbr and nsa and not rn and not np.isnan(psr_) and not np.isnan(bv)  and CBB[i] and bv>r  and bv>psr_)
        if nls: plr=bv;   nla=False
        if nss: psr_=brv; nsa=False

        if (plb or nls) and _nl+1<=PYRAMIDING and _ns==0:
            pos.append(('L', c+sp/2, i)); st=False; lsb=i; lst='LONG'
        if (psb or nss) and _ns+1<=PYRAMIDING and _nl==0:
            pos.append(('S', c-sp/2, i)); lt=False; lsb=i; lst='SHORT'
        if nl()==0: lt=False
        if ns()==0: st=False

    last = N - 1
    return {
        'pos': pos, 'lt': lt, 'st': st, 'plr': plr, 'psr': psr_,
        'nla': nla, 'nsa': nsa,
        'bb': BB[last], 'bbr': BBR[last], 'rl': RL[last],
        'sar': SAR[last], 'c': C[last], 'bv': BV[last], 'brv': BRV[last],
        'aok': aok[last], 'sok': sok[last], 'hok': hok[last], 'fok': FLT[last],
        'ts': df.index[last], 'trades': trades,
        'lsb': lsb, 'lst': lst, 'idx': df.index,
    }


# ================================================================
# ANALYSE COMPLETE D'UNE PAIRE
# ================================================================
def fp(p, pair):
    if np.isnan(p): return "n/a"
    return f"{p:.3f}" if pair in JPY_PAIRS else f"{p:.5f}"

def analyse(pair):
    cfg   = PAIRS_CONFIG[pair]
    adx, ss, se, hol, af0, af, maf, label, pf_bt, stab = cfg

    h1, d1, w1 = fetch_pair(pair)
    if h1 is None or d1 is None or w1 is None:
        return {'pair': pair, 'error': True, 'label': label, 'pf_bt': pf_bt, 'stab': stab}

    df = build_df(pair, h1, d1, w1, af0, af, maf)
    if df is None:
        return {'pair': pair, 'error': True, 'label': label, 'pf_bt': pf_bt, 'stab': stab}

    st  = simulate(df, pair, adx, ss, se, hol)
    th  = st['trades']
    nt  = len(th); wr = sum(th)/nt*100 if nt else 0.0

    longs  = [p for p in st['pos'] if p[0]=='L']
    shorts = [p for p in st['pos'] if p[0]=='S']

    sig = 'AUCUN'
    if st['lsb'] == len(df) - 1:
        sig = f"ENTREE {st['lst']}"

    watch = []
    bv=st['bv']; brv=st['brv']; r=st['rl']
    if st['bb'] and not np.isnan(r):
        if not np.isnan(brv) and brv < r:
            watch.append(f"Pre_LONG si close > bear_vw0 ({fp(brv,pair)})")
        if st['nla'] and not np.isnan(st['plr']):
            watch.append(f"Neo_LONG arme (ref={fp(st['plr'],pair)})")
        if longs and st['lt']:
            watch.append(f"Trailing LONG -- exit si close < bull_vw0 ({fp(bv,pair)})")
    if st['bbr'] and not np.isnan(r):
        if not np.isnan(bv) and bv > r:
            watch.append(f"Pre_SHORT si close < bull_vw0 ({fp(bv,pair)})")
        if st['nsa'] and not np.isnan(st['psr']):
            watch.append(f"Neo_SHORT arme (ref={fp(st['psr'],pair)})")
        if shorts and st['st']:
            watch.append(f"Trailing SHORT -- exit si close > bear_vw0 ({fp(brv,pair)})")

    return {
        'pair':   pair, 'label': label, 'pf_bt': pf_bt, 'stab': stab,
        'biais':  'BULL' if st['bb'] else ('BEAR' if st['bbr'] else 'NEUTRE'),
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
        'chg_d': (d1['Close'].iloc[-1] / d1['Close'].iloc[-2] - 1) * 100 if len(d1) >= 2 else float('nan'),
        'error': False,
    }


# ================================================================
# AFFICHAGE
# ================================================================
BIAIS_SYM = {'BULL': 'BULL ^', 'BEAR': 'BEAR v', 'NEUTRE': '  --  '}

def print_summary(results):
    now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*100}")
    print(f"  SIGNAL v7 LIVE -- 28 paires -- {now} Paris")
    print(f"  Optimisation : Bayesian Optuna | 4 folds WF | Stabilite parametrique")
    print(f"{'='*100}")

    label_order = {'ROBUST': 0, 'MOYEN': 1, 'FRAGILE': 2}
    def sort_key(r):
        has_sig = 0 if r.get('sig') != 'AUCUN' else 1
        has_pos = 0 if (r.get('longs') or r.get('shorts')) else 1
        b = 0 if r.get('biais') in ('BULL', 'BEAR') else 1
        return (label_order.get(r.get('label','FRAGILE'), 2), has_sig, has_pos, b, r.get('pair',''))
    results = sorted(results, key=sort_key)

    hdr = (f"  {'Paire':<8} {'PF':>6} {'Stab':>5} {'Label':<8} {'Biais':<8} "
           f"{'Filtre':<7} {'Close':>10} {'SAR 1H':>10} {'Ref':>10} "
           f"{'Pos':>6} {'WR%':>6} {'Signal / A surveiller'}")
    print(hdr)
    print("  " + "-"*98)

    current_label = None
    for r in results:
        if r.get('error'):
            print(f"  {r['pair']:<8}  {'?':>6}   {'?':>4}  {r['label']:<8}  ERREUR (donnees manquantes)")
            continue
        if r['label'] != current_label:
            print(f"\n  --- {r['label']} ---")
            current_label = r['label']

        b   = BIAIS_SYM.get(r['biais'], '  --  ')
        flt = ("OK     " if r['fok'] else
               ("ADX    " if not r['aok'] else
                ("SESSION" if not r['sok'] else "HOLIDAY")))
        pos = ""
        if r['longs']:  pos += f"+{len(r['longs'])}L"
        if r['shorts']: pos += f"-{len(r['shorts'])}S"
        if not pos:     pos = " none"
        trl = "[TRL]" if ((r['longs'] and r['lt']) or (r['shorts'] and r['st_arm'])) else ""
        sig_w = r['sig'] if r['sig'] != 'AUCUN' else (r['watch'][0][:38] if r['watch'] else '-')

        # PF backtest coloré par label
        pf_s  = f"{r['pf_bt']:>5.2f}"
        stab_s = f"{r['stab']:>3}%"

        print(f"  {r['pair']:<8} {pf_s} {stab_s}  {r['label']:<8} {b:<8} {flt:<7}"
              f" {fp(r['close'],r['pair']):>10} {fp(r['sar'],r['pair']):>10}"
              f" {fp(r['ref'],r['pair']):>10} {pos:>6}{trl:<5}  {r['wr']:>4.0f}%  {sig_w}")

    print("  " + "-"*98)


def print_active_signals(results):
    actives = [r for r in results
               if not r.get('error') and (r['sig'] != 'AUCUN' or r['longs'] or r['shorts'])]
    if not actives:
        print("\n  Aucune position ouverte ni signal d'entree en ce moment.")
        return

    print(f"\n{'='*100}")
    print(f"  DETAIL -- POSITIONS OUVERTES & SIGNAUX")
    print(f"{'='*100}")
    for r in actives:
        pair = r['pair']
        b    = BIAIS_SYM.get(r['biais'])
        print(f"\n  {pair} [{r['label']} PF={r['pf_bt']:.2f} Stab={r['stab']}%]"
              f"  Biais:{b}  Close:{fp(r['close'],pair)}  SAR:{fp(r['sar'],pair)}")
        c = r['close']
        for p in r['longs']:
            ts  = r['idx'][p[2]]
            pnl = (c - p[1]) * LOT_SIZE * CONTRACT
            trl = " [TRAILING ACTIF]" if r['lt'] else ""
            print(f"    LONG  {str(ts)[:16]}  entree={fp(p[1],pair)}  PnL~{pnl:+.0f} pts{trl}")
        for p in r['shorts']:
            ts  = r['idx'][p[2]]
            pnl = (p[1] - c) * LOT_SIZE * CONTRACT
            trl = " [TRAILING ACTIF]" if r['st_arm'] else ""
            print(f"    SHORT {str(ts)[:16]}  entree={fp(p[1],pair)}  PnL~{pnl:+.0f} pts{trl}")
        if r['sig'] != 'AUCUN':
            print(f"    *** {r['sig']} ***")
        for w in r['watch']:
            print(f"    -> {w}")
        if r['nla']: print(f"    Neo_LONG arme  (plr={fp(r['plr'],pair)})")
        if r['nsa']: print(f"    Neo_SHORT arme (psr={fp(r['psr'],pair)})")


# ================================================================
# TELEGRAM
# ================================================================
def _pair_dots(r):
    """
    1ère boule = tendance  (🔴 BEAR, 🟢 BULL, ⚪ neutre)
    2ème boule = GO si même couleur, WAIT si couleur inverse
    GO  = filtre OK ET signal actif (entree ou trailing)
    WAIT = filtre bloquant OU pas de signal
    """
    biais = r.get('biais', '')
    if 'BEAR' in biais:
        dot1 = '🔴'
        dot2_go = '🔴'
        dot2_wait = '🟢'
    elif 'BULL' in biais:
        dot1 = '🟢'
        dot2_go = '🟢'
        dot2_wait = '🔴'
    else:
        return '⚪⚪'

    fok = r.get('fok', '') == 'OK'
    has_signal = r['sig'] != 'AUCUN'
    is_trailing = (r['longs'] and r['lt']) or (r['shorts'] and r['st_arm'])
    go = fok and (has_signal or is_trailing)

    dot2 = dot2_go if go else dot2_wait
    return f"{dot1}{dot2}"


def telegram_text(results):
    now  = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines = ["SIGNAL V7", ""]

    for label in ("ROBUST", "MOYEN"):
        group = [r for r in results if not r.get('error') and r['label'] == label
                 and r.get('biais', '--') not in ('--', 'NEUTRE', '')]
        if not group:
            continue
        lines.append(label)
        for r in group:
            dots = _pair_dots(r)
            chg  = r.get('chg_d', float('nan'))
            chg_s = f"({chg:+.2f}%)".replace('.', ',') if not (chg != chg) else ""
            lines.append(f"{dots} {r['pair']} {chg_s}")
        lines.append("")

    lines.append(f"⏰ {now} Paris")
    return "\n".join(lines)

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
        print("  Telegram: envoye")
    except Exception as e:
        print(f"  Telegram: erreur ({e})")


# ================================================================
# EXPORT CSV
# ================================================================
def export_csv(results, fname='signal_v7_current.csv'):
    rows = []
    for r in results:
        if r.get('error'):
            rows.append({'pair': r['pair'], 'label': r['label'], 'error': True})
            continue
        rows.append({
            'pair':        r['pair'],
            'label':       r['label'],
            'pf_backtest': r['pf_bt'],
            'stab_pct':    r['stab'],
            'timestamp':   str(r['ts'])[:16],
            'biais':       r['biais'],
            'filters_ok':  r['fok'],
            'close':       round(r['close'], 5),
            'sar':         round(r['sar'], 5) if not np.isnan(r['sar']) else '',
            'ref_level':   round(r['ref'], 5)  if not np.isnan(r['ref'])  else '',
            'longs':       len(r['longs']),
            'shorts':      len(r['shorts']),
            'trailing':    int(bool((r['longs'] and r['lt']) or (r['shorts'] and r['st_arm']))),
            'signal':      r['sig'],
            'wr_hist_pct': round(r['wr'], 1),
            'n_trades':    r['nt'],
        })
    pd.DataFrame(rows).to_csv(fname, index=False)
    return fname


# ================================================================
# SCAN PRINCIPAL
# ================================================================
def run_scan(target_pairs):
    t0  = time.time()
    now = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n  Scan {len(target_pairs)} paires -- {now}")
    print(f"  Source: TradingView WebSocket (OANDA)\n")

    results = []
    for i, pair in enumerate(target_pairs, 1):
        cfg   = PAIRS_CONFIG[pair]
        label = cfg[7]
        print(f"  [{i:>2}/{len(target_pairs)}] {pair} [{label}]...", end=' ', flush=True)
        try:
            r = analyse(pair)
            if r.get('error'):
                print("ERREUR")
            else:
                biais = r['biais']
                pos_s = (f"+{len(r['longs'])}L" if r['longs'] else
                         f"-{len(r['shorts'])}S" if r['shorts'] else "flat")
                sig_s = f" *** {r['sig']} ***" if r['sig'] != 'AUCUN' else ""
                print(f"OK  {biais:<6}  {pos_s}{sig_s}")
            results.append(r)
        except Exception as e:
            print(f"ERREUR ({e})")
            cfg = PAIRS_CONFIG.get(pair, (0,0,24,False,0.02,0.02,0.2,'FRAGILE',0,0))
            results.append({'pair': pair, 'error': True, 'label': cfg[7],
                            'pf_bt': cfg[8], 'stab': cfg[9]})

    print_summary(results)
    print_active_signals(results)

    csv_f = export_csv(results)
    tg    = telegram_text(results)
    send_telegram(tg)

    elapsed = time.time() - t0
    print(f"\n  -> {csv_f}")
    print(f"  Temps: {elapsed:.1f}s  ({elapsed/len(target_pairs):.1f}s/paire)")
    print(f"{'='*100}\n")
    return results


# ================================================================
# MAIN
# ================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Signal v7 -- 28 paires TradingView WebSocket (Optuna + 4 folds)")
    parser.add_argument('--pair',     default=None,
                        help='Paire unique (ex: GBPNZD)')
    parser.add_argument('--filter',   default=None,
                        help='Filtrer par label: ROBUST / MOYEN / FRAGILE')
    parser.add_argument('--loop',     action='store_true',
                        help='Mode boucle (scan a intervalle regulier)')
    parser.add_argument('--interval', type=int, default=3600,
                        help='Intervalle boucle en secondes (defaut: 3600)')
    args = parser.parse_args()

    if args.pair:
        p = args.pair.upper()
        if p not in PAIRS_CONFIG:
            print(f"Paire {p} inconnue. Disponibles: {', '.join(PAIRS_CONFIG)}")
            return
        target = [p]
    elif args.filter:
        lbl = args.filter.upper()
        target = [p for p, cfg in PAIRS_CONFIG.items() if cfg[7] == lbl]
        if not target:
            print(f"Aucune paire avec label '{lbl}'")
            return
        print(f"Filtre '{lbl}' -> {len(target)} paires: {', '.join(target)}")
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
