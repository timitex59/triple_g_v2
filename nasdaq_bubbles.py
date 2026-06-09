#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nasdaq_bubbles.py

Visualisation "bubbles" facon cryptobubbles.net / finviz bubbles / bubblescreener
mais pour le Nasdaq-100. Deux modes:
  - ACTIONS : 1 bulle par action (les ~100 du Nasdaq-100), comme cryptobubbles
  - THEMES  : 1 bulle par categorie/secteur, clic = drill-down dans ses actions

Fonctionnalites (inspirees des references, ameliorees):
  - logos des societes dans les bulles (financialmodelingprep, sans cle)
  - taille reglable: Market Cap / |Performance| / Score composite
  - horizons 5J / 21J / 63J / 126J / YTD
  - panneau tableau trie + filtres Tous / Gainers / Losers + filtre secteur
  - bandeau news live (issues du pipeline)
  - halo "trend Renko valide", recherche, schemas de couleur
  - physique de bulles maison (canvas, zero dependance front)

Source: nasdaq_sector_pipeline_report.json (produit par nasdaq_sector_pipeline.py).
Par defaut AUCUN appel reseau cote generateur: il lit le rapport (+ un sidecar
optionnel de market caps) et ecrit un HTML autonome.

  python nasdaq_bubbles.py            # genere le HTML depuis le rapport
  python nasdaq_bubbles.py --enrich   # recupere d'abord market cap + volume
                                      # (yfinance) dans nasdaq_market_caps.json

Le script ne donne pas de conseil financier.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from statistics import median
from typing import Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_JSON = os.path.join(SCRIPT_DIR, "nasdaq_sector_pipeline_report.json")
MCAP_SIDECAR = os.path.join(SCRIPT_DIR, "nasdaq_market_caps.json")
OUTPUT_HTML = os.path.join(SCRIPT_DIR, "nasdaq_bubbles.html")

TIMEFRAMES = [
    ("perf_5d", "5J"),
    ("perf_21d", "21J"),
    ("perf_63d", "63J"),
    ("perf_126d", "126J"),
    ("perf_ytd", "YTD"),
]

THEME_SHORT = {
    "Semiconductors / AI infrastructure": "Semis / AI",
    "AI platforms / Mega-cap tech": "AI Mega-cap",
    "Cloud / Data software": "Cloud / SaaS",
    "Consumer internet / E-commerce": "Internet / E-com",
    "Healthcare / Biotech": "Health / Bio",
    "Industrials / Automation": "Industrials",
    "Utilities / Power": "Utilities",
    "Cybersecurity": "Cybersecurity",
    "Communication Services": "Comm. Services",
    "Consumer Defensive": "Cons. Defensive",
    "Consumer Cyclical": "Cons. Cyclical",
    "Basic Materials": "Materials",
    "Energy": "Energy",
}


def _median_or_none(values: list[float | None]) -> float | None:
    clean = [v for v in values if v is not None]
    return float(median(clean)) if clean else None


# --------------------------------------------------------------------------- #
# Enrichissement market cap / volume (optionnel, --enrich)                     #
# --------------------------------------------------------------------------- #
def enrich_market_caps(tickers: list[str], max_workers: int = 12) -> dict[str, dict[str, float]]:
    """Recupere market cap + dernier volume via yfinance fast_info. Ecrit le
    resultat dans nasdaq_market_caps.json pour que la generation reste offline."""
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def one(tk: str) -> tuple[str, dict[str, float]]:
        try:
            fi = yf.Ticker(tk).fast_info
            mc = fi["marketCap"]
            vol = fi["lastVolume"]
            out: dict[str, float] = {}
            if mc:
                out["market_cap"] = float(mc)
            if vol:
                out["volume"] = float(vol)
            return tk, out
        except Exception:
            return tk, {}

    result: dict[str, dict[str, float]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(one, t) for t in tickers]
        for i, fut in enumerate(as_completed(futures), 1):
            tk, data = fut.result()
            if data:
                result[tk] = data
            if i % 20 == 0:
                print(f"  ...{i}/{len(tickers)} tickers", file=sys.stderr)

    with open(MCAP_SIDECAR, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Sidecar market caps -> {MCAP_SIDECAR} ({len(result)} tickers)")
    return result


def load_mcap_sidecar() -> dict[str, dict[str, float]]:
    if os.path.exists(MCAP_SIDECAR):
        try:
            with open(MCAP_SIDECAR, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


# --------------------------------------------------------------------------- #
# Construction du payload                                                      #
# --------------------------------------------------------------------------- #
def build_payload(report: dict[str, Any], mcaps: dict[str, dict[str, float]]) -> dict[str, Any]:
    themes_in = report.get("themes", [])
    stocks_in = report.get("stocks", [])
    renko_in = report.get("renko", {})
    news_in = report.get("news", {})

    validated = {
        key.split(":", 1)[1]
        for key, v in renko_in.items()
        if v.get("final_action") == "VALIDATED"
    }

    tf_keys = [k for k, _ in TIMEFRAMES]

    def mcap_of(stock: dict[str, Any]) -> float | None:
        t = stock.get("ticker", "")
        side = mcaps.get(t, {})
        return side.get("market_cap") or stock.get("market_cap")

    # Liste plate des actions (vue ACTIONS).
    stocks_out: list[dict[str, Any]] = []
    stocks_by_theme: dict[str, list[str]] = {}
    for s in stocks_in:
        ticker = s.get("ticker", "")
        theme = s.get("theme") or "Other"
        stocks_by_theme.setdefault(theme, []).append(ticker)
        stocks_out.append({
            "ticker": ticker,
            "name": s.get("name", ticker),
            "theme": theme,
            "short_theme": THEME_SHORT.get(theme, theme),
            "sector": s.get("sector", ""),
            "score": s.get("score"),
            "rsi": s.get("rsi14"),
            "rel63": s.get("rel_63d_vs_qqq"),
            "above50": bool(s.get("above_sma50")),
            "validated": ticker in validated,
            "mcap": mcap_of(s),
            "close": s.get("close"),
            "perf": {k: s.get(k) for k in tf_keys},
        })

    # Bulles de theme (vue THEMES).
    themes_out: list[dict[str, Any]] = []
    stock_by_ticker = {s["ticker"]: s for s in stocks_out}
    for t in themes_in:
        name = t.get("theme", "")
        kids = stocks_by_theme.get(name, [])
        perf = {
            k: _median_or_none([stock_by_ticker[tk]["perf"][k] for tk in kids if tk in stock_by_ticker])
            for k in tf_keys
        }
        news = news_in.get(name, {})
        themes_out.append({
            "theme": name,
            "short": THEME_SHORT.get(name, name),
            "score": t.get("final_score"),
            "stocks": t.get("stocks", len(kids)),
            "above50": t.get("pct_above_sma50"),
            "above200": t.get("pct_above_sma200"),
            "rel63": t.get("median_rel_63d"),
            "news_hits": news.get("hits", t.get("news_hits", 0)),
            "news_sources": news.get("sources", t.get("news_sources", 0)),
            "perf": perf,
            "children": sorted(
                kids, key=lambda tk: (stock_by_ticker.get(tk, {}).get("score") or 0), reverse=True
            ),
        })

    # News aplaties, dedupliquees, recentes d'abord.
    news_out: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for theme, blob in news_in.items():
        for e in blob.get("entries", []):
            title = e.get("title", "").strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            news_out.append({
                "theme": THEME_SHORT.get(theme, theme),
                "title": title,
                "source": e.get("source", ""),
                "published": e.get("published", ""),
                "link": e.get("link", ""),
            })
    news_out.sort(key=lambda x: x.get("published") or "", reverse=True)
    news_out = news_out[:40]

    sectors = sorted({s["theme"] for s in stocks_out})

    return {
        "generated_at": report.get("generated_at"),
        "runtime_seconds": report.get("runtime_seconds"),
        "timeframes": [{"key": k, "label": lbl} for k, lbl in TIMEFRAMES],
        "has_mcap": any(s["mcap"] for s in stocks_out),
        "sectors": sectors,
        "stocks": stocks_out,
        "themes": themes_out,
        "news": news_out,
    }


def render_html(payload: dict[str, Any]) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return HTML_TEMPLATE.replace("/*__PAYLOAD__*/null", data_json)


# --------------------------------------------------------------------------- #
# Template HTML                                                                #
# --------------------------------------------------------------------------- #
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no" />
<title>NASDAQ-100 Bubbles</title>
<style>
  :root{
    --bg:#05070d; --panel:rgba(13,19,33,.92); --line:rgba(120,150,200,.16);
    --txt:#e7edf7; --muted:#8aa0c0; --accent:#38e1ff;
  }
  *{box-sizing:border-box}
  html,body{margin:0;height:100%;overflow:hidden;background:var(--bg);
    font-family:"Segoe UI",Roboto,system-ui,sans-serif;color:var(--txt)}
  #app{position:fixed;inset:0}
  canvas{display:block;width:100%;height:100%}

  header{position:fixed;top:0;left:0;right:0;z-index:10;display:flex;align-items:center;
    gap:10px;flex-wrap:wrap;padding:9px 14px;
    background:linear-gradient(180deg,rgba(5,7,13,.96) 0%,rgba(5,7,13,.55) 75%,rgba(5,7,13,0) 100%)}
  .brand{display:flex;align-items:baseline;gap:9px}
  .brand b{font-size:18px;letter-spacing:.5px}
  .brand .sub{font-size:11px;color:var(--muted)}
  .spacer{flex:1 1 auto}
  .seg{display:inline-flex;background:rgba(20,28,46,.82);border:1px solid var(--line);
    border-radius:9px;overflow:hidden}
  .seg button{all:unset;padding:6px 11px;font-size:12px;color:var(--muted);cursor:pointer;transition:.15s}
  .seg button:hover{color:var(--txt)}
  .seg button.on{background:var(--accent);color:#02121a;font-weight:700}
  .lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-right:2px}
  input.search{background:rgba(20,28,46,.82);border:1px solid var(--line);border-radius:9px;
    color:var(--txt);padding:6px 10px;font-size:12px;width:130px;outline:none}
  input.search:focus{border-color:var(--accent)}
  select.scheme{background:rgba(20,28,46,.82);border:1px solid var(--line);border-radius:9px;
    color:var(--txt);padding:6px 8px;font-size:12px;outline:none}

  .crumb{position:fixed;top:88px;left:14px;z-index:10;display:none;align-items:center;gap:8px;
    font-size:13px;color:var(--muted)}
  .crumb button{all:unset;cursor:pointer;color:var(--accent);font-weight:600;padding:4px 10px;
    border:1px solid var(--line);border-radius:8px}
  .crumb button:hover{background:rgba(56,225,255,.12)}
  .crumb .here{color:var(--txt);font-weight:600}

  /* panneau tableau */
  #panel{position:fixed;top:46px;right:0;bottom:30px;width:300px;z-index:9;display:flex;flex-direction:column;
    background:var(--panel);border-left:1px solid var(--line);backdrop-filter:blur(8px);
    transform:translateX(100%);transition:transform .25s}
  #panel.open{transform:translateX(0)}
  #panel .ph{display:flex;align-items:center;gap:6px;padding:8px 10px;border-bottom:1px solid var(--line);flex-wrap:wrap}
  .chip{all:unset;cursor:pointer;font-size:11px;padding:4px 9px;border-radius:7px;color:var(--muted);
    border:1px solid var(--line)}
  .chip.on{background:var(--accent);color:#02121a;font-weight:700;border-color:var(--accent)}
  #panel select{background:rgba(20,28,46,.9);border:1px solid var(--line);color:var(--txt);
    border-radius:7px;font-size:11px;padding:4px 6px;outline:none;flex:1 1 100%}
  #rows{overflow-y:auto;flex:1 1 auto}
  .r{display:grid;grid-template-columns:22px 1fr auto;gap:8px;align-items:center;padding:6px 10px;
    font-size:12px;cursor:pointer;border-bottom:1px solid rgba(120,150,200,.06)}
  .r:hover{background:rgba(56,225,255,.08)}
  .r img{width:18px;height:18px;border-radius:4px;background:#fff2;object-fit:contain}
  .r .tk{font-weight:600}
  .r .nm{font-size:10px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .r .pf{font-variant-numeric:tabular-nums;font-weight:600;text-align:right}
  #panelToggle{all:unset;position:fixed;top:54px;right:14px;z-index:11;cursor:pointer;font-size:12px;
    color:var(--muted);border:1px solid var(--line);border-radius:8px;padding:5px 10px;background:rgba(20,28,46,.82)}
  #panelToggle:hover{color:var(--txt)}

  .legend{position:fixed;bottom:34px;left:14px;z-index:10;font-size:11px;color:var(--muted);
    display:flex;align-items:center;gap:8px;background:rgba(10,14,24,.6);padding:5px 9px;border-radius:8px;
    border:1px solid var(--line)}
  .legend .bar{width:96px;height:8px;border-radius:4px;background:linear-gradient(90deg,#ff3b4e,#6b7280,#1fd17a)}

  /* bandeau news */
  #news{position:fixed;bottom:0;left:0;right:0;height:28px;z-index:10;display:flex;align-items:center;
    gap:8px;background:rgba(8,11,20,.92);border-top:1px solid var(--line);overflow:hidden;font-size:12px}
  #news .tag{flex:0 0 auto;background:#ff3b4e;color:#fff;font-weight:700;font-size:10px;padding:3px 8px;
    letter-spacing:.5px;height:100%;display:flex;align-items:center}
  #newsTrack{display:inline-flex;gap:40px;white-space:nowrap;will-change:transform;padding-left:100%;
    animation:marq 220s linear infinite}
  #news:hover #newsTrack{animation-play-state:paused}
  #newsTrack a{color:var(--txt);text-decoration:none}
  #newsTrack a:hover{color:var(--accent);text-decoration:underline}
  #newsTrack .src{color:var(--muted)}
  #news .pausehint{flex:0 0 auto;margin-left:auto;padding-right:10px;color:var(--muted);font-size:10px}
  @keyframes marq{from{transform:translateX(0)}to{transform:translateX(-100%)}}

  #tip{position:fixed;z-index:20;pointer-events:none;display:none;max-width:280px;background:var(--panel);
    border:1px solid var(--line);border-radius:12px;padding:10px 12px;font-size:12px;line-height:1.5;
    backdrop-filter:blur(8px);box-shadow:0 10px 40px rgba(0,0,0,.5)}
  #tip h4{margin:0 0 4px;font-size:13px}
  #tip .row{display:flex;justify-content:space-between;gap:16px;color:var(--muted)}
  #tip .row b{color:var(--txt);font-weight:600}
  #tip .tag{display:inline-block;margin-top:6px;padding:2px 7px;border-radius:6px;font-size:10px;
    font-weight:700;background:rgba(31,209,122,.18);color:#4cf0a0}
  .pos{color:#1fd17a}.neg{color:#ff5a6a}

  /* toolbar (boutons icones) */
  .tbtn{all:unset;cursor:pointer;font-size:12px;color:var(--muted);border:1px solid var(--line);
    border-radius:9px;padding:6px 9px;background:rgba(20,28,46,.82);transition:.15s}
  .tbtn:hover{color:var(--txt);border-color:var(--accent)}
  .tbtn.on{background:var(--accent);color:#02121a;font-weight:700}
  .tbtn.pro{color:#ffd86b;border-color:rgba(255,216,107,.4)}
  .tbtn.pro:hover{background:rgba(255,216,107,.14)}
  .alertbox{display:inline-flex;align-items:center;gap:5px;background:rgba(20,28,46,.82);
    border:1px solid var(--line);border-radius:9px;padding:3px 7px}
  .alertbox input{width:46px;background:transparent;border:none;color:var(--txt);font-size:12px;outline:none}
  .alertbox .lk{font-size:10px;color:#ffd86b}

  /* etoile watchlist dans le tableau */
  .r .star{cursor:pointer;color:#3a4660;font-size:14px;text-align:center}
  .r .star.on{color:#ffd86b}
  .r{grid-template-columns:16px 20px 1fr auto}

  /* HUD breadth */
  #breadth{position:fixed;top:54px;left:14px;z-index:10;display:flex;gap:10px;align-items:center;
    background:rgba(10,14,24,.66);border:1px solid var(--line);border-radius:9px;padding:5px 10px;font-size:11px}
  #breadth b{font-variant-numeric:tabular-nums}
  #breadth .up{color:#1fd17a}#breadth .dn{color:#ff5a6a}#breadth .sep{color:var(--line)}

  /* modal Pro */
  .modal{position:fixed;inset:0;z-index:40;display:none;align-items:center;justify-content:center;
    background:rgba(2,5,12,.72);backdrop-filter:blur(4px)}
  .modal.open{display:flex}
  .modal .card{width:min(820px,92vw);background:linear-gradient(160deg,#0c1426,#0a1020);
    border:1px solid var(--line);border-radius:18px;padding:24px 26px;box-shadow:0 30px 90px rgba(0,0,0,.6)}
  .modal h2{margin:0 0 4px;font-size:22px}
  .modal .lead{color:var(--muted);font-size:13px;margin-bottom:18px}
  .tiers{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
  .tier{border:1px solid var(--line);border-radius:14px;padding:18px;display:flex;flex-direction:column;gap:9px}
  .tier.hl{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent) inset}
  .tier h3{margin:0;font-size:15px}
  .tier .price{font-size:26px;font-weight:800}.tier .price span{font-size:12px;color:var(--muted);font-weight:400}
  .tier ul{margin:0;padding-left:16px;color:var(--muted);font-size:12px;line-height:1.7;flex:1}
  .tier button{all:unset;text-align:center;cursor:pointer;border-radius:9px;padding:9px;font-weight:700;font-size:13px}
  .tier .buy{background:var(--accent);color:#02121a}
  .tier .buy.gold{background:#ffd86b}
  .tier .cur{background:rgba(120,150,200,.14);color:var(--muted);cursor:default}
  .modal .close{float:right;cursor:pointer;color:var(--muted);font-size:18px}
  .modal .foot{margin-top:14px;font-size:11px;color:var(--muted);text-align:center}
  .badge-pro{font-size:9px;font-weight:800;letter-spacing:.5px;background:#ffd86b;color:#1a1300;
    border-radius:5px;padding:1px 5px;margin-left:4px;vertical-align:middle}
</style>
</head>
<body>
<div id="app"><canvas id="cv"></canvas></div>

<header>
  <div class="brand"><b>NASDAQ-100 <span style="color:var(--accent)">BUBBLES</span></b>
    <span class="sub" id="genAt"></span></div>
  <div class="spacer"></div>
  <span class="lbl">Vue</span><div class="seg" id="modeSeg"></div>
  <span class="lbl">Taille</span><div class="seg" id="sizeSeg"></div>
  <span class="lbl">Période</span><div class="seg" id="tfSeg"></div>
  <select class="scheme" id="scheme" title="Schéma de couleur">
    <option value="gr">Vert / Rouge</option>
    <option value="by">Bleu / Jaune</option>
    <option value="heat">Heat</option>
  </select>
  <input class="search" id="search" placeholder="Rechercher…" autocomplete="off" />
  <button class="tbtn" id="btnPause" title="Pause / Reprendre (espace)">⏸</button>
  <button class="tbtn" id="btnFull" title="Plein écran">⛶</button>
  <button class="tbtn" id="btnShot" title="Exporter en PNG">⤓ PNG</button>
  <span class="alertbox" id="alertWrap" title="Alerte: surligne les variations ≥ seuil">
    🔔<input id="alertInput" type="number" placeholder="±%"><span class="lk" id="alertLock">PRO</span></span>
  <button class="tbtn pro" id="btnPro">★ Passer Pro</button>
</header>

<div id="breadth"></div>

<div class="modal" id="proModal"><div class="card">
  <span class="close" id="proClose">✕</span>
  <h2>NASDAQ Bubbles <span style="color:var(--accent)">Pro</span></h2>
  <div class="lead">Alertes de variation, watchlist illimitée, export de données et schémas avancés.</div>
  <div class="tiers">
    <div class="tier"><h3>Free</h3><div class="price">0€<span>/mois</span></div>
      <ul><li>Vue Actions &amp; Thèmes</li><li>5 périodes</li><li>Watchlist (5 max)</li><li>Données J-1</li></ul>
      <button class="cur" id="tierFree">Votre offre</button></div>
    <div class="tier hl"><h3>Pro</h3><div class="price">12€<span>/mois</span></div>
      <ul><li>Tout Free +</li><li>Alertes de variation</li><li>Watchlist illimitée</li><li>Export CSV / PNG HD</li>
        <li>Schémas de couleur avancés</li><li>Données intraday*</li></ul>
      <button class="buy gold" id="tierPro">Activer Pro (démo)</button></div>
    <div class="tier"><h3>Team</h3><div class="price">39€<span>/mois</span></div>
      <ul><li>Tout Pro +</li><li>5 sièges</li><li>Indices S&amp;P 500 / Dow*</li><li>API &amp; webhooks*</li><li>Support prioritaire</li></ul>
      <button class="buy" id="tierTeam">Nous contacter</button></div>
  </div>
  <div class="foot">*Nécessite un backend temps réel. Démo: « Activer Pro » débloque les fonctions côté client.</div>
</div></div>

<div class="crumb" id="crumb"><button id="backBtn">&larr; Retour</button><span>/</span>
  <span class="here" id="crumbHere"></span></div>

<button id="panelToggle">☰ Tableau</button>
<div id="panel">
  <div class="ph">
    <button class="chip on" data-f="all">Tous</button>
    <button class="chip" data-f="gain">Gainers</button>
    <button class="chip" data-f="lose">Losers</button>
    <button class="chip" data-f="watch" title="Ma watchlist">★</button>
    <button class="chip" id="csvBtn" title="Export CSV (Pro)">⤓ CSV</button>
    <select id="sectorFilter"><option value="">Tous secteurs</option></select>
  </div>
  <div id="rows"></div>
</div>

<div class="legend"><span>Baisse</span><span class="bar" id="legbar"></span><span>Hausse</span>
  <span style="margin-left:6px">• Halo = Renko validé</span></div>

<div id="news"><span class="tag">NEWS</span><div id="newsTrack"></div><span class="pausehint">⏸ survol = pause</span></div>
<div id="tip"></div>

<script>
const DATA = /*__PAYLOAD__*/null;

const cv=document.getElementById('cv'), ctx=cv.getContext('2d'), tip=document.getElementById('tip');
let DPR=Math.min(window.devicePixelRatio||1,2), W=0, H=0, panelW=0;
function resize(){
  DPR=Math.min(window.devicePixelRatio||1,2);
  W=window.innerWidth; H=window.innerHeight;
  panelW=document.getElementById('panel').classList.contains('open')?300:0;
  cv.width=W*DPR; cv.height=H*DPR; cv.style.width=W+'px'; cv.style.height=H+'px';
  ctx.setTransform(DPR,0,0,DPR,0,0);
}
let _rzT=null;
window.addEventListener('resize',()=>{
  resize();
  if(bubbles.length){ computeTargets(true);            // retaille pour la nouvelle fenetre
    for(const b of bubbles){b.vx*=.25;b.vy*=.25;} }   // tue l'inertie -> pas d'emballement
  clearTimeout(_rzT); _rzT=setTimeout(()=>{ if(bubbles.length)for(const b of bubbles){b.vx*=.4;b.vy*=.4;} },140);
});

const fmtPct=v=>(v==null)?'—':(v>=0?'+':'')+v.toFixed(1)+'%';
const cls=v=>(v==null)?'':(v>=0?'pos':'neg');
function fmtCap(v){ if(v==null)return '—';
  if(v>=1e12)return (v/1e12).toFixed(2)+' T$'; if(v>=1e9)return (v/1e9).toFixed(1)+' Md$';
  if(v>=1e6)return (v/1e6).toFixed(0)+' M$'; return v.toFixed(0)+'$'; }

// ---- couleurs (3 schemas) ----
function mix(a,b,t){return {r:a.r+(b.r-a.r)*t,g:a.g+(b.g-a.g)*t,b:a.b+(b.b-a.b)*t};}
function perfColor(v,cap){
  if(v==null)return {r:120,g:130,b:150};
  const t=Math.max(-1,Math.min(1,v/cap)), a=Math.abs(t);
  if(SCHEME==='by'){ const mid={r:95,g:105,b:125};
    return mix(mid, t>=0?{r:90,g:160,b:255}:{r:255,g:208,b:64}, a); }
  if(SCHEME==='heat'){ // froid->chaud sur la perf signee
    const u=(t+1)/2; const stops=[{r:40,g:90,b:200},{r:80,g:200,b:200},{r:240,g:230,b:90},{r:240,g:70,b:50}];
    const x=u*3, i=Math.min(2,Math.floor(x)); return mix(stops[i],stops[i+1],x-i); }
  const mid={r:95,g:105,b:125}; // vert/rouge
  return mix(mid, t>=0?{r:31,g:209,b:122}:{r:255,g:59,b:78}, a);
}

// ---- etat ----
let MODE='stocks';          // 'stocks' | 'themes'
let SIZE='perf';            // 'perf' | 'mcap' | 'score' — defaut perf: les gros movers grossissent
let TF='perf_63d';
let SCHEME='gr';
let drillTheme=null;        // theme name when drilled from THEMES view
let listFilter='all';       // all | gain | lose
let sectorFilter='';
let query='', hover=null, focusTicker=null, focusUntil=0;
let bubbles=[];
const STOCKS=DATA.stocks, BYTICKER={}; STOCKS.forEach(s=>BYTICKER[s.ticker]=s);

// --- SaaS state (persistance localStorage) ---
let PAUSED=false, PRO=false, alertTh=0;
let dragBubble=null,dragPX=0,dragPY=0,dragVX=0,dragVY=0,dragOX=0,dragOY=0,dragSX=0,dragSY=0,dragMoved=false;
let WATCH=new Set();
const LS='nb_';
try{const s=JSON.parse(localStorage.getItem(LS+'settings')||'{}');
  if(s.MODE)MODE=s.MODE; if(s.SIZE)SIZE=s.SIZE; if(s.TF)TF=s.TF; if(s.SCHEME)SCHEME=s.SCHEME;}catch(e){}
try{WATCH=new Set(JSON.parse(localStorage.getItem(LS+'watch')||'[]'));}catch(e){}
PRO=localStorage.getItem(LS+'pro')==='1';
function saveSettings(){try{localStorage.setItem(LS+'settings',JSON.stringify({MODE,SIZE,TF,SCHEME}));}catch(e){}}
function saveWatch(){try{localStorage.setItem(LS+'watch',JSON.stringify([...WATCH]));}catch(e){}}

if(!DATA.has_mcap && SIZE==='mcap') SIZE='score';

// ---- logos (charges a la demande, caches) ----
const logoCache={};
function logo(ticker){
  if(logoCache[ticker]!==undefined) return logoCache[ticker];
  const img=new Image();
  img.crossOrigin='anonymous';   // FMP renvoie ACAO:* -> canvas non taint -> export PNG possible
  img.onload=()=>{logoCache[ticker]=img;};
  img.onerror=()=>{logoCache[ticker]=null;};
  logoCache[ticker]='loading';
  img.src='https://financialmodelingprep.com/image-stock/'+encodeURIComponent(ticker)+'.png';
  return 'loading';
}

// ---- construction des bulles ----
function sizeValue(item,kind){
  if(kind==='theme') return item.score||1;
  if(SIZE==='mcap') return item.mcap||item.score||1;
  if(SIZE==='perf') return Math.abs(item.perf[TF]||0)+1;
  return item.score||1;
}
function currentStockList(){
  let list=STOCKS;
  if(MODE==='themes' && drillTheme) list=list.filter(s=>s.theme===drillTheme);
  if(sectorFilter) list=list.filter(s=>s.theme===sectorFilter);
  if(listFilter==='gain') list=list.filter(s=>(s.perf[TF]||0)>0);
  if(listFilter==='lose') list=list.filter(s=>(s.perf[TF]||0)<0);
  if(listFilter==='watch') list=list.filter(s=>WATCH.has(s.ticker));
  return list;
}
// Calcule le rayon CIBLE de chaque bulle pour la metrique/periode courante.
// Ecart volontairement marque (les gros movers dominent) + animation si demande.
// Met les rayons a l'echelle pour que la somme des AIRES occupe `fill` de la
// zone visible -> tout rentre quelle que soit la metrique (Perf/Cap/Score).
// Le poids relatif (winners plus gros) est preserve car r ∝ sqrt(poids).
function areaNormalize(weights, fill, minR, maxR, animated){
  const availW=Math.max(220,W-panelW), availH=Math.max(220,H-76-30);
  const target=fill*availW*availH;
  let radii=weights.map(w=>Math.sqrt(Math.max(1e-4,w)));
  // convergence: a chaque passe on recalcule l'echelle depuis l'etat courant
  // (le clamp minR/maxR distord l'aire, donc on itere pour viser `fill`).
  for(let pass=0;pass<4;pass++){
    const area=radii.reduce((s,r)=>s+Math.PI*r*r,0)||1;
    const scale=Math.sqrt(target/area);
    radii=radii.map(r=>Math.max(minR,Math.min(maxR,r*scale)));
  }
  bubbles.forEach((b,i)=>b.tr=radii[i]);
  if(!animated)for(const b of bubbles)b.r=b.tr;
}
function computeTargets(animated){
  const big=Math.min(Math.max(220,W-panelW),Math.max(220,H));
  if(MODE==='themes' && !drillTheme){
    const vals=bubbles.map(b=>b.ref.score||1);
    const lo=Math.min(...vals),hi=Math.max(...vals);
    const ws=vals.map(v=>{const t=hi===lo?.5:((v-lo)/(hi-lo)); return .35+Math.pow(t,.8);});
    areaNormalize(ws,.56,big*.05,big*.18,animated);
  }else{
    let ws;
    if(SIZE==='mcap'){
      // r ∝ cap^0.25 : megacaps nettement plus grosses, ecart visible mais borne
      ws=bubbles.map(b=>Math.pow((b.ref.mcap||b.ref.score||1),0.5));
    }else{
      const vals=bubbles.map(b=>sizeValue(b.ref,'stock'));
      const lo=Math.min(...vals),hi=Math.max(...vals);
      ws=vals.map(v=>{const t=hi===lo?.5:((v-lo)/(hi-lo)); return .14+Math.pow(t,.85);});
    }
    const n=bubbles.length, fill=n>60?.54:n>25?.6:.64;
    areaNormalize(ws,fill,big*.014,big*.17,animated);
  }
}
function makeBubble(spec){
  return {...spec,r:6,tr:30,x:W/2+(Math.random()-.5)*W*.6,y:H/2+(Math.random()-.5)*H*.5,
    vx:(Math.random()-.5)*1.6,vy:(Math.random()-.5)*1.6};
}
function rebuild(){
  resize();
  if(MODE==='themes' && !drillTheme){
    bubbles=DATA.themes.map(t=>makeBubble({kind:'theme',label:t.short,ref:t,validated:false}));
  }else{
    bubbles=currentStockList().map(s=>makeBubble({kind:'stock',label:s.ticker,ref:s,validated:s.validated}));
  }
  computeTargets(false);
  for(const b of bubbles)b.r=b.tr*0.18;   // entree "pop": part petit puis grossit
  renderRows();
}

// ---- physique ----
function step(){
  const cx=(W-panelW)/2, cy=H/2+12;
  for(const b of bubbles){
    b.r+=(b.tr-b.r)*.10;                                  // animation fluide de la taille
    if(b===dragBubble) continue;                          // bulle tenue a la souris: pas de force
    b.vx+=(cx-b.x)*.00055; b.vy+=(cy-b.y)*.00055;         // attraction douce (moins serree)
    b.vx+=(Math.random()-.5)*.35; b.vy+=(Math.random()-.5)*.35;  // micro-mouvement vivant
    b.vx*=.972; b.vy*=.972;
    const sp=Math.hypot(b.vx,b.vy), mx=2.6; if(sp>mx){b.vx*=mx/sp;b.vy*=mx/sp;}
    b.x+=b.vx; b.y+=b.vy;
    const pad=6, top=pad+46, right=W-panelW-pad, bot=H-pad-30;
    if(b.x-b.r<pad){b.x=b.r+pad;b.vx=Math.abs(b.vx)*.7;}
    if(b.x+b.r>right){b.x=right-b.r;b.vx=-Math.abs(b.vx)*.7;}
    if(b.y-b.r<top){b.y=b.r+top;b.vy=Math.abs(b.vy)*.7;}
    if(b.y+b.r>bot){b.y=bot-b.r;b.vy=-Math.abs(b.vy)*.7;}
  }
  for(let i=0;i<bubbles.length;i++)for(let j=i+1;j<bubbles.length;j++){
    const a=bubbles[i],b=bubbles[j]; let dx=b.x-a.x,dy=b.y-a.y,d=Math.hypot(dx,dy)||.01;
    const min=a.r+b.r+2;
    if(d<min){const ov=(min-d)/2,nx=dx/d,ny=dy/d;
      a.x-=nx*ov;a.y-=ny*ov;b.x+=nx*ov;b.y+=ny*ov;
      const p=(a.vx*nx+a.vy*ny-b.vx*nx-b.vy*ny);
      if(p>0){a.vx-=p*nx*.65;a.vy-=p*ny*.65;b.vx+=p*nx*.65;b.vy+=p*ny*.65;}}
  }
  if(dragBubble){dragBubble.x=dragPX;dragBubble.y=dragPY;}  // re-epingle sous le pointeur
}

// ---- rendu ----
function draw(){
  ctx.clearRect(0,0,W,H);
  const cap=(MODE==='themes'&&!drillTheme)?25:120;
  const q=query.trim().toLowerCase();
  const now=performance.now();
  for(const b of bubbles){
    const perf=b.ref.perf[TF], c=perfColor(perf,cap);
    const match=q&&(b.label.toLowerCase().includes(q)||(b.ref.name||'').toLowerCase().includes(q)||(b.ref.theme||'').toLowerCase().includes(q));
    const dim=q&&!match?.16:1;
    const isFocus=focusTicker&&b.ref.ticker===focusTicker&&now<focusUntil;

    if(b.validated){ctx.beginPath();ctx.arc(b.x,b.y,b.r+4,0,7);ctx.strokeStyle=`rgba(76,240,160,${.7*dim})`;
      ctx.lineWidth=2;ctx.shadowColor='rgba(76,240,160,.6)';ctx.shadowBlur=14;ctx.stroke();ctx.shadowBlur=0;}
    if(b.kind==='stock'&&WATCH.has(b.ref.ticker)){ctx.beginPath();ctx.arc(b.x,b.y,b.r+(b.validated?7:4),0,7);
      ctx.strokeStyle=`rgba(255,216,107,${.95*dim})`;ctx.lineWidth=2.5;ctx.stroke();}
    if(alertTh>0&&Math.abs(b.ref.perf[TF]||0)>=alertTh){const pl=.5+.5*Math.sin(now/200);
      ctx.beginPath();ctx.arc(b.x,b.y,b.r+6,0,7);ctx.strokeStyle=`rgba(255,216,107,${(.3+.55*pl)*dim})`;
      ctx.lineWidth=3;ctx.shadowColor='rgba(255,216,107,.7)';ctx.shadowBlur=16;ctx.stroke();ctx.shadowBlur=0;}

    const g=ctx.createRadialGradient(b.x-b.r*.32,b.y-b.r*.36,b.r*.1,b.x,b.y,b.r);
    g.addColorStop(0,`rgba(${c.r+45|0},${c.g+45|0},${c.b+45|0},${.96*dim})`);
    g.addColorStop(.7,`rgba(${c.r|0},${c.g|0},${c.b|0},${.5*dim})`);
    g.addColorStop(1,`rgba(${c.r*.35|0},${c.g*.35|0},${c.b*.35|0},${.08*dim})`);
    ctx.beginPath();ctx.arc(b.x,b.y,b.r,0,7);ctx.fillStyle=g;ctx.fill();
    ctx.lineWidth=1.5;ctx.strokeStyle=(isFocus?'rgba(56,225,255,.95)':`rgba(${c.r|0},${c.g|0},${c.b|0},${.9*dim})`);
    if(isFocus)ctx.lineWidth=3; ctx.stroke();
    if(hover===b){ctx.lineWidth=2.5;ctx.strokeStyle='rgba(255,255,255,.85)';ctx.stroke();}

    // reflet glossy
    ctx.beginPath();ctx.arc(b.x-b.r*.3,b.y-b.r*.34,b.r*.42,0,7);
    const gl=ctx.createRadialGradient(b.x-b.r*.3,b.y-b.r*.34,0,b.x-b.r*.3,b.y-b.r*.34,b.r*.42);
    gl.addColorStop(0,`rgba(255,255,255,${.22*dim})`);gl.addColorStop(1,'rgba(255,255,255,0)');
    ctx.fillStyle=gl;ctx.fill();

    ctx.globalAlpha=dim;
    // logo (actions seulement, bulles assez grandes)
    let logoH=0;
    if(b.kind==='stock'&&b.r>22){
      const im=logo(b.ref.ticker);
      if(im&&im!=='loading'){const s=b.r*.85,ix=b.x-s/2,iy=b.y-b.r*.52;
        ctx.save();ctx.beginPath();ctx.rect(ix,iy,s,s*.6);ctx.clip();
        try{ctx.drawImage(im,ix,iy,s,s*.55);}catch(e){} ctx.restore(); logoH=b.r*.24;}
    }
    ctx.textAlign='center';ctx.textBaseline='middle';ctx.fillStyle='#fff';
    const big=b.r>26;
    ctx.font=`700 ${Math.max(10,Math.min(b.r*.34,22))}px "Segoe UI",sans-serif`;
    ctx.fillText(fitLabel(b.label,b.r),b.x,b.y+(logoH)-(big?b.r*.04:0));
    if(big){ctx.font=`600 ${Math.max(9,Math.min(b.r*.26,16))}px "Segoe UI",sans-serif`;
      ctx.fillStyle=perf>=0?'#d6ffe9':'#ffe0e4';ctx.fillText(fmtPct(perf),b.x,b.y+b.r*.34+logoH);}
    ctx.globalAlpha=1;
  }
}
function fitLabel(t,r){const m=Math.floor(r/4.2);return t.length>m?t.slice(0,Math.max(2,m-1))+'…':t;}
function loop(){if(!PAUSED)step();draw();requestAnimationFrame(loop);}

// ---- interaction canvas ----
function bubbleAt(mx,my){for(let i=bubbles.length-1;i>=0;i--){const b=bubbles[i];
  if(Math.hypot(mx-b.x,my-b.y)<=b.r)return b;}return null;}
cv.addEventListener('mousedown',e=>{const b=bubbleAt(e.clientX,e.clientY);if(!b)return;
  dragBubble=b;dragMoved=false;dragSX=e.clientX;dragSY=e.clientY;
  dragOX=e.clientX-b.x;dragOY=e.clientY-b.y;dragPX=b.x;dragPY=b.y;dragVX=0;dragVY=0;
  cv.style.cursor='grabbing';tip.style.display='none';});
cv.addEventListener('mousemove',e=>{
  if(dragBubble){const nx=e.clientX-dragOX,ny=e.clientY-dragOY;
    dragVX=(nx-dragPX)*.45;dragVY=(ny-dragPY)*.45;dragPX=nx;dragPY=ny;dragBubble.x=nx;dragBubble.y=ny;
    if(Math.hypot(e.clientX-dragSX,e.clientY-dragSY)>4)dragMoved=true; return;}
  const b=bubbleAt(e.clientX,e.clientY);hover=b;cv.style.cursor=b?'grab':'default';
  if(!b){tip.style.display='none';return;} showTip(b,e.clientX,e.clientY);});
window.addEventListener('mouseup',()=>{
  if(!dragBubble)return; const b=dragBubble,moved=dragMoved;
  b.vx=Math.max(-9,Math.min(9,dragVX));b.vy=Math.max(-9,Math.min(9,dragVY));
  dragBubble=null;cv.style.cursor='default';
  if(!moved&&b.kind==='theme')drillIn(b.ref.theme);});
cv.addEventListener('mouseleave',()=>{if(!dragBubble){hover=null;tip.style.display='none';}});

function showTip(b,mx,my){
  const perf=b.ref.perf[TF]; let html='';
  if(b.kind==='theme'){const t=b.ref;
    html=`<h4>${t.theme}</h4>
      <div class="row"><span>Perf médiane (${labelOf(TF)})</span><b class="${cls(perf)}">${fmtPct(perf)}</b></div>
      <div class="row"><span>Score composite</span><b>${(t.score??0).toFixed(1)}</b></div>
      <div class="row"><span>Actions</span><b>${t.stocks}</b></div>
      <div class="row"><span>% &gt; SMA50</span><b>${t.above50!=null?t.above50.toFixed(0)+'%':'—'}</b></div>
      <div class="row"><span>Rel 63J vs QQQ</span><b class="${cls(t.rel63)}">${fmtPct(t.rel63)}</b></div>
      <div class="row"><span>News</span><b>${t.news_hits}/${t.news_sources}</b></div>
      <span class="tag">Cliquer pour éclater →</span>`;
  }else{const s=b.ref;
    html=`<h4>${s.ticker} — ${s.name}</h4>
      <div class="row"><span>Perf (${labelOf(TF)})</span><b class="${cls(perf)}">${fmtPct(perf)}</b></div>
      <div class="row"><span>Market cap</span><b>${fmtCap(s.mcap)}</b></div>
      <div class="row"><span>Score</span><b>${(s.score??0).toFixed(1)}</b></div>
      <div class="row"><span>RSI 14</span><b>${s.rsi!=null?s.rsi.toFixed(0):'—'}</b></div>
      <div class="row"><span>Rel 63J vs QQQ</span><b class="${cls(s.rel63)}">${fmtPct(s.rel63)}</b></div>
      <div class="row"><span>Secteur</span><b>${s.short_theme}</b></div>
      ${s.validated?'<span class="tag">Renko 3M/M/W validé</span>':''}`;
  }
  tip.innerHTML=html;tip.style.display='block';
  const tw=tip.offsetWidth,th=tip.offsetHeight; let x=mx+16,y=my+16;
  if(x+tw>W)x=mx-tw-16; if(y+th>H)y=my-th-16; tip.style.left=x+'px';tip.style.top=y+'px';
}
function labelOf(k){return (DATA.timeframes.find(t=>t.key===k)||{}).label||k;}

function drillIn(theme){MODE='themes';drillTheme=theme;
  document.getElementById('crumb').style.display='flex';
  document.getElementById('crumbHere').textContent=theme; tip.style.display='none'; rebuild();}
function drillOut(){drillTheme=null;
  document.getElementById('crumb').style.display='none';
  if(MODE==='themes')rebuild(); }
document.getElementById('backBtn').addEventListener('click',drillOut);
document.addEventListener('keydown',e=>{if(e.key==='Escape'&&drillTheme)drillOut();});

// ---- panneau tableau ----
function renderRows(){
  const box=document.getElementById('rows');
  let list=currentStockList().slice().sort((a,b)=>(b.perf[TF]||-999)-(a.perf[TF]||-999));
  box.innerHTML='';
  for(const s of list){
    const perf=s.perf[TF];
    const row=document.createElement('div'); row.className='r';
    row.innerHTML=`<div class="star ${WATCH.has(s.ticker)?'on':''}">★</div>
      <img loading="lazy" crossorigin="anonymous" src="https://financialmodelingprep.com/image-stock/${s.ticker}.png" onerror="this.style.visibility='hidden'">
      <div style="min-width:0"><div class="tk">${s.ticker}${s.validated?' <span style="color:#4cf0a0">●</span>':''}</div>
      <div class="nm">${s.name}</div></div>
      <div class="pf ${cls(perf)}">${fmtPct(perf)}<div class="nm" style="text-align:right">${fmtCap(s.mcap)}</div></div>`;
    const star=row.querySelector('.star');
    star.onclick=ev=>{ev.stopPropagation();toggleWatch(s.ticker,star);};
    row.onclick=()=>{focusTicker=s.ticker;focusUntil=performance.now()+2500;
      const b=bubbles.find(x=>x.ref.ticker===s.ticker); if(b){b.vx+=(Math.random()-.5)*4;b.vy-=6;}};
    box.appendChild(row);
  }
  updateBreadth();
}
function toggleWatch(tk,starEl){
  if(WATCH.has(tk)){WATCH.delete(tk);}
  else{ if(!PRO && WATCH.size>=5){window.__openPro&&window.__openPro();return;} WATCH.add(tk);}
  saveWatch(); if(starEl)starEl.classList.toggle('on',WATCH.has(tk));
  if(listFilter==='watch')rebuild();
}
function updateBreadth(){
  const list=currentStockList(); let up=0,dn=0,sum=0,nn=0;
  for(const s of list){const v=s.perf[TF]; if(v==null)continue; if(v>0.05)up++; else if(v<-0.05)dn++; sum+=v;nn++;}
  const avg=nn?sum/nn:0;
  document.getElementById('breadth').innerHTML=
    `<b>${list.length}</b> actifs <span class="sep">|</span> <span class="up">▲ ${up}</span> `+
    `<span class="dn">▼ ${dn}</span> <span class="sep">|</span> moy `+
    `<b class="${avg>=0?'up':'dn'}">${(avg>=0?'+':'')+avg.toFixed(1)}%</b>`;
}
document.getElementById('panelToggle').addEventListener('click',()=>{
  document.getElementById('panel').classList.toggle('open');resize();});
document.querySelectorAll('.chip[data-f]').forEach(c=>c.addEventListener('click',()=>{
  document.querySelectorAll('.chip[data-f]').forEach(x=>x.classList.remove('on'));
  c.classList.add('on'); listFilter=c.dataset.f; rebuild();}));
document.getElementById('sectorFilter').addEventListener('change',e=>{sectorFilter=e.target.value;rebuild();});

// ---- header ----
function seg(id,items,cur,on){const el=document.getElementById(id);el.innerHTML='';
  items.forEach(it=>{const b=document.createElement('button');b.textContent=it.label;b.dataset.k=it.key;
    if(it.key===cur())b.classList.add('on');
    b.onclick=()=>{on(it.key);[...el.children].forEach(c=>c.classList.toggle('on',c.dataset.k===cur()));};
    el.appendChild(b);});}
function buildHeader(){
  seg('modeSeg',[{key:'stocks',label:'Actions'},{key:'themes',label:'Thèmes'}],()=>MODE,k=>{
    MODE=k;drillOut();document.getElementById('sizeSeg').style.opacity=(k==='themes')?.4:1;saveSettings();rebuild();});
  const sizeItems=[{key:'perf',label:'Perf'},{key:'mcap',label:'Cap'},{key:'score',label:'Score'}];
  seg('sizeSeg',sizeItems,()=>SIZE,k=>{SIZE=k;saveSettings();computeTargets(true);renderRows();});
  if(!DATA.has_mcap){const b=[...document.querySelectorAll('#sizeSeg button')].find(x=>x.dataset.k==='mcap');
    if(b){b.title='Market cap absent — lance: python nasdaq_bubbles.py --enrich';b.style.opacity=.45;}}
  seg('tfSeg',DATA.timeframes,()=>TF,k=>{TF=k;saveSettings();computeTargets(true);renderRows();});
}
function applyScheme(){document.getElementById('scheme').value=SCHEME;
  document.getElementById('legbar').style.background=
    SCHEME==='by'?'linear-gradient(90deg,#ffd040,#6b7280,#5aa0ff)':
    SCHEME==='heat'?'linear-gradient(90deg,#285ec8,#50c8c8,#f0e65a,#f04632)':
    'linear-gradient(90deg,#ff3b4e,#6b7280,#1fd17a)';}
document.getElementById('scheme').addEventListener('change',e=>{SCHEME=e.target.value;saveSettings();applyScheme();});
document.getElementById('search').addEventListener('input',e=>{query=e.target.value;});

// ---- news ----
function buildNews(){const tr=document.getElementById('newsTrack');
  if(!DATA.news.length){document.getElementById('news').style.display='none';return;}
  tr.innerHTML=DATA.news.map(n=>`<span>[<span style="color:var(--accent)">${n.theme}</span>] `+
    `<a href="${n.link}" target="_blank">${n.title}</a> <span class="src">— ${n.source}</span></span>`).join('');}

function buildSaaS(){
  const bp=document.getElementById('btnPause');
  bp.onclick=()=>{PAUSED=!PAUSED;bp.textContent=PAUSED?'▶':'⏸';bp.classList.toggle('on',PAUSED);};
  document.addEventListener('keydown',e=>{const t=e.target.tagName;
    if(e.code==='Space'&&t!=='INPUT'&&t!=='SELECT'){e.preventDefault();bp.click();}});
  document.getElementById('btnFull').onclick=()=>{
    if(!document.fullscreenElement){const f=document.documentElement.requestFullscreen;if(f)f.call(document.documentElement);}
    else{const x=document.exitFullscreen;if(x)x.call(document);}};
  document.getElementById('btnShot').onclick=()=>{
    try{const a=document.createElement('a');a.href=cv.toDataURL('image/png');a.download='nasdaq_bubbles.png';a.click();}
    catch(err){alert('Export PNG indisponible (logos bloqués par le cache navigateur). Recharge et réessaie.');}};
  // --- Pro / paywall ---
  const setPro=on=>{PRO=on;try{localStorage.setItem(LS+'pro',on?'1':'0');}catch(e){}
    document.getElementById('alertLock').style.display=on?'none':'inline';
    const pb=document.getElementById('btnPro');pb.textContent=on?'★ Pro actif':'★ Passer Pro';pb.classList.toggle('on',on);};
  const openPro=()=>document.getElementById('proModal').classList.add('open');
  const closePro=()=>document.getElementById('proModal').classList.remove('open');
  window.__openPro=openPro;
  document.getElementById('btnPro').onclick=openPro;
  document.getElementById('proClose').onclick=closePro;
  document.getElementById('proModal').addEventListener('click',e=>{if(e.target.id==='proModal')closePro();});
  document.getElementById('tierPro').onclick=()=>{setPro(true);closePro();};
  document.getElementById('tierTeam').onclick=()=>{location.href='mailto:sales@example.com?subject=NASDAQ%20Bubbles%20Team';};
  setPro(PRO || !!DATA.owner_pro);   // build proprietaire (--pro): Pro par defaut
  // --- CSV (Pro) ---
  document.getElementById('csvBtn').onclick=()=>{ if(!PRO){openPro();return;}
    const list=currentStockList().slice().sort((a,b)=>(b.perf[TF]||-999)-(a.perf[TF]||-999));
    const head=['ticker','name','theme','perf_'+TF,'mcap','score','rsi','rel63','validated'];
    const rows=list.map(s=>[s.ticker,'"'+(s.name||'').replace(/"/g,'')+'"',s.short_theme,
      (s.perf[TF]??''),(s.mcap??''),(s.score??''),(s.rsi??''),(s.rel63??''),s.validated]);
    const csv=[head.join(','),...rows.map(r=>r.join(','))].join('\n');
    const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'}));
    a.download='nasdaq_bubbles_'+TF+'.csv';a.click();};
  // --- Alertes (Pro) ---
  const ai=document.getElementById('alertInput');
  ai.addEventListener('focus',()=>{if(!PRO){ai.blur();openPro();}});
  ai.addEventListener('input',()=>{if(PRO)alertTh=Math.abs(parseFloat(ai.value)||0);});
}

// ---- init ----
(function init(){
  const dt=DATA.generated_at?new Date(DATA.generated_at):null;
  document.getElementById('genAt').textContent=dt
    ?'maj '+dt.toLocaleString('fr-FR',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'})
    +(DATA.has_mcap?'':' • cap: --enrich'):'';
  const sf=document.getElementById('sectorFilter');
  DATA.sectors.forEach(s=>{const o=document.createElement('option');o.value=s;
    o.textContent=s.slice(0,26);sf.appendChild(o);});
  buildHeader();buildNews();buildSaaS();applyScheme();
  document.getElementById('sizeSeg').style.opacity=(MODE==='themes')?.4:1;
  if(MODE==='themes')document.querySelector('#modeSeg button[data-k="themes"]').classList.add('on');
  rebuild();loop();
})();
</script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Genere nasdaq_bubbles.html depuis le rapport pipeline.")
    parser.add_argument("--report", default=REPORT_JSON, help="Rapport JSON source.")
    parser.add_argument("--out", default=OUTPUT_HTML, help="HTML de sortie.")
    parser.add_argument("--enrich", action="store_true",
                        help="Recupere market cap + volume (yfinance) avant de generer.")
    parser.add_argument("--pro", action="store_true",
                        help="Build proprietaire: debloque les fonctions Pro par defaut (ta version perso).")
    args = parser.parse_args()

    if not os.path.exists(args.report):
        print(f"Rapport introuvable: {args.report}", file=sys.stderr)
        return 1

    with open(args.report, encoding="utf-8") as f:
        report = json.load(f)

    if args.enrich:
        tickers = [s.get("ticker", "") for s in report.get("stocks", []) if s.get("ticker")]
        print(f"Enrichissement market cap pour {len(tickers)} tickers (yfinance)...")
        enrich_market_caps(tickers)

    mcaps = load_mcap_sidecar()
    payload = build_payload(report, mcaps)
    payload["owner_pro"] = bool(args.pro)
    html = render_html(payload)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)

    n_mcap = sum(1 for s in payload["stocks"] if s["mcap"])
    print(f"OK -> {args.out}")
    print(f"  {len(payload['stocks'])} actions, {len(payload['themes'])} themes, "
          f"{len(payload['news'])} news, market cap dispo pour {n_mcap} actions.")
    if not payload["has_mcap"]:
        print("  Astuce: 'python nasdaq_bubbles.py --enrich' pour la taille par capitalisation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
