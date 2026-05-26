#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 10 Reuters/EIA Narrative Sentiment LIGHT

Local light pipeline:
  Reuters Commodities RSS/feed + EIA Today in Energy RSS
  -> headline/snippet parsing
  -> deduplication
  -> commodity tagging
  -> event category tagging
  -> direction + confidence scoring
  -> Claude-ready tagged event alerts
  -> JSON / JSONL / CSV + validation report

Reuters policy:
  - headline/snippet event detection only
  - no full-article fetch
  - no full-article reproduction
  - output is paraphrase/summary only
  - tos_status = REVIEW_PARAPHRASE_ONLY

v1.1 updates:
  - default Reuters source is GOOGLE_NEWS_REUTERS_COMMODITIES_PROXY
  - --reuters-feed activates official REUTERS_COMMODITIES_RSS source_id
  - strict enabled-feed validation
  - confidence_method=RULE_BASED
  - claude_preprocessing_payload included
  - NEUTRAL when commodity/event are clear but direction evidence is absent

v1.2 updates:
  - default lookback window changed to 21 days
  - default Reuters proxy collection is target-first by commodity
  - each commodity is processed sequentially with a target minimum of 30 raw items
  - per-target fetch metadata and warnings are included in validation output
  - latest output remains capped at 20 alerts

v1.3 updates:
  - target-aware deduplication so one cross-market item can remain mapped to multiple commodities
  - target coverage is validated after source merge, deduplication, and lookback filtering
  - EIA RSS is fetched once and reused across target filters
  - enabled-feed validation is aggregated instead of overwritten by repeated target metas
  - keyword matching handles symbols such as OPEC+ and gold/silver

Run:
  python _reuters_eia_narrative_light.py

Optional:
  python _reuters_eia_narrative_light.py --lookback-hours 504
  python _reuters_eia_narrative_light.py --disable-reuters
  python _reuters_eia_narrative_light.py --reuters-feed "YOUR_OFFICIAL_REUTERS_RSS_URL"
"""

import argparse
import csv
import hashlib
import html
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

try:
    import requests
except Exception:
    requests = None


DEFAULT_OUT_DIR = Path.home() / "narrative_output" / "step10_reuters_eia_narrative_light"
DEFAULT_LOOKBACK_HOURS = 24 * 21
DEFAULT_MAX_ITEMS_PER_FEED = 100
DEFAULT_MIN_ITEMS_PER_COMMODITY = 30
DEFAULT_LATEST_LIMIT = 20
DEFAULT_GATEKEEPER_ID = "LOCAL_PROTO"

DEFAULT_EIA_FEED = "https://www.eia.gov/rss/todayinenergy.xml"

# Reuters public topic RSS changes over time. For local light testing, this
# default uses Google News RSS constrained to Reuters commodities pages. If you
# have official Reuters Connect RSS, pass it with --reuters-feed.
DEFAULT_REUTERS_FEED = (
    "https://news.google.com/rss/search?q="
    + quote_plus("site:reuters.com/markets/commodities Reuters commodities")
    + "&hl=en-US&gl=US&ceid=US:en"
)

SOURCE_CONFIG = {
    "reuters_proxy": {
        "source_name": "Google News Reuters Commodities Proxy",
        "source_id": "GOOGLE_NEWS_REUTERS_COMMODITIES_PROXY",
        "underlying_publisher": "Reuters",
        "tos_status": "REVIEW_PARAPHRASE_ONLY",
        "gatekeeper_cleared": True,
        "policy": "Google News RSS query constrained to Reuters commodities pages; headline/snippet event detection only; no full article reproduction.",
        "is_proxy": True,
    },
    "reuters": {
        "source_name": "Reuters",
        "source_id": "REUTERS_COMMODITIES_RSS",
        "underlying_publisher": "Reuters",
        "tos_status": "REVIEW_PARAPHRASE_ONLY",
        "gatekeeper_cleared": True,
        "policy": "Official Reuters RSS/feed supplied by user; headline/snippet event detection only; no full article reproduction.",
        "is_proxy": False,
    },
    "eia": {
        "source_name": "EIA Today in Energy",
        "source_id": "EIA_TODAY_IN_ENERGY_RSS",
        "underlying_publisher": "EIA",
        "tos_status": "GO_PUBLIC_SOURCE",
        "gatekeeper_cleared": True,
        "policy": "Public EIA RSS feed.",
        "is_proxy": False,
    },
}

COMMODITY_KEYWORDS = {
    "CL": ["wti", "crude oil", "crude", "oil prices", "brent", "opec", "oil output", "oil production", "oil inventories", "cushing", "petroleum"],
    "NG": ["natural gas", "natgas", "lng", "henry hub", "gas storage", "gas inventories", "gas output", "power burn", "feedgas"],
    "HO": ["heating oil", "diesel", "distillate", "distillates", "ulsd", "gasoil"],
    "RB": ["gasoline", "rbob", "motor fuel", "gasoline stocks", "gasoline inventories"],
    "ZC": ["corn", "maize", "ethanol", "corn crop", "corn exports"],
    "ZS": ["soybean", "soybeans", "soymeal", "soy oil", "soybean oil", "soy exports"],
    "ZW": ["wheat", "srw", "hrw", "black sea wheat", "wheat exports", "grain exports"],
    "GC": ["gold", "bullion", "precious metals", "safe haven", "safe-haven", "real yields", "fed rate", "dollar"],
    "SI": ["silver", "precious metals", "industrial metals", "gold/silver"],
}

DEFAULT_COMMODITY_ORDER = ["CL", "NG", "HO", "RB", "ZC", "ZS", "ZW", "GC", "SI"]

# Search phrases used by the Reuters Google News proxy. Keep these concise so
# each target is collected first by its own market language before moving on.
REUTERS_PROXY_COMMODITY_QUERIES = {
    "CL": "Reuters commodities crude oil WTI Brent oil inventories OPEC",
    "NG": "Reuters commodities natural gas LNG Henry Hub gas storage",
    "HO": "Reuters commodities heating oil diesel distillates ULSD",
    "RB": "Reuters commodities gasoline RBOB gasoline inventories",
    "ZC": "Reuters commodities corn maize ethanol crop exports",
    "ZS": "Reuters commodities soybeans soymeal soybean oil exports",
    "ZW": "Reuters commodities wheat grain Black Sea exports",
    "GC": "Reuters commodities gold bullion precious metals dollar yields",
    "SI": "Reuters commodities silver precious metals industrial demand",
}


def build_reuters_proxy_feed_for_commodity(symbol: str) -> str:
    query = REUTERS_PROXY_COMMODITY_QUERIES.get(symbol, f"Reuters commodities {symbol}")
    return (
        "https://news.google.com/rss/search?q="
        + quote_plus("site:reuters.com/markets/commodities " + query)
        + "&hl=en-US&gl=US&ceid=US:en"
    )


def parse_commodity_order(value: str) -> List[str]:
    symbols = [x.strip().upper() for x in re.split(r"[,\s]+", value or "") if x.strip()]
    unknown = [x for x in symbols if x not in COMMODITY_KEYWORDS]
    if unknown:
        raise ValueError(f"Unknown commodity symbol(s): {unknown}. Allowed: {sorted(COMMODITY_KEYWORDS)}")
    return symbols or DEFAULT_COMMODITY_ORDER


def item_mentions_commodity(item: Dict[str, Any], symbol: str) -> bool:
    text = " ".join([item.get("title", ""), item.get("summary", "")]).strip()
    return bool(hits(text, COMMODITY_KEYWORDS.get(symbol, [])))

EVENT_KEYWORDS = {
    "INVENTORY_DRAW": ["draw", "drawdown", "stocks fell", "stockpiles fell", "inventories fell", "inventory draw"],
    "INVENTORY_BUILD": ["build", "stocks rose", "stockpiles rose", "inventories rose", "inventory build"],
    "SUPPLY_DISRUPTION": ["outage", "shutdown", "disruption", "strike", "sanction", "sanctions", "halt", "attack", "conflict", "war", "storm", "hurricane", "force majeure"],
    "SUPPLY_INCREASE": ["output rises", "production rises", "production increase", "supply increase", "higher output", "record output"],
    "DEMAND_STRENGTH": ["strong demand", "demand rises", "demand growth", "higher demand", "export demand", "strong exports"],
    "DEMAND_WEAKNESS": ["weak demand", "demand falls", "demand slowdown", "lower demand", "demand concern", "demand worries"],
    "EXPORT_STRENGTH": ["exports rise", "export sales", "strong exports", "export demand", "shipments rise"],
    "EXPORT_WEAKNESS": ["exports fall", "export slowdown", "weak exports", "shipments fall", "export curbs"],
    "REFINERY_OUTAGE": ["refinery outage", "refinery shutdown", "refinery fire", "refinery maintenance", "refinery run"],
    "GEOPOLITICAL_RISK": ["geopolitical", "middle east", "russia", "ukraine", "iran", "red sea", "sanctions", "conflict", "war", "tariff"],
    "WEATHER_RISK": ["weather", "drought", "rain", "flood", "freeze", "heat", "hurricane", "storm", "crop condition", "planting", "harvest"],
    "MACRO_POLICY": ["fed", "federal reserve", "interest rates", "rates", "dollar", "treasury yields", "inflation", "cpi", "ppi", "tariff", "policy"],
    "OPEC_POLICY": ["opec", "opec+", "production cut", "output cut", "quota"],
    "EIA_REPORT": ["eia", "weekly petroleum", "storage report", "today in energy"],
    "USDA_REPORT": ["usda", "wasde", "crop progress", "export sales"],
}

BULLISH_HINTS = ["draw", "drawdown", "fell", "falls", "decline", "lower inventories", "shortage", "tight", "outage", "shutdown", "disruption", "sanctions", "production cut", "output cut", "strong demand", "demand growth", "exports rise", "weaker dollar", "safe haven"]
BEARISH_HINTS = ["build", "rose", "rises", "rising inventories", "surplus", "weak demand", "demand slowdown", "lower demand", "production rise", "output rises", "higher output", "record output", "ceasefire", "stronger dollar", "higher yields", "hawkish fed"]

DIRECTION_BY_EVENT = {
    "INVENTORY_DRAW": "BULLISH",
    "INVENTORY_BUILD": "BEARISH",
    "SUPPLY_DISRUPTION": "BULLISH",
    "SUPPLY_INCREASE": "BEARISH",
    "DEMAND_STRENGTH": "BULLISH",
    "DEMAND_WEAKNESS": "BEARISH",
    "EXPORT_STRENGTH": "BULLISH",
    "EXPORT_WEAKNESS": "BEARISH",
    "REFINERY_OUTAGE": "MIXED",
    "GEOPOLITICAL_RISK": "BULLISH",
}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_json(p: Path, obj: Any) -> None:
    ensure_dir(p.parent)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(p: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(p.parent)
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_csv(p: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(p.parent)
    if not rows:
        p.write_text("", encoding="utf-8")
        return
    preferred = [
        "event_alert_id", "source_id", "source", "target_commodity", "target_fetch_rank",
        "target_keyword_query", "timestamp", "published_at", "data_type",
        "commodities", "commodity_groups", "primary_commodity", "event_category",
        "direction", "confidence_score", "confidence_label", "confidence_method", "headline_original",
        "headline_paraphrase", "snippet_summary", "url", "raw_text_hash",
        "tos_status", "gatekeeper_cleared", "gatekeeper_id", "raw_source_url",
        "retrieved_at", "normalized_at",
    ]
    keys = [k for k in preferred if any(k in r for r in rows)]
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: json.dumps(r.get(k), ensure_ascii=False) if isinstance(r.get(k), (list, dict)) else r.get(k) for k in keys})


def require_dependencies() -> None:
    if requests is None:
        print("[ERROR] Missing dependency: requests. Install with: pip install requests")
        raise SystemExit(2)


def clean_text(x: Any) -> str:
    s = html.unescape("" if x is None else str(x))
    s = re.sub(r"<script.*?</script>|<style.*?</style>", " ", s, flags=re.I | re.S)
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_dt(x: Any) -> Optional[str]:
    if not x:
        return None
    s = str(x).strip()
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s[:25], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
        except Exception:
            pass
    return None


def iso_to_dt(x: Optional[str]) -> Optional[datetime]:
    try:
        return datetime.fromisoformat((x or "").replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def short_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore") + b"\n")
    return h.hexdigest()[:24]


def child_text(node: ET.Element, names: List[str]) -> str:
    for child in node:
        tag = child.tag.split("}")[-1].lower()
        if tag in {n.lower() for n in names}:
            return child.text or ""
    return ""


def child_attr(node: ET.Element, tag_name: str, attr_name: str) -> str:
    for child in node:
        if child.tag.split("}")[-1].lower() == tag_name.lower():
            return child.attrib.get(attr_name, "")
    return ""


def http_get_text(url: str) -> str:
    headers = {"User-Agent": "Metrixx-Step10-Narrative-Light/1.0", "Accept": "application/rss+xml,application/xml,text/xml,*/*"}
    last = None
    for i in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=45)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (2 ** i))
                continue
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(1.5 * (2 ** i))
    raise RuntimeError(f"GET failed: {url}; error={last}")


def parse_feed(xml_text: str, source_key: str, feed_url: str, retrieved_at: str, max_items: int, target_commodity: Optional[str] = None, target_keyword_query: Optional[str] = None, target_fetch_rank: Optional[int] = None) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_text.encode("utf-8"))
    nodes = [e for e in root.iter() if e.tag.split("}")[-1].lower() == "item"]
    if not nodes:
        nodes = [e for e in root.iter() if e.tag.split("}")[-1].lower() == "entry"]
    out = []
    cfg = SOURCE_CONFIG[source_key]
    for n in nodes[:max_items]:
        atom = n.tag.split("}")[-1].lower() == "entry"
        title = clean_text(child_text(n, ["title"]))
        summary = clean_text(child_text(n, ["description", "summary", "content", "content:encoded"]))
        link = clean_text(child_text(n, ["link"]))
        if atom:
            link = child_attr(n, "link", "href") or link
        published_raw = child_text(n, ["pubDate", "published", "updated", "date"])
        published_at = parse_dt(published_raw) or retrieved_at
        if not title and not summary:
            continue
        raw_text_hash = short_hash(cfg["source_id"], title, summary, link, published_at)
        out.append({
            "source_key": source_key, "source_id": cfg["source_id"], "source": cfg["source_name"],
            "target_commodity": target_commodity, "target_keyword_query": target_keyword_query, "target_fetch_rank": target_fetch_rank,
            "title": title, "summary": summary, "link": link, "published_at": published_at,
            "published_raw": published_raw, "raw_text_hash": raw_text_hash,
            "raw_source_url": feed_url, "retrieved_at": retrieved_at,
        })
    return out


def fetch_feed(source_key: str, feed_url: str, raw_dir: Path, max_items: int, target_commodity: Optional[str] = None, target_keyword_query: Optional[str] = None, target_fetch_rank: Optional[int] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    retrieved_at = now_utc()
    try:
        xml_text = http_get_text(feed_url)
        suffix = f"_{target_commodity}" if target_commodity else ""
        raw_path = raw_dir / f"{source_key}{suffix}_rss_raw.xml"
        raw_path.write_text(xml_text, encoding="utf-8")
        items = parse_feed(xml_text, source_key, feed_url, retrieved_at, max_items, target_commodity, target_keyword_query, target_fetch_rank)
        return items, {
            "feed_name": source_key, "target_commodity": target_commodity, "target_keyword_query": target_keyword_query,
            "target_fetch_rank": target_fetch_rank, "feed_url": feed_url, "fetched": True,
            "raw_saved": str(raw_path), "parsed_count": len(items),
            "latest_published_at": max([i["published_at"] for i in items], default=None),
            "errors": [], "warnings": [],
        }
    except Exception as e:
        suffix = f"_{target_commodity}" if target_commodity else ""
        save_json(raw_dir / f"{source_key}{suffix}_fetch_error.json", {"feed_name": source_key, "target_commodity": target_commodity, "feed_url": feed_url, "error": str(e), "retrieved_at": retrieved_at})
        return [], {"feed_name": source_key, "target_commodity": target_commodity, "target_keyword_query": target_keyword_query, "target_fetch_rank": target_fetch_rank, "feed_url": feed_url, "fetched": False, "parsed_count": 0, "latest_published_at": None, "errors": [str(e)], "warnings": []}


def hits(text: str, keywords: List[str]) -> List[str]:
    t = text.lower()
    res = []
    for kw in keywords:
        k = kw.lower()
        # Word-boundary regex works for normal words but can miss terms with
        # symbols, e.g. OPEC+ or gold/silver. Use substring matching only for
        # such symbol-bearing keywords.
        if re.search(r"[^\w\s-]", k):
            if k in t:
                res.append(kw)
        elif re.search(r"\b" + re.escape(k) + r"\b", t):
            res.append(kw)
    return res


def tag_commodities(text: str) -> Tuple[List[str], List[str], Dict[str, List[str]]]:
    hitmap = {sym: hits(text, kws) for sym, kws in COMMODITY_KEYWORDS.items()}
    hitmap = {k: v for k, v in hitmap.items() if v}
    commodities = sorted(hitmap)
    groups = set()
    for c in commodities:
        groups.add("ENERGY" if c in {"CL", "NG", "HO", "RB"} else "GRAINS" if c in {"ZC", "ZS", "ZW"} else "METALS")
    if not commodities:
        return ["UNKNOWN_COMMODITY"], ["UNKNOWN_GROUP"], {}
    return commodities, sorted(groups), hitmap


def tag_event(text: str, source_key: str) -> Tuple[str, Dict[str, List[str]]]:
    hitmap = {cat: hits(text, kws) for cat, kws in EVENT_KEYWORDS.items()}
    hitmap = {k: v for k, v in hitmap.items() if v}
    if not hitmap:
        return "OTHER", {}
    priority = [
        "INVENTORY_DRAW", "INVENTORY_BUILD", "SUPPLY_DISRUPTION", "REFINERY_OUTAGE",
        "SUPPLY_INCREASE", "DEMAND_STRENGTH", "DEMAND_WEAKNESS", "EXPORT_STRENGTH",
        "EXPORT_WEAKNESS", "OPEC_POLICY", "WEATHER_RISK", "GEOPOLITICAL_RISK",
        "EIA_REPORT", "USDA_REPORT", "MACRO_POLICY",
    ]
    for p in priority:
        if p in hitmap:
            return p, hitmap
    return sorted(hitmap)[0], hitmap


def score_direction(text: str, event_category: str, commodities: Optional[List[str]] = None) -> Tuple[str, Dict[str, Any]]:
    bull = hits(text, BULLISH_HINTS)
    bear = hits(text, BEARISH_HINTS)
    event_dir = DIRECTION_BY_EVENT.get(event_category, "UNKNOWN")
    commodities = commodities or []

    if bull and bear:
        return "MIXED", {"bullish_hits": bull, "bearish_hits": bear, "event_direction": event_dir}
    if bull:
        return "BULLISH", {"bullish_hits": bull, "bearish_hits": [], "event_direction": event_dir}
    if bear:
        return "BEARISH", {"bullish_hits": [], "bearish_hits": bear, "event_direction": event_dir}
    if event_dir in {"BULLISH", "BEARISH", "MIXED"}:
        return event_dir, {"bullish_hits": [], "bearish_hits": [], "event_direction": event_dir, "reason": "Inferred from event category."}

    # v1.1: NEUTRAL means the item is clearly commodity/event relevant,
    # but no directional evidence was detected.
    if "UNKNOWN_COMMODITY" not in commodities and event_category != "OTHER":
        return "NEUTRAL", {
            "bullish_hits": [],
            "bearish_hits": [],
            "event_direction": event_dir,
            "reason": "Commodity and event category detected, but no directional evidence.",
        }

    return "UNKNOWN", {"bullish_hits": [], "bearish_hits": [], "event_direction": event_dir, "reason": "No directional evidence."}


def confidence(commodities: List[str], event_category: str, direction: str, detail: Dict[str, Any], text: str) -> Tuple[float, str]:
    s = 0.20
    if "UNKNOWN_COMMODITY" not in commodities:
        s += 0.25
        if len(commodities) == 1:
            s += 0.05
    if event_category != "OTHER":
        s += 0.20
    if direction in {"BULLISH", "BEARISH"}:
        s += 0.20
    elif direction == "MIXED":
        s += 0.10
    s += min(0.10, 0.03 * (len(detail.get("bullish_hits", [])) + len(detail.get("bearish_hits", []))))
    if len(text) < 25:
        s -= 0.10
    s = round(max(0.0, min(1.0, s)), 3)
    return s, "high" if s >= 0.75 else "medium" if s >= 0.50 else "low"


def paraphrase(headline: str, max_len: int = 180) -> str:
    s = clean_text(headline)
    s = re.sub(r"\s+-\s+Reuters.*$", "", s, flags=re.I)
    s = re.sub(r"\s+\|\s+Reuters.*$", "", s, flags=re.I)
    return s if len(s) <= max_len else s[:max_len - 3].rstrip() + "..."


def summarize(snippet: str, max_len: int = 260) -> str:
    s = clean_text(snippet)
    return s if len(s) <= max_len else s[:max_len - 3].rstrip() + "..."


def build_event(item: Dict[str, Any], gatekeeper_id: str) -> Dict[str, Any]:
    cfg = SOURCE_CONFIG[item["source_key"]]
    text = " ".join([item.get("title", ""), item.get("summary", "")]).strip()
    commodities, groups, commodity_hits = tag_commodities(text)
    event_category, event_hits = tag_event(text, item["source_key"])
    direction, detail = score_direction(text, event_category, commodities)
    score, label = confidence(commodities, event_category, direction, detail, text)
    target_commodity = item.get("target_commodity")
    primary = target_commodity if target_commodity in commodities else commodities[0]
    event_id = f"{cfg['source_id']}_{short_hash(item.get('raw_text_hash', ''), primary, event_category, direction)}"
    return {
        "event_alert_id": event_id, "source_id": cfg["source_id"], "source": cfg["source_name"],
        "target_commodity": target_commodity, "target_fetch_rank": item.get("target_fetch_rank"),
        "target_keyword_query": item.get("target_keyword_query"),
        "timestamp": item["published_at"], "published_at": item["published_at"], "data_type": "NARRATIVE_EVENT",
        "commodities": commodities, "commodity_groups": groups, "primary_commodity": primary,
        "commodity_keyword_hits": commodity_hits, "event_category": event_category, "event_keyword_hits": event_hits,
        "direction": direction, "direction_detail": detail, "confidence_score": score, "confidence_label": label,
        "confidence_method": "RULE_BASED",
        "headline_original": item.get("title", "")[:240], "headline_paraphrase": paraphrase(item.get("title", "")),
        "snippet_summary": summarize(item.get("summary", "")), "url": item.get("link"),
        "raw_text_hash": item.get("raw_text_hash"), "tos_status": cfg["tos_status"],
        "gatekeeper_cleared": cfg["gatekeeper_cleared"], "gatekeeper_id": gatekeeper_id,
        "raw_source_url": item.get("raw_source_url"), "retrieved_at": item.get("retrieved_at"),
        "normalized_at": now_utc(), "full_article_text_included": False, "data_policy": cfg["policy"],
        "source_is_proxy": cfg.get("is_proxy", False),
        "underlying_publisher": cfg.get("underlying_publisher"),
        "claude_preprocessing_payload": {
            "task": "commodity_narrative_sentiment_preprocessing",
            "commodities": commodities,
            "target_commodity": target_commodity,
            "primary_commodity": primary,
            "event_category": event_category,
            "direction": direction,
            "confidence_score": score,
            "confidence_label": label,
            "confidence_method": "RULE_BASED",
            "headline_paraphrase": paraphrase(item.get("title", "")),
            "snippet_summary": summarize(item.get("summary", "")),
            "source": cfg["source_name"],
            "underlying_publisher": cfg.get("underlying_publisher"),
            "source_is_proxy": cfg.get("is_proxy", False),
            "tos_status": cfg["tos_status"],
            "source_policy_summary": cfg["policy"],
            "do_not_quote_source_text": True,
            "full_article_text_included": False,
        },
    }


def dedup(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen, out = set(), []
    for it in items:
        base = it.get("raw_text_hash") or short_hash(it.get("title", ""), it.get("link", ""))
        # v1.3: deduplicate within each target commodity, not globally across
        # all targets. The same Reuters/EIA item can legitimately map to CL and RB.
        k = short_hash(it.get("target_commodity") or "NO_TARGET", base)
        if k not in seen:
            seen.add(k)
            out.append(it)
    return out


def filter_lookback(events: List[Dict[str, Any]], hours: int) -> List[Dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    out = []
    for e in events:
        dt = iso_to_dt(e.get("published_at"))
        if dt is None or dt >= cutoff:
            out.append(e)
    return out


def validate(events: List[Dict[str, Any]], feed_metas: List[Dict[str, Any]], latest_limit: int, enabled_feeds: List[str], min_items_per_commodity: int) -> Dict[str, Any]:
    errors, warnings = [], []
    if not any(m.get("fetched") for m in feed_metas):
        errors.append("No feed fetched successfully.")

    def feed_any_fetched(feed_name: str) -> bool:
        return any(m.get("feed_name") == feed_name and m.get("fetched") for m in feed_metas)

    def feed_any_failed(feed_name: str) -> bool:
        return any(m.get("feed_name") == feed_name and not m.get("fetched") for m in feed_metas)

    if "eia" in enabled_feeds:
        if not feed_any_fetched("eia"):
            errors.append("EIA feed is enabled but did not fetch successfully.")
        elif feed_any_failed("eia"):
            warnings.append("At least one EIA target-filter metadata entry failed; review feed_results.")
    if "reuters" in enabled_feeds:
        if not feed_any_fetched("reuters"):
            errors.append("Official Reuters feed is enabled but did not fetch successfully.")
        elif feed_any_failed("reuters"):
            warnings.append("At least one official Reuters target fetch failed; review feed_results.")
    if "reuters_proxy" in enabled_feeds:
        warnings.append("Reuters default feed is a Google News Reuters commodities proxy, not official Reuters RSS. Use --reuters-feed for official Reuters RSS if available.")
        if not feed_any_fetched("reuters_proxy"):
            errors.append("Reuters proxy feed is enabled but did not fetch successfully.")
        elif feed_any_failed("reuters_proxy"):
            warnings.append("At least one Reuters proxy target fetch failed; review feed_results.")

    allowed = {"BULLISH", "BEARISH", "MIXED", "NEUTRAL", "UNKNOWN"}
    seen = set()
    counters = {"duplicate_hashes": 0, "full_article_violations": 0, "bad_confidence": 0, "bad_direction": 0, "missing_core": 0, "reuters_tos_violations": 0, "bad_confidence_method": 0}
    for e in events:
        if not e.get("source_id") or not e.get("published_at") or not (e.get("headline_original") or e.get("snippet_summary")):
            counters["missing_core"] += 1
        h = short_hash(e.get("target_commodity") or "NO_TARGET", e.get("raw_text_hash") or "")
        if h in seen:
            counters["duplicate_hashes"] += 1
        seen.add(h)
        if e.get("full_article_text") or e.get("full_article_text_included") is True:
            counters["full_article_violations"] += 1
        c = e.get("confidence_score")
        if not isinstance(c, (int, float)) or not 0 <= c <= 1:
            counters["bad_confidence"] += 1
        if e.get("direction") not in allowed:
            counters["bad_direction"] += 1
        if e.get("source_id") in {"REUTERS_COMMODITIES_RSS", "GOOGLE_NEWS_REUTERS_COMMODITIES_PROXY"} and e.get("tos_status") != "REVIEW_PARAPHRASE_ONLY":
            counters["reuters_tos_violations"] += 1
        if e.get("confidence_method") != "RULE_BASED":
            counters["bad_confidence_method"] += 1
    for k, v in counters.items():
        if v:
            errors.append(f"{k}: {v}")
    if not events:
        warnings.append("No events normalized after lookback filter.")

    targets = sorted({m.get("target_commodity") for m in feed_metas if m.get("target_commodity")}, key=lambda x: next((m.get("target_fetch_rank") or 9999 for m in feed_metas if m.get("target_commodity") == x), 9999))
    target_coverage_summary = []
    for target in targets:
        target_metas = [m for m in feed_metas if m.get("target_commodity") == target]
        raw_matched_total = sum((m.get("target_matched_count") or 0) for m in target_metas)
        normalized_count = len([e for e in events if e.get("target_commodity") == target])
        status = "OK" if normalized_count >= min_items_per_commodity else "BELOW_TARGET_BUT_VALID"
        if normalized_count < min_items_per_commodity:
            warnings.append(
                f"Target {target} has {normalized_count} normalized events within lookback, below target minimum {min_items_per_commodity}. "
                "Kept all available real records; no backfill/duplication; pipeline continues."
            )
        target_coverage_summary.append({
            "target_commodity": target,
            "target_fetch_rank": next((m.get("target_fetch_rank") for m in target_metas if m.get("target_fetch_rank") is not None), None),
            "target_minimum": min_items_per_commodity,
            "raw_matched_total_before_dedup_lookback": raw_matched_total,
            "normalized_count_after_dedup_lookback": normalized_count,
            "coverage_status": status,
            "enabled_sources": sorted({m.get("feed_name") for m in target_metas if m.get("feed_name")}),
        })

    feed_results = []
    for m in feed_metas:
        matched_count = m.get("target_matched_count")
        target = m.get("target_commodity")
        item_warnings = list(m.get("warnings", []))
        feed_results.append({
            "feed_name": m.get("feed_name"), "target_commodity": target,
            "target_fetch_rank": m.get("target_fetch_rank"), "target_keyword_query": m.get("target_keyword_query"),
            "feed_url": m.get("feed_url"),
            "ok": bool(m.get("fetched")) and not m.get("errors"), "fetched": m.get("fetched"),
            "parsed_count": m.get("parsed_count"), "target_matched_count": matched_count,
            "normalized_count": len([e for e in events if e.get("raw_source_url") == m.get("feed_url") and (not target or e.get("target_commodity") == target)]),
            "latest_published_at": m.get("latest_published_at"), "errors": m.get("errors", []), "warnings": item_warnings,
        })
    return {
        "validated_at": now_utc(), "stage": "REUTERS_EIA_NARRATIVE_STEP10_VALIDATION_LIGHT_V1_3",
        "all_required_ok": len(errors) == 0, "record_count": len(events),
        "target_coverage_summary": target_coverage_summary,
        "feed_results": feed_results, "errors": errors, "warnings": warnings,
        "validation_rules": {
            "feeds": "At least one Reuters/EIA feed must fetch successfully.",
            "no_full_article_text": "Full article body is not fetched or stored.",
            "reuters_policy": "Reuters official/proxy records must be REVIEW_PARAPHRASE_ONLY.",
            "feed_requirements": "Enabled feeds are validated by aggregated feed status across target-level metadata.",
            "direction": "Direction must be BULLISH / BEARISH / MIXED / NEUTRAL / UNKNOWN.",
            "confidence": "confidence_score must be numeric in [0, 1].",
            "deduplication": "raw_text_hash must be unique within each target_commodity.",
            "target_first_collection": "Reuters proxy is queried one commodity at a time; target minimum is coverage guidance, not a hard validation failure.",
            "target_coverage": "Target minimum is checked only after source merge, target-aware deduplication, and lookback filtering.",
        },
    }


def latest_payload(events: List[Dict[str, Any]], limit: int) -> Dict[str, Any]:
    events = sorted(events, key=lambda e: e.get("published_at") or "", reverse=True)[:limit]
    alerts = []
    for e in events:
        alerts.append({
            "event_alert_id": e["event_alert_id"], "timestamp": e["published_at"], "source": e["source"],
            "target_commodity": e.get("target_commodity"),
            "commodities": e["commodities"], "primary_commodity": e["primary_commodity"],
            "event_category": e["event_category"], "direction": e["direction"],
            "confidence_score": e["confidence_score"], "confidence_label": e["confidence_label"],
            "confidence_method": e.get("confidence_method"),
            "short_paraphrase": e["headline_paraphrase"], "snippet_summary": e["snippet_summary"],
            "url": e["url"], "tos_status": e["tos_status"],
        })
    return {
        "source_id": "NARRATIVE_SENTIMENT_LATEST",
        "timestamp": alerts[0]["timestamp"] if alerts else now_utc(),
        "narrative_status": "GO_WITH_SOURCE_SPECIFIC_TOS",
        "latest_limit": limit, "alerts": alerts, "generated_at": now_utc(),
    }


def write_validation_txt(path: Path, report: Dict[str, Any]) -> None:
    lines = ["Reuters / EIA Narrative Sentiment Step 10 Validation Report", "=" * 88,
             f"Validated at: {report.get('validated_at')}",
             f"All required OK: {report.get('all_required_ok')}",
             f"Record count: {report.get('record_count')}", ""]
    for item in report.get("feed_results", []):
        status = "PASS" if item.get("ok") else "FAIL"
        lines.append(f"{status} | {item.get('feed_name'):<8} | fetched={item.get('fetched')} | parsed={item.get('parsed_count')} | normalized={item.get('normalized_count')} | latest={item.get('latest_published_at')}")
        if item.get("errors"):
            lines.append(f"      errors={item.get('errors')}")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_feed_outputs(out_dir: Path, source_key: str, events: List[Dict[str, Any]], validation_report: Dict[str, Any]) -> None:
    d = out_dir / "feeds" / source_key
    ensure_dir(d)
    save_json(d / f"{source_key}_narrative_events.json", {"generated_at": now_utc(), "source_key": source_key, "records": events})
    write_jsonl(d / f"{source_key}_narrative_events.jsonl", events)
    write_csv(d / f"{source_key}_narrative_events.csv", events)
    result = next((x for x in validation_report.get("feed_results", []) if x.get("feed_name") == source_key), {})
    save_json(d / f"{source_key}_validation_report.json", result)


def main() -> int:
    require_dependencies()
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    p.add_argument("--lookback-hours", type=int, default=DEFAULT_LOOKBACK_HOURS)
    p.add_argument("--max-items-per-feed", type=int, default=DEFAULT_MAX_ITEMS_PER_FEED)
    p.add_argument("--min-items-per-commodity", type=int, default=DEFAULT_MIN_ITEMS_PER_COMMODITY)
    p.add_argument("--latest-limit", type=int, default=DEFAULT_LATEST_LIMIT)
    p.add_argument("--commodities", default=",".join(DEFAULT_COMMODITY_ORDER), help="Comma/space separated commodity symbols, processed sequentially. Example: CL,NG,HO,RB")
    p.add_argument("--reuters-feed", default=None, help="Official Reuters RSS/feed URL. If omitted, local light uses target-first Google News Reuters commodities proxy.")
    p.add_argument("--eia-feed", default=DEFAULT_EIA_FEED)
    p.add_argument("--disable-reuters", action="store_true")
    p.add_argument("--disable-eia", action="store_true")
    p.add_argument("--gatekeeper-id", default=DEFAULT_GATEKEEPER_ID)
    args = p.parse_args()

    try:
        commodity_order = parse_commodity_order(args.commodities)
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 2

    out_dir = Path(args.out_dir)
    raw_dir = out_dir / "raw"
    combined_dir = out_dir / "combined"
    ensure_dir(raw_dir); ensure_dir(combined_dir); ensure_dir(out_dir / "feeds")

    feed_plan = []
    if not args.disable_reuters:
        feed_plan.append("reuters" if args.reuters_feed else "reuters_proxy")
    if not args.disable_eia:
        feed_plan.append("eia")
    enabled_feed_keys = sorted(set(feed_plan))

    print("=" * 88)
    print("Reuters / EIA Narrative Sentiment LIGHT — Step 10 v1.3")
    print("=" * 88)
    print(f"Output directory:          {out_dir}")
    print(f"Lookback hours:            {args.lookback_hours} ({round(args.lookback_hours / 24, 2)} days)")
    print(f"Max items per target feed: {args.max_items_per_feed}")
    print(f"Min items per commodity:   {args.min_items_per_commodity}")
    print(f"Latest alert limit:        {args.latest_limit}")
    print(f"Commodity order:           {', '.join(commodity_order)}")
    print(f"Reuters enabled:           {not args.disable_reuters}")
    print(f"Reuters mode:              {'OFFICIAL_RSS_FILTERED_BY_TARGET' if args.reuters_feed else 'GOOGLE_NEWS_REUTERS_COMMODITIES_PROXY_TARGET_FIRST' if not args.disable_reuters else 'DISABLED'}")
    print(f"EIA enabled:               {not args.disable_eia}")
    print("Full article fetch:        NO")
    print("Reuters mode:              REVIEW_PARAPHRASE_ONLY")

    all_items, metas = [], []
    eia_cache: Optional[Tuple[List[Dict[str, Any]], Dict[str, Any]]] = None

    def get_eia_cache() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        nonlocal eia_cache
        if eia_cache is None:
            print("\n" + "-" * 88)
            print(f"[FEED CACHE] EIA | {args.eia_feed}")
            print("-" * 88)
            items, meta = fetch_feed(
                "eia",
                args.eia_feed,
                raw_dir,
                args.max_items_per_feed,
                target_commodity=None,
                target_keyword_query="EIA Today in Energy base RSS cache",
                target_fetch_rank=None,
            )
            status = "PASS" if meta.get("fetched") and not meta.get("errors") else "FAIL"
            print(f"{status} | parsed={meta.get('parsed_count')} | latest={meta.get('latest_published_at')}")
            if meta.get("errors"):
                print(f"      errors={meta.get('errors')}")
            eia_cache = (items, meta)
        return eia_cache

    def collect_target(source_key: str, symbol: str, rank: int, feed_url: str, query: Optional[str]) -> None:
        print("\n" + "-" * 88)
        print(f"[TARGET {rank:02d}] {symbol} | {source_key.upper()} | {feed_url}")
        print("-" * 88)
        if source_key == "eia":
            cached_items, cached_meta = get_eia_cache()
            items = []
            for it in cached_items:
                cloned = dict(it)
                cloned["target_commodity"] = symbol
                cloned["target_keyword_query"] = query
                cloned["target_fetch_rank"] = rank
                items.append(cloned)
            meta = dict(cached_meta)
            meta.update({
                "target_commodity": symbol,
                "target_keyword_query": query,
                "target_fetch_rank": rank,
            })
        else:
            items, meta = fetch_feed(
                source_key,
                feed_url,
                raw_dir,
                args.max_items_per_feed,
                target_commodity=symbol,
                target_keyword_query=query,
                target_fetch_rank=rank,
            )
        matched = [it for it in items if item_mentions_commodity(it, symbol)]
        meta["target_matched_count"] = len(matched)
        all_items.extend(matched)
        metas.append(meta)
        status = "PASS" if meta.get("fetched") and not meta.get("errors") else "FAIL"
        print(
            f"{status} | parsed={meta.get('parsed_count')} | matched_target={len(matched)} "
            f"| target_min={args.min_items_per_commodity} | latest={meta.get('latest_published_at')}"
        )
        if meta.get("warnings"):
            print(f"      warnings={meta.get('warnings')}")
        if meta.get("errors"):
            print(f"      errors={meta.get('errors')}")

    for rank, symbol in enumerate(commodity_order, start=1):
        if not args.disable_reuters:
            if args.reuters_feed:
                collect_target("reuters", symbol, rank, args.reuters_feed, f"official Reuters feed filtered by {symbol} keywords")
            else:
                query = REUTERS_PROXY_COMMODITY_QUERIES.get(symbol, f"Reuters commodities {symbol}")
                collect_target("reuters_proxy", symbol, rank, build_reuters_proxy_feed_for_commodity(symbol), query)

        if not args.disable_eia:
            # EIA Today in Energy RSS is not a search endpoint. v1.3 fetches it
            # once and reuses the parsed items for each target keyword filter.
            collect_target("eia", symbol, rank, args.eia_feed, f"EIA Today in Energy filtered by {symbol} keywords")

    unique_items = dedup(all_items)
    events = [build_event(it, args.gatekeeper_id) for it in unique_items]
    events = filter_lookback(events, args.lookback_hours)
    events.sort(key=lambda e: e.get("published_at") or "", reverse=True)
    latest = latest_payload(events, args.latest_limit)
    report = validate(events, metas, args.latest_limit, enabled_feed_keys, args.min_items_per_commodity)

    save_json(combined_dir / "narrative_events_normalized.json", {
        "generated_at": now_utc(), "source": "REUTERS_EIA_NARRATIVE_SENTIMENT",
        "stage": "step10_reuters_eia_narrative_light_v1_3", "target_first": True,
        "commodity_order": commodity_order,
        "min_items_per_commodity": args.min_items_per_commodity,
        "lookback_hours": args.lookback_hours,
        "records": events, "latest": latest,
    })
    write_jsonl(combined_dir / "narrative_events_normalized.jsonl", events)
    write_csv(combined_dir / "narrative_events_normalized.csv", events)
    save_json(combined_dir / "narrative_events_latest.json", latest)
    write_csv(combined_dir / "narrative_events_latest.csv", latest.get("alerts", []))
    save_json(combined_dir / "narrative_validation_report.json", report)
    write_validation_txt(combined_dir / "narrative_validation_report.txt", report)

    for source_key in enabled_feed_keys:
        source_events = [e for e in events if e.get("source_id") == SOURCE_CONFIG[source_key]["source_id"]]
        write_feed_outputs(out_dir, source_key, source_events, report)

    print("\n" + "=" * 88)
    print("STEP 10 NARRATIVE SENTIMENT SUMMARY")
    print("=" * 88)
    print(f"Parsed raw items:       {sum(m.get('parsed_count') or 0 for m in metas)}")
    print(f"Target-matched items:   {len(all_items)}")
    print(f"After deduplication:    {len(unique_items)}")
    print(f"Normalized events:      {len(events)}")
    print(f"Latest alerts:          {len(latest.get('alerts', []))}")
    print(f"Validation:             {'PASS' if report['all_required_ok'] else 'FAIL'}")
    if report.get("errors"):
        print(f"Validation errors:      {report.get('errors')}")
    if report.get("warnings"):
        print(f"Validation warnings:    {report.get('warnings')}")

    print("\nSaved combined outputs:")
    for name in [
        "narrative_events_normalized.json", "narrative_events_normalized.jsonl",
        "narrative_events_normalized.csv", "narrative_events_latest.json",
        "narrative_events_latest.csv", "narrative_validation_report.json",
        "narrative_validation_report.txt",
    ]:
        print(combined_dir / name)
    print("\nSaved per-feed outputs under:")
    print(out_dir / "feeds")
    print("\nSaved raw feeds under:")
    print(raw_dir)

    if report["all_required_ok"]:
        print("\n[DONE] Step 10 Reuters/EIA narrative sentiment layer completed and validated.")
        return 0
    print("\n[DONE WITH FAILURES] Review validation report.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
