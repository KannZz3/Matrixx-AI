#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
CFTC COT Step 6 Fetcher LIGHT v4 CL AUDIT FIXED

Purpose
-------
Fetch CFTC COT data required for Step 6 positioning/scoring layer.

Targets:
    CL, NG, ZC, ZS, ZW, GC, SI

Fixes vs v2:
1. NG selection: strongly prefers the current NAT GAS NYME / Henry Hub style Natural Gas market instead of stale NATURAL GAS NYMEX series.
2. CL selection: prefers current NYMEX WTI when available; otherwise allows current ICE WTI and labels it as PROXY.
3. Validation freshness check: latest report dates must be close to the freshest COT date across symbols, so stale 2022 series will fail validation.
4. CL market audit: writes candidate market diagnostics and hard-prefers fresh NYMEX WTI before falling back to ICE WTI proxy.
5. Producer/Merchant dynamic field mapping and commodity-split outputs remain from v2.

Data sources:
- CFTC Disaggregated Futures Only: 72hh-3qpy
- CFTC Legacy Futures Only:        6dca-aqww

Run:
    python cftc_cot_step6_fetcher_light_v4_cl_audit_fixed.py --limit-per-symbol 156 --query-limit 5000

Optional app token:
    $env:SOCRATA_APP_TOKEN="YOUR_TOKEN"
"""

import argparse
import csv
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


DOMAIN = "https://publicreporting.cftc.gov"
DISAGG_DATASET = "72hh-3qpy"
LEGACY_DATASET = "6dca-aqww"


TARGETS: Dict[str, Dict[str, Any]] = {
    "CL": {
        "instrument": "CL",
        "label": "WTI Light Sweet Crude Oil",
        "asset_class": "ENERGY",
        "query_terms": ["CRUDE OIL", "LIGHT SWEET", "WTI"],
        "prefer_terms": [
            "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
            "CRUDE OIL", "LIGHT SWEET", "WTI", "NEW YORK MERCANTILE EXCHANGE", "NYMEX"
        ],
        "acceptable_proxy_terms": ["CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE", "ICE FUTURES EUROPE"],
        "avoid_terms": ["BRENT", "DUBAI", "ARGUS", "CALIFORNIA", "MINI", "E-MINI", "MICRO"],
    },
    "NG": {
        "instrument": "NG",
        "label": "Natural Gas",
        "asset_class": "ENERGY",
        "query_terms": ["NATURAL GAS", "NAT GAS", "HENRY HUB"],
        "prefer_terms": [
            "NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE",
            "NAT GAS NYME",
            "HENRY HUB",
            "NATURAL GAS",
            "NEW YORK MERCANTILE EXCHANGE",
            "NYMEX"
        ],
        "avoid_terms": ["E-MINI", "MINI", "BASIS", "PENULTIMATE", "LOOK-ALIKE", "ICE", "FINANCIAL", "INDEX"],
    },
    "ZC": {
        "instrument": "ZC",
        "label": "Corn",
        "asset_class": "GRAIN",
        "query_terms": ["CORN"],
        "prefer_terms": ["CORN - CHICAGO BOARD OF TRADE", "CORN", "CHICAGO BOARD OF TRADE", "CBOT"],
        "avoid_terms": ["MINI"],
    },
    "ZS": {
        "instrument": "ZS",
        "label": "Soybeans",
        "asset_class": "GRAIN",
        "query_terms": ["SOYBEANS"],
        "prefer_terms": ["SOYBEANS - CHICAGO BOARD OF TRADE", "SOYBEANS", "CHICAGO BOARD OF TRADE", "CBOT"],
        "avoid_terms": ["MINI", "SOYBEAN MEAL", "SOYBEAN OIL"],
    },
    "ZW": {
        "instrument": "ZW",
        "label": "SRW Wheat",
        "asset_class": "GRAIN",
        "query_terms": ["WHEAT"],
        "prefer_terms": ["WHEAT-SRW - CHICAGO BOARD OF TRADE", "WHEAT-SRW", "CHICAGO BOARD OF TRADE", "CBOT"],
        "avoid_terms": ["WHEAT-HRW", "HARD RED", "MINNEAPOLIS", "MGE", "KC HRW", "KANSAS"],
    },
    "GC": {
        "instrument": "GC",
        "label": "Gold",
        "asset_class": "METAL",
        "query_terms": ["GOLD"],
        "prefer_terms": ["GOLD - COMMODITY EXCHANGE INC.", "GOLD", "COMMODITY EXCHANGE", "COMEX"],
        "avoid_terms": ["MINI", "MICRO"],
    },
    "SI": {
        "instrument": "SI",
        "label": "Silver",
        "asset_class": "METAL",
        "query_terms": ["SILVER"],
        "prefer_terms": ["SILVER - COMMODITY EXCHANGE INC.", "SILVER", "COMMODITY EXCHANGE", "COMEX"],
        "avoid_terms": ["MINI", "MICRO"],
    },
}


DATE_FIELDS = ["report_date_as_yyyy_mm_dd", "report_date", "as_of_date_in_form_yyyy_mm_dd"]
MARKET_FIELDS = ["market_and_exchange_names", "market_and_exchange_name"]
CONTRACT_CODE_FIELDS = ["cftc_contract_market_code", "cftc_market_code", "commodity_code"]
OPEN_INTEREST_FIELDS = ["open_interest_all", "open_interest"]

REQUIRED_COMMON_FIELDS = [
    "source_id", "instrument", "commodity_label", "report_date", "report_type",
    "market_and_exchange_name", "open_interest", "raw_source_url",
    "tos_status", "gatekeeper_cleared", "gatekeeper_id",
]


def norm_text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def lower(x: Any) -> str:
    return norm_text(x).lower()


def parse_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        s = str(x).strip().replace(",", "").replace("%", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def parse_date(s: Any) -> datetime:
    txt = norm_text(s)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(txt[:19] if "T" in txt else txt[:10], fmt)
        except Exception:
            pass
    return datetime.min


def get_first(row: Dict[str, Any], names: List[str]) -> Any:
    for name in names:
        if name in row:
            return row[name]
    lower_map = {str(k).lower(): k for k in row.keys()}
    for name in names:
        k = lower_map.get(name.lower())
        if k is not None:
            return row[k]
    return None


def find_numeric_by_key_patterns(
    row: Dict[str, Any],
    required_any_groups: List[List[str]],
    required_all: List[str],
    forbidden: List[str] = None,
    prefer_all: List[str] = None,
) -> Optional[float]:
    """
    Robust dynamic CFTC column finder.
    required_any_groups: each group means at least one of its tokens must appear in key.
    required_all: all tokens must appear in key.
    forbidden: reject keys with any token.
    prefer_all: extra priority when all appear.
    """
    forbidden = forbidden or []
    prefer_all = prefer_all or []

    candidates = []
    for key, value in row.items():
        k = str(key).lower()

        if any(f in k for f in forbidden):
            continue
        if any(tok not in k for tok in required_all):
            continue

        ok = True
        for group in required_any_groups:
            if not any(tok in k for tok in group):
                ok = False
                break
        if not ok:
            continue

        val = parse_float(value)
        if val is None:
            continue

        score = 0
        if "all" in k:
            score += 20
        if "positions" in k:
            score += 10
        if all(tok in k for tok in prefer_all):
            score += 20
        if "old" in k or "other" in k:
            score -= 10

        candidates.append((score, key, val))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][2]


def open_interest(row: Dict[str, Any]) -> Optional[float]:
    direct = parse_float(get_first(row, OPEN_INTEREST_FIELDS))
    if direct is not None:
        return direct
    return find_numeric_by_key_patterns(row, [], ["open", "interest"], forbidden=["change", "percent", "traders"])


def disagg_value(row: Dict[str, Any], group: str, side: str) -> Optional[float]:
    side = side.lower()

    if group == "producer_merchant":
        return find_numeric_by_key_patterns(
            row,
            required_any_groups=[["prod", "producer"], ["merc", "merchant"]],
            required_all=[side],
            forbidden=["traders", "change", "percent", "pct", "concentration"],
            prefer_all=["positions"],
        )

    if group == "managed_money":
        return find_numeric_by_key_patterns(
            row,
            required_any_groups=[["m_money", "managed"]],
            required_all=[side],
            forbidden=["traders", "change", "percent", "pct", "concentration"],
            prefer_all=["positions"],
        )

    if group == "swap":
        return find_numeric_by_key_patterns(row, [["swap"]], [side], forbidden=["traders", "change", "percent", "pct"])

    if group == "other_reportable":
        return find_numeric_by_key_patterns(row, [["other"]], [side], forbidden=["traders", "change", "percent", "pct", "nonrept", "nonreportable"])

    if group == "nonreportable":
        return find_numeric_by_key_patterns(row, [["nonrept", "nonreportable"]], [side], forbidden=["traders", "change", "percent", "pct"])

    return None


def legacy_value(row: Dict[str, Any], group: str, side: str) -> Optional[float]:
    side = side.lower()
    if group == "commercial":
        return find_numeric_by_key_patterns(row, [["commercial", "comm"]], [side], forbidden=["noncommercial", "noncomm", "traders", "change", "percent", "pct"])
    if group == "noncommercial":
        return find_numeric_by_key_patterns(row, [["noncommercial", "noncomm"]], [side], forbidden=["traders", "change", "percent", "pct"])
    if group == "nonreportable":
        return find_numeric_by_key_patterns(row, [["nonrept", "nonreportable"]], [side], forbidden=["traders", "change", "percent", "pct"])
    return None


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b


def percentile_rank(values: List[Optional[float]], latest: Optional[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if latest is None or not vals:
        return None
    return sum(1 for v in vals if v <= latest) / len(vals)


def zscore(values: List[Optional[float]], latest: Optional[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if latest is None or len(vals) < 2:
        return None
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)
    sd = math.sqrt(var)
    if sd == 0:
        return None
    return (latest - mean) / sd


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    preferred = [
        "instrument", "commodity_label", "asset_class", "report_date", "report_type",
        "market_and_exchange_name", "open_interest",
        "producer_merchant_long", "producer_merchant_short", "producer_merchant_net",
        "managed_money_long", "managed_money_short", "managed_money_net",
        "managed_money_net_chg_1w", "managed_money_net_pct_oi",
        "producer_merchant_net_pct_oi", "managed_money_net_percentile",
        "commercial_long", "commercial_short", "commercial_net",
        "noncommercial_long", "noncommercial_short", "noncommercial_net",
        "legacy_noncommercial_net_percentile",
        "source_id", "raw_source_url", "tos_status", "gatekeeper_cleared", "gatekeeper_id",
    ]
    keys = []
    for k in preferred:
        if any(k in r for r in rows):
            keys.append(k)
    for row in rows:
        for k in row.keys():
            if k not in keys:
                keys.append(k)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in keys})


def write_generic_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in keys})


def request_json(url: str, params: Dict[str, Any], app_token: str = "", retries: int = 3) -> Any:
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token
    last_error = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=90)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.0 * (2 ** attempt))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_error = exc
            time.sleep(1.0 * (2 ** attempt))
    raise RuntimeError(f"GET failed: {url} params={params} error={last_error}")


def get_metadata(dataset_id: str, app_token: str) -> Dict[str, Any]:
    return request_json(f"{DOMAIN}/api/views/{dataset_id}.json", {}, app_token=app_token)


def build_where(term: str) -> str:
    safe = term.replace("'", "''")
    return f"upper(market_and_exchange_names) like '%{safe.upper()}%'"


def fetch_candidate_rows(dataset_id: str, symbol: str, cfg: Dict[str, Any], app_token: str, limit: int) -> List[Dict[str, Any]]:
    url = f"{DOMAIN}/resource/{dataset_id}.json"
    all_rows: List[Dict[str, Any]] = []

    for term in cfg["query_terms"]:
        params = {
            "$limit": limit,
            "$order": "report_date_as_yyyy_mm_dd DESC",
            "$where": build_where(term),
        }
        try:
            rows = request_json(url, params, app_token=app_token)
            if isinstance(rows, list):
                all_rows.extend(rows)
        except Exception as exc:
            print(f"  [WARN] {symbol} query term={term} failed dataset={dataset_id}: {exc}")

    seen = set()
    dedup = []
    for row in all_rows:
        key = (get_first(row, DATE_FIELDS), get_first(row, MARKET_FIELDS), get_first(row, CONTRACT_CODE_FIELDS))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(row)
    return dedup


def score_market_name(market: str, cfg: Dict[str, Any]) -> int:
    txt = lower(market)
    score = 0

    # Exact / specific preferred market names should dominate generic matches.
    for idx, t in enumerate(cfg.get("prefer_terms", [])):
        if lower(t) in txt:
            score += 60 if idx == 0 else 25

    for t in cfg.get("query_terms", []):
        if lower(t) in txt:
            score += 8

    # Acceptable proxy terms add score, but less than strict preferred terms.
    for t in cfg.get("acceptable_proxy_terms", []):
        if lower(t) in txt:
            score += 20

    for t in cfg.get("avoid_terms", []):
        if lower(t) in txt:
            score -= 120

    # Heavy penalty for mini/e-mini/micro unless explicitly wanted.
    if "mini" in txt or "e-mini" in txt or "micro" in txt:
        score -= 150

    return score


def is_cl_strict_nymex_wti_market(market: str) -> bool:
    m = lower(market)
    return (
        ("new york mercantile exchange" in m or "nymex" in m)
        and ("crude oil" in m or "light sweet" in m or "wti" in m)
        and "brent" not in m
        and "mini" not in m
        and "e-mini" not in m
        and "micro" not in m
    )


def is_cl_ice_wti_proxy_market(market: str) -> bool:
    m = lower(market)
    return (
        "ice futures europe" in m
        and ("wti" in m or "light sweet" in m or "crude oil" in m)
        and "brent" not in m
    )


def select_best_market_rows(rows: List[Dict[str, Any]], cfg: Dict[str, Any], max_rows: int) -> Tuple[List[Dict[str, Any]], Optional[str], int]:
    """
    Freshness-aware market selector.

    For CL:
      1. Hard-prefer fresh NYMEX WTI / Light Sweet Crude if available.
      2. If no fresh strict NYMEX candidate exists, use fresh ICE WTI as explicit proxy.
      3. Store full candidate audit in cfg["_last_market_audit"].

    For other symbols:
      use v3 freshness-aware ranking.
    """
    if not rows:
        cfg["_last_market_audit"] = []
        return [], None, 0

    by_market: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        market = norm_text(get_first(row, MARKET_FIELDS))
        if market:
            by_market.setdefault(market, []).append(row)

    freshest_date = datetime.min
    for market, mrows in by_market.items():
        mrows.sort(key=lambda r: parse_date(get_first(r, DATE_FIELDS)), reverse=True)
        latest = parse_date(get_first(mrows[0], DATE_FIELDS))
        freshest_date = max(freshest_date, latest)

    scored = []
    for market, mrows in by_market.items():
        mrows.sort(key=lambda r: parse_date(get_first(r, DATE_FIELDS)), reverse=True)
        latest = parse_date(get_first(mrows[0], DATE_FIELDS))
        count = len(mrows)
        name_score = score_market_name(market, cfg)

        days_behind = (freshest_date - latest).days if freshest_date != datetime.min else 0
        freshness_score = max(0, 300 - days_behind * 10)
        enough_history = count >= min(52, max_rows // 2)
        history_score = 80 if enough_history else -150
        stale_penalty = -1000 if days_behind > 45 else 0

        cl_strict_nymex = is_cl_strict_nymex_wti_market(market)
        cl_ice_proxy = is_cl_ice_wti_proxy_market(market)

        rank_score = name_score + freshness_score + history_score + stale_penalty

        scored.append({
            "market": market,
            "latest": latest,
            "latest_date": latest.date().isoformat() if latest != datetime.min else None,
            "count": count,
            "name_score": name_score,
            "freshness_score": freshness_score,
            "history_score": history_score,
            "rank_score": rank_score,
            "days_behind": days_behind,
            "is_cl_strict_nymex_wti": cl_strict_nymex,
            "is_cl_ice_wti_proxy": cl_ice_proxy,
            "rows": mrows,
        })

    instrument = cfg.get("instrument", "")

    if instrument == "CL":
        # Fresh strict NYMEX wins if it exists.
        strict = [
            x for x in scored
            if x["is_cl_strict_nymex_wti"]
            and x["days_behind"] <= 21
            and x["count"] >= min(52, max_rows // 2)
        ]
        if strict:
            strict.sort(key=lambda x: (x["rank_score"], x["latest"], x["count"]), reverse=True)
            best = strict[0]
            best["selection_reason"] = "STRICT_NYMEX_WTI_SELECTED"
        else:
            proxy = [
                x for x in scored
                if x["is_cl_ice_wti_proxy"]
                and x["days_behind"] <= 21
                and x["count"] >= min(52, max_rows // 2)
            ]
            if proxy:
                proxy.sort(key=lambda x: (x["rank_score"], x["latest"], x["count"]), reverse=True)
                best = proxy[0]
                best["selection_reason"] = "ICE_WTI_PROXY_SELECTED_NO_FRESH_NYMEX"
            else:
                scored.sort(key=lambda x: (x["rank_score"], x["latest"], x["count"]), reverse=True)
                best = scored[0]
                best["selection_reason"] = "BEST_AVAILABLE_SELECTED_NO_STRICT_OR_PROXY"
    else:
        scored.sort(key=lambda x: (x["rank_score"], x["latest"], x["count"]), reverse=True)
        best = scored[0]
        best["selection_reason"] = "FRESHNESS_RANKED_SELECTED"

    # Store audit without row payloads.
    audit = []
    for x in sorted(scored, key=lambda y: (y["rank_score"], y["latest"], y["count"]), reverse=True):
        audit.append({
            "market": x["market"],
            "latest_date": x["latest_date"],
            "count": x["count"],
            "days_behind": x["days_behind"],
            "name_score": x["name_score"],
            "freshness_score": x["freshness_score"],
            "history_score": x["history_score"],
            "rank_score": x["rank_score"],
            "is_cl_strict_nymex_wti": x["is_cl_strict_nymex_wti"],
            "is_cl_ice_wti_proxy": x["is_cl_ice_wti_proxy"],
            "selected": x["market"] == best["market"],
            "selection_reason": best.get("selection_reason") if x["market"] == best["market"] else None,
        })
    cfg["_last_market_audit"] = audit

    return best["rows"][:max_rows], best["market"], best["rank_score"]

def source_mapping_status_for_market(symbol: str, market: str) -> Tuple[str, Optional[str]]:
    """
    For CL only: prefer NYMEX WTI. If current NYMEX WTI is unavailable and ICE WTI is selected,
    disclose it as a proxy instead of silently treating it as strict NYMEX CL.
    """
    m = lower(market)
    if symbol == "CL" and "ice futures europe" in m:
        return "PROXY", "NYMEX WTI CL COT was not selected as current/fresh; using ICE WTI COT as proxy."
    return "STRICT", None

def normalize_disagg_row(row: Dict[str, Any], symbol: str, cfg: Dict[str, Any], gatekeeper_id: str) -> Dict[str, Any]:
    report_date = norm_text(get_first(row, DATE_FIELDS))[:10]
    market = norm_text(get_first(row, MARKET_FIELDS))
    oi = open_interest(row)

    pm_long = disagg_value(row, "producer_merchant", "long")
    pm_short = disagg_value(row, "producer_merchant", "short")
    mm_long = disagg_value(row, "managed_money", "long")
    mm_short = disagg_value(row, "managed_money", "short")

    pm_net = pm_long - pm_short if pm_long is not None and pm_short is not None else None
    mm_net = mm_long - mm_short if mm_long is not None and mm_short is not None else None

    return {
        "source_id": "CFTC_DISAGG_FUTURES_ONLY",
        "instrument": symbol,
        "commodity_label": cfg["label"],
        "asset_class": cfg["asset_class"],
        "report_date": report_date,
        "report_type": "DISAGGREGATED_FUTURES_ONLY",
        "market_and_exchange_name": market,
        "cftc_contract_market_code": norm_text(get_first(row, CONTRACT_CODE_FIELDS)),
        "open_interest": oi,

        "producer_merchant_long": pm_long,
        "producer_merchant_short": pm_short,
        "producer_merchant_net": pm_net,
        "producer_merchant_net_pct_oi": safe_div(pm_net, oi),

        "managed_money_long": mm_long,
        "managed_money_short": mm_short,
        "managed_money_net": mm_net,
        "managed_money_net_pct_oi": safe_div(mm_net, oi),
        "managed_money_spreading": disagg_value(row, "managed_money", "spread"),

        "swap_long": disagg_value(row, "swap", "long"),
        "swap_short": disagg_value(row, "swap", "short"),
        "other_reportable_long": disagg_value(row, "other_reportable", "long"),
        "other_reportable_short": disagg_value(row, "other_reportable", "short"),
        "nonreportable_long": disagg_value(row, "nonreportable", "long"),
        "nonreportable_short": disagg_value(row, "nonreportable", "short"),

        "tos_status": "GO",
        "gatekeeper_cleared": True,
        "gatekeeper_id": gatekeeper_id,
        "raw_source_url": f"{DOMAIN}/resource/{DISAGG_DATASET}.json",
        "source_mapping_status": source_mapping_status_for_market(symbol, market)[0],
        "source_mapping_note": source_mapping_status_for_market(symbol, market)[1],
        "normalized_at": now_utc(),
    }


def normalize_legacy_row(row: Dict[str, Any], symbol: str, cfg: Dict[str, Any], gatekeeper_id: str) -> Dict[str, Any]:
    report_date = norm_text(get_first(row, DATE_FIELDS))[:10]
    market = norm_text(get_first(row, MARKET_FIELDS))
    oi = open_interest(row)

    comm_long = legacy_value(row, "commercial", "long")
    comm_short = legacy_value(row, "commercial", "short")
    noncomm_long = legacy_value(row, "noncommercial", "long")
    noncomm_short = legacy_value(row, "noncommercial", "short")

    comm_net = comm_long - comm_short if comm_long is not None and comm_short is not None else None
    noncomm_net = noncomm_long - noncomm_short if noncomm_long is not None and noncomm_short is not None else None

    return {
        "source_id": "CFTC_LEGACY_FUTURES_ONLY",
        "instrument": symbol,
        "commodity_label": cfg["label"],
        "asset_class": cfg["asset_class"],
        "report_date": report_date,
        "report_type": "LEGACY_FUTURES_ONLY",
        "market_and_exchange_name": market,
        "cftc_contract_market_code": norm_text(get_first(row, CONTRACT_CODE_FIELDS)),
        "open_interest": oi,

        "commercial_long": comm_long,
        "commercial_short": comm_short,
        "commercial_net": comm_net,
        "commercial_net_pct_oi": safe_div(comm_net, oi),

        "noncommercial_long": noncomm_long,
        "noncommercial_short": noncomm_short,
        "noncommercial_net": noncomm_net,
        "noncommercial_net_pct_oi": safe_div(noncomm_net, oi),
        "noncommercial_spreading": legacy_value(row, "noncommercial", "spread"),

        "nonreportable_long": legacy_value(row, "nonreportable", "long"),
        "nonreportable_short": legacy_value(row, "nonreportable", "short"),

        "tos_status": "GO",
        "gatekeeper_cleared": True,
        "gatekeeper_id": gatekeeper_id,
        "raw_source_url": f"{DOMAIN}/resource/{LEGACY_DATASET}.json",
        "source_mapping_status": source_mapping_status_for_market(symbol, market)[0],
        "source_mapping_note": source_mapping_status_for_market(symbol, market)[1],
        "normalized_at": now_utc(),
    }


def add_time_series_features(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for rec in records:
        groups.setdefault((rec["instrument"], rec["report_type"]), []).append(rec)

    for _, group in groups.items():
        group.sort(key=lambda r: parse_date(r.get("report_date")))

        for i, rec in enumerate(group):
            prev = group[i - 1] if i > 0 else None

            if rec["report_type"] == "DISAGGREGATED_FUTURES_ONLY":
                mm_net = rec.get("managed_money_net")
                pm_net = rec.get("producer_merchant_net")
                rec["managed_money_net_chg_1w"] = None if prev is None or mm_net is None or prev.get("managed_money_net") is None else mm_net - prev.get("managed_money_net")
                rec["producer_merchant_net_chg_1w"] = None if prev is None or pm_net is None or prev.get("producer_merchant_net") is None else pm_net - prev.get("producer_merchant_net")
                rec["managed_money_net_percentile"] = percentile_rank([r.get("managed_money_net") for r in group[: i + 1]], mm_net)
                rec["producer_merchant_net_percentile"] = percentile_rank([r.get("producer_merchant_net") for r in group[: i + 1]], pm_net)
                rec["managed_money_net_zscore"] = zscore([r.get("managed_money_net") for r in group[: i + 1]], mm_net)
                rec["producer_merchant_net_zscore"] = zscore([r.get("producer_merchant_net") for r in group[: i + 1]], pm_net)

            elif rec["report_type"] == "LEGACY_FUTURES_ONLY":
                nc_net = rec.get("noncommercial_net")
                comm_net = rec.get("commercial_net")
                rec["noncommercial_net_chg_1w"] = None if prev is None or nc_net is None or prev.get("noncommercial_net") is None else nc_net - prev.get("noncommercial_net")
                rec["commercial_net_chg_1w"] = None if prev is None or comm_net is None or prev.get("commercial_net") is None else comm_net - prev.get("commercial_net")
                rec["legacy_noncommercial_net_percentile"] = percentile_rank([r.get("noncommercial_net") for r in group[: i + 1]], nc_net)
                rec["legacy_commercial_net_percentile"] = percentile_rank([r.get("commercial_net") for r in group[: i + 1]], comm_net)
                rec["legacy_noncommercial_net_zscore"] = zscore([r.get("noncommercial_net") for r in group[: i + 1]], nc_net)
                rec["legacy_commercial_net_zscore"] = zscore([r.get("commercial_net") for r in group[: i + 1]], comm_net)

    records.sort(key=lambda r: (r["instrument"], r["report_type"], parse_date(r["report_date"])))
    return records


def validate_records(records: List[Dict[str, Any]], per_symbol: Dict[str, Dict[str, Any]], limit: int, max_stale_days: int = 21) -> Dict[str, Any]:
    # Reference date = freshest available disaggregated/legacy report date among all selected records.
    all_dates = [parse_date(r.get("report_date")) for r in records if parse_date(r.get("report_date")) != datetime.min]
    reference_latest = max(all_dates) if all_dates else datetime.min

    results = []
    for symbol in TARGETS:
        group = [r for r in records if r.get("instrument") == symbol]
        disagg = [r for r in group if r.get("report_type") == "DISAGGREGATED_FUTURES_ONLY"]
        legacy = [r for r in group if r.get("report_type") == "LEGACY_FUTURES_ONLY"]

        errors = []
        warnings = []

        if not disagg:
            errors.append("Missing disaggregated COT records.")
        if not legacy:
            errors.append("Missing legacy COT records.")
        if len(disagg) > limit:
            errors.append(f"Disaggregated record count exceeds limit={limit}.")
        if len(legacy) > limit:
            errors.append(f"Legacy record count exceeds limit={limit}.")
        if len(disagg) < min(52, limit):
            warnings.append(f"Disaggregated history is short: {len(disagg)} rows.")
        if len(legacy) < min(52, limit):
            warnings.append(f"Legacy history is short: {len(legacy)} rows.")

        latest_disagg = max(disagg, key=lambda r: parse_date(r["report_date"])) if disagg else None
        latest_legacy = max(legacy, key=lambda r: parse_date(r["report_date"])) if legacy else None

        # Freshness check: stale 2022-style market series must fail.
        if reference_latest != datetime.min:
            for label, rec in [("disaggregated", latest_disagg), ("legacy", latest_legacy)]:
                if rec:
                    lag_days = (reference_latest - parse_date(rec.get("report_date"))).days
                    if lag_days > max_stale_days:
                        errors.append(
                            f"{label} latest report date is stale by {lag_days} days vs reference latest {reference_latest.date()}."
                        )

        if latest_disagg:
            for field in REQUIRED_COMMON_FIELDS:
                if field not in latest_disagg:
                    errors.append(f"Missing common field in disaggregated: {field}")
            for field in ["producer_merchant_net", "managed_money_net", "managed_money_net_pct_oi"]:
                if latest_disagg.get(field) is None:
                    errors.append(f"Missing disaggregated scoring field in latest record: {field}")

        if latest_legacy:
            for field in REQUIRED_COMMON_FIELDS:
                if field not in latest_legacy:
                    errors.append(f"Missing common field in legacy: {field}")
            for field in ["commercial_net", "noncommercial_net"]:
                if latest_legacy.get(field) is None:
                    errors.append(f"Missing legacy field in latest record: {field}")

        # Proxy disclosure warning for CL if ICE WTI is used.
        proxy_notes = sorted(set(norm_text(r.get("source_mapping_note")) for r in group if norm_text(r.get("source_mapping_note"))))
        warnings.extend(proxy_notes)

        info = per_symbol.get(symbol, {})
        results.append({
            "instrument": symbol,
            "commodity_label": TARGETS[symbol]["label"],
            "ok": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "disagg_count": len(disagg),
            "legacy_count": len(legacy),
            "latest_disagg_date": latest_disagg.get("report_date") if latest_disagg else None,
            "latest_legacy_date": latest_legacy.get("report_date") if latest_legacy else None,
            "selected_disagg_market": info.get("selected_disagg_market"),
            "selected_legacy_market": info.get("selected_legacy_market"),
            "disagg_match_score": info.get("disagg_match_score"),
            "legacy_match_score": info.get("legacy_match_score"),
            "reference_latest_date": reference_latest.date().isoformat() if reference_latest != datetime.min else None,
            "max_stale_days": max_stale_days,
        })

    return {
        "validated_at": now_utc(),
        "stage": "CFTC_COT_STEP6_FETCH_VALIDATION_V4_CL_AUDIT",
        "all_required_ok": all(r["ok"] for r in results),
        "total_records": len(records),
        "target_symbols": list(TARGETS.keys()),
        "datasets": {
            "disaggregated_futures_only": DISAGG_DATASET,
            "legacy_futures_only": LEGACY_DATASET,
        },
        "validation_rules": {
            "required_by_symbol": "Each symbol must have Disaggregated Futures Only and Legacy Futures Only records.",
            "freshness": f"Latest report date for each symbol/report type must be within {max_stale_days} days of the freshest selected COT date.",
            "disaggregated_required_fields": "Producer/Merchant net and Managed Money net must be computable in the latest record.",
            "legacy_required_fields": "Commercial net and Noncommercial net must be computable in the latest record.",
            "scoring_prep": "Week-over-week changes, net/open-interest ratios, percentiles, and z-scores are computed when enough history is available.",
            "cl_mapping": "Fresh NYMEX WTI is hard-preferred. If unavailable, fresh ICE WTI is selected and explicitly labeled PROXY.",
            "market_audit": "Per-symbol market candidate audit JSON/CSV files are saved under raw/.",
        },
        "group_results": results,
    }

def write_validation_txt(path: Path, validation: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    lines = []
    lines.append("CFTC COT Step 6 Fetch Validation Report")
    lines.append("=" * 80)
    lines.append(f"Validated at: {validation['validated_at']}")
    lines.append(f"All required OK: {validation['all_required_ok']}")
    lines.append(f"Total records: {validation['total_records']}")
    lines.append("")
    for g in validation["group_results"]:
        status = "PASS" if g["ok"] else "WARN"
        lines.append(
            f"{status} | {g['instrument']:<2} | disagg={g['disagg_count']:4d} latest={g['latest_disagg_date']} "
            f"| legacy={g['legacy_count']:4d} latest={g['latest_legacy_date']}"
        )
        lines.append(f"      disagg_market={g['selected_disagg_market']}")
        lines.append(f"      legacy_market={g['selected_legacy_market']}")
        if g["errors"]:
            lines.append(f"      errors={g['errors']}")
        if g["warnings"]:
            lines.append(f"      warnings={g['warnings']}")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_symbol_outputs(out_dir: Path, symbol: str, records: List[Dict[str, Any]], validation_group: Dict[str, Any]) -> Dict[str, str]:
    sdir = out_dir / "commodities" / symbol
    ensure_dir(sdir)

    json_path = sdir / f"cftc_step6_{symbol}_cot_positioning_normalized.json"
    jsonl_path = sdir / f"cftc_step6_{symbol}_cot_positioning_normalized.jsonl"
    csv_path = sdir / f"cftc_step6_{symbol}_cot_positioning_normalized.csv"
    validation_path = sdir / f"cftc_step6_{symbol}_cot_validation_report.json"

    write_jsonl(jsonl_path, records)
    write_csv(csv_path, records)
    save_json(json_path, {
        "generated_at": now_utc(),
        "source": "CFTC_PUBLIC_REPORTING",
        "stage": "step6_cot_fetch_clean_normalize_light_v4_cl_audit",
        "normalization_status": "COMPLETED_IN_FETCHER",
        "instrument": symbol,
        "commodity_label": TARGETS[symbol]["label"],
        "records": records,
    })
    save_json(validation_path, validation_group)

    return {
        "normalized_json": str(json_path),
        "jsonl": str(jsonl_path),
        "csv": str(csv_path),
        "validation_json": str(validation_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-per-symbol", type=int, default=156)
    parser.add_argument("--query-limit", type=int, default=5000)
    parser.add_argument("--max-stale-days", type=int, default=21, help="Freshness tolerance versus the freshest selected COT report date.")
    parser.add_argument("--out-dir", default=str(Path.home() / "cftc_output" / "step6_cot_light_v4"))
    parser.add_argument("--gatekeeper-id", default="LOCAL_PROTO")
    parser.add_argument("--app-token", default=os.getenv("SOCRATA_APP_TOKEN", "").strip())
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    raw_dir = out_dir / "raw"
    combined_dir = out_dir / "combined"
    ensure_dir(raw_dir)
    ensure_dir(combined_dir)

    print("=" * 80)
    print("CFTC COT Step 6 Fetcher LIGHT v4 CL AUDIT FIXED")
    print("=" * 80)
    print(f"Output directory: {out_dir}")
    print(f"Raw directory:    {raw_dir}")
    print(f"Limit per symbol: {args.limit_per_symbol}")
    print(f"Query limit:      {args.query_limit}")
    print(f"Max stale days:   {args.max_stale_days}")
    print(f"App token:        {'YES' if args.app_token else 'NO'}")
    print(f"Targets:          {', '.join(TARGETS.keys())}")

    print("\n[METADATA] Fetching dataset metadata...")
    disagg_meta = get_metadata(DISAGG_DATASET, args.app_token)
    legacy_meta = get_metadata(LEGACY_DATASET, args.app_token)
    save_json(raw_dir / "cftc_disagg_metadata.json", disagg_meta)
    save_json(raw_dir / "cftc_legacy_metadata.json", legacy_meta)
    print(f"  Disaggregated metadata columns: {len(disagg_meta.get('columns', []))}")
    print(f"  Legacy metadata columns:        {len(legacy_meta.get('columns', []))}")

    all_records: List[Dict[str, Any]] = []
    per_symbol_info: Dict[str, Dict[str, Any]] = {}

    for symbol, cfg in TARGETS.items():
        print("\n" + "-" * 80)
        print(f"[TARGET] {symbol} | {cfg['label']}")
        print("-" * 80)

        disagg_candidates = fetch_candidate_rows(DISAGG_DATASET, symbol, cfg, args.app_token, args.query_limit)
        save_json(raw_dir / f"{symbol}_disagg_candidates_raw.json", disagg_candidates)
        disagg_rows, disagg_market, disagg_score = select_best_market_rows(disagg_candidates, cfg, args.limit_per_symbol)
        save_json(raw_dir / f"{symbol}_disagg_market_audit.json", cfg.get("_last_market_audit", []))
        write_generic_csv(raw_dir / f"{symbol}_disagg_market_audit.csv", cfg.get("_last_market_audit", []))
        disagg_records = [normalize_disagg_row(r, symbol, cfg, args.gatekeeper_id) for r in disagg_rows]

        legacy_candidates = fetch_candidate_rows(LEGACY_DATASET, symbol, cfg, args.app_token, args.query_limit)
        save_json(raw_dir / f"{symbol}_legacy_candidates_raw.json", legacy_candidates)
        legacy_rows, legacy_market, legacy_score = select_best_market_rows(legacy_candidates, cfg, args.limit_per_symbol)
        save_json(raw_dir / f"{symbol}_legacy_market_audit.json", cfg.get("_last_market_audit", []))
        write_generic_csv(raw_dir / f"{symbol}_legacy_market_audit.csv", cfg.get("_last_market_audit", []))
        legacy_records = [normalize_legacy_row(r, symbol, cfg, args.gatekeeper_id) for r in legacy_rows]

        print(f"  disagg candidates={len(disagg_candidates):5d} selected={len(disagg_records):4d} market={disagg_market}")
        print(f"  legacy candidates={len(legacy_candidates):5d} selected={len(legacy_records):4d} market={legacy_market}")

        per_symbol_info[symbol] = {
            "selected_disagg_market": disagg_market,
            "selected_legacy_market": legacy_market,
            "disagg_match_score": disagg_score,
            "legacy_match_score": legacy_score,
        }

        all_records.extend(disagg_records)
        all_records.extend(legacy_records)

    all_records = add_time_series_features(all_records)
    validation = validate_records(all_records, per_symbol_info, args.limit_per_symbol, args.max_stale_days)

    validation_by_symbol = {g["instrument"]: g for g in validation["group_results"]}
    per_symbol_outputs = {}
    for symbol in TARGETS:
        symbol_records = [r for r in all_records if r.get("instrument") == symbol]
        per_symbol_outputs[symbol] = write_symbol_outputs(out_dir, symbol, symbol_records, validation_by_symbol[symbol])

    combined_json = combined_dir / "cftc_step6_cot_positioning_normalized.json"
    combined_jsonl = combined_dir / "cftc_step6_cot_positioning_normalized.jsonl"
    combined_csv = combined_dir / "cftc_step6_cot_positioning_normalized.csv"
    validation_json = combined_dir / "cftc_step6_cot_validation_report.json"
    validation_txt = combined_dir / "cftc_step6_cot_validation_report.txt"

    write_jsonl(combined_jsonl, all_records)
    write_csv(combined_csv, all_records)
    save_json(combined_json, {
        "generated_at": now_utc(),
        "source": "CFTC_PUBLIC_REPORTING",
        "stage": "step6_cot_fetch_clean_normalize_light_v4_cl_audit",
        "normalization_status": "COMPLETED_IN_FETCHER",
        "target_symbols": list(TARGETS.keys()),
        "records": all_records,
        "per_symbol_outputs": per_symbol_outputs,
    })
    save_json(validation_json, validation)
    write_validation_txt(validation_txt, validation)

    print("\n" + "=" * 80)
    print("CFTC COT Step 6 Summary")
    print("=" * 80)
    for g in validation["group_results"]:
        status = "PASS" if g["ok"] else "WARN"
        print(
            f"{status} | {g['instrument']:<2} | disagg={g['disagg_count']:4d} latest={g['latest_disagg_date']} "
            f"| legacy={g['legacy_count']:4d} latest={g['latest_legacy_date']}"
        )
        print(f"      disagg_market={g['selected_disagg_market']}")
        print(f"      legacy_market={g['selected_legacy_market']}")
        if g["errors"]:
            print(f"      errors={g['errors']}")
        if g["warnings"]:
            print(f"      warnings={g['warnings']}")

    print("\nSaved commodity-split outputs under:")
    print(out_dir / "commodities")
    print("\nSaved combined normalized JSON:")
    print(combined_json)
    print("\nSaved validation report:")
    print(validation_json)
    print(validation_txt)
    print("\nRaw API responses:")
    print(raw_dir)

    if validation["all_required_ok"]:
        print("\n[DONE] CFTC COT Step 6 fetch layer completed and validated.")
        return 0

    print("\n[DONE WITH WARNINGS] Review validation report.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
