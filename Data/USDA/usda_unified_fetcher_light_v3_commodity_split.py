#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
USDA AMS / MARS Step 5 Unified Fetcher LIGHT

Purpose
-------
One lightweight script to complete USDA Step 5 local MVP in one run, including fetch, cleaning, unit normalization, commodity-level split outputs, and validation.

Document target set:
  1. CORN_DECATUR
  2. CORN_GULF
  3. SOYBEAN_MEAL
  4. HRW_WHEAT
  5. SRW_WHEAT

Implementation policy
---------------------
A. Non-corn targets are treated strictly:
   - SOYBEAN_MEAL:
       USDA slug 3511
       commodity = Soybean Meal
       price_unit = $ Per Ton
       sale_type is preserved honestly. Current USDA feedstuff records are usually Ask.
   - HRW_WHEAT:
       USDA slug 2886
       commodity = Wheat
       class = Hard Red Winter / HRW
       price_unit = $ Per Bushel
       sale_type = Bid
   - SRW_WHEAT:
       USDA slugs 2787 and 2851 in light mode
       commodity = Wheat
       class = Soft Red Winter / SRW
       price_unit = $ Per Bushel
       sale_type = Bid

B. Corn targets are treated separately as PROXY/FALLBACK because strict Decatur/Gulf
   corn records were not found in broad USDA MARS search:
   - CORN_DECATUR_PROXY:
       USDA slug 3192
       Illinois grain bid proxy for Decatur
       commodity = Corn
       sale_type = Bid
       price_unit = $ Per Bushel
       source_mapping_status = PROXY
       proxy_for = CORN_DECATUR

   - CORN_GULF_PROXY:
       USDA slug 3043
       Barge/terminal corn bid proxy for Gulf
       commodity = Corn
       sale_type = Bid
       price_unit = $ Per Bushel
       requires row text to include barge/terminal/river style terms
       source_mapping_status = PROXY
       proxy_for = CORN_GULF

   - CORN_COUNTRY_ELEVATOR_OK_FALLBACK:
       USDA slug 3100
       Oklahoma Daily Grain Bids fallback. The USDA report explicitly contains
       US #2 Yellow Corn (Bulk), Country Elevators - Conventional, Bid, Price($/Bu).
       source_mapping_status = FALLBACK
       proxy_for = None

Flow
----
USDA AMS / MARS API
↓
fetch selected Report Detail payloads
↓
expand nested results[]
↓
filter target commodity/class/location-proxy logic
↓
clean bid/ask price
↓
normalize unit
↓
deduplicate
↓
keep latest N records per target
↓
write commodity-split normalized JSON/JSONL/CSV, plus optional combined files
↓
write one normalized file set per commodity group and per target
↓
write validation JSON/TXT report

Authentication
--------------
PowerShell:
    $env:USDA_MARS_API_KEY="YOUR_KEY"

Run
---
    python usda_step5_unified_fetcher_light.py --days 60 --limit-per-target 30

Output directory default
------------------------
    C:\Users\<YOU>\usda_output\step5_unified_light

Combined outputs
----------------
    combined/usda_step5_grain_physical_prices_normalized.json
    combined/usda_step5_grain_physical_prices_normalized.jsonl
    combined/usda_step5_grain_physical_prices_normalized.csv
    combined/usda_step5_grain_physical_prices_validation_report.json
    combined/usda_step5_grain_physical_prices_validation_report.txt

Per-target outputs
------------------
    targets/<TARGET>/<TARGET>_records.jsonl
    targets/<TARGET>/<TARGET>_records.csv
    targets/<TARGET>/<TARGET>_normalized.json
    targets/<TARGET>/<TARGET>_validation_report.json
"""

import argparse
import csv
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2"


TARGET_CONFIG: Dict[str, Dict[str, Any]] = {
    # -----------------------------
    # Corn proxy / fallback targets
    # -----------------------------
    "CORN_DECATUR_PROXY": {
        "document_target": "CORN_DECATUR",
        "source_id": "USDA_AMS_CORN_ILLINOIS_PROXY_FOR_DECATUR",
        "instrument": "CORN",
        "delivery_point": "ILLINOIS_GRAIN_BID_PROXY_FOR_DECATUR",
        "data_type": "ELEVATOR_BID",
        "unit": "USD_PER_BUSHEL",
        "slugs": ["3192"],
        "commodity": "Corn",
        "class_contains": [],
        "price_unit_allowed": {"$ per bushel", "dollars per bushel", "usd per bushel"},
        "sale_type_required": "bid",
        "row_text_any": ["illinois", "country elevator", "country elevators", "processor", "central illinois", "interior illinois"],
        "row_text_prefer": ["illinois", "central illinois", "interior illinois"],
        "allow_without_text_hit": True,
        "source_mapping_status": "PROXY",
        "proxy_for": "CORN_DECATUR",
        "proxy_reason": "Strict Decatur corn bid was not found in USDA MARS broad search; Illinois grain-bid source is used as the closest searchable proxy.",
        "tos_status": "GO",
        "gatekeeper_cleared": True,
    },
    "CORN_GULF_PROXY": {
        "document_target": "CORN_GULF",
        "source_id": "USDA_AMS_CORN_BARGE_TERMINAL_PROXY_FOR_GULF",
        "instrument": "CORN",
        "delivery_point": "BARGE_TERMINAL_PROXY_FOR_GULF",
        "data_type": "ELEVATOR_BID",
        "unit": "USD_PER_BUSHEL",
        "slugs": ["3043"],
        "commodity": "Corn",
        "class_contains": [],
        "price_unit_allowed": {"$ per bushel", "dollars per bushel", "usd per bushel"},
        "sale_type_required": "bid",
        "row_text_any": ["barge", "barge loading", "terminal", "river", "mississippi river"],
        "row_text_prefer": ["barge", "terminal", "river"],
        "allow_without_text_hit": False,
        "source_mapping_status": "PROXY",
        "proxy_for": "CORN_GULF",
        "proxy_reason": "Strict Gulf/New Orleans corn bid was not found in USDA MARS broad search; barge/terminal corn bids are used as the closest searchable proxy.",
        "tos_status": "GO",
        "gatekeeper_cleared": True,
    },
    "CORN_COUNTRY_ELEVATOR_OK_FALLBACK": {
        "document_target": "CORN_FALLBACK",
        "source_id": "USDA_AMS_CORN_OK_COUNTRY_ELEVATOR_BID",
        "instrument": "CORN",
        "delivery_point": "OKLAHOMA_COUNTRY_ELEVATORS",
        "data_type": "ELEVATOR_BID",
        "unit": "USD_PER_BUSHEL",
        "slugs": ["3100"],
        "commodity": "Corn",
        "class_contains": [],
        "price_unit_allowed": {"$ per bushel", "dollars per bushel", "usd per bushel"},
        "sale_type_required": "bid",
        "row_text_any": ["country elevator", "country elevators", "oklahoma", "ok"],
        "row_text_prefer": ["country elevator", "country elevators"],
        "allow_without_text_hit": True,
        "source_mapping_status": "FALLBACK",
        "proxy_for": None,
        "proxy_reason": "Clean USDA corn country-elevator bid fallback. It is not Decatur/Gulf.",
        "tos_status": "GO",
        "gatekeeper_cleared": True,
    },

    # -----------------------------
    # Strict non-corn targets
    # -----------------------------
    "SOYBEAN_MEAL": {
        "document_target": "SOYBEAN_MEAL",
        "source_id": "USDA_AMS_SOYBEAN_MEAL_NATIONAL",
        "instrument": "SOYBEAN_MEAL",
        "delivery_point": "NATIONAL_PROCESSOR_FEEDSTUFF",
        "data_type": "PHYSICAL_PRICE",
        "unit": "USD_PER_SHORT_TON",
        "slugs": ["3511"],
        "commodity": "Soybean Meal",
        "class_contains": [],
        "price_unit_allowed": {"$ per ton", "dollars per ton", "usd per ton"},
        "sale_type_required": None,  # preserve Ask if that is what USDA reports
        "row_text_any": [],
        "row_text_prefer": [],
        "allow_without_text_hit": True,
        "source_mapping_status": "STRICT",
        "proxy_for": None,
        "proxy_reason": None,
        "tos_status": "GO",
        "gatekeeper_cleared": True,
        "warning_if_no_bid": True,
    },
    "HRW_WHEAT": {
        "document_target": "HRW_WHEAT",
        "source_id": "USDA_AMS_HRW_WHEAT_KANSAS_BID",
        "instrument": "HRW_WHEAT",
        "delivery_point": "KANSAS_GRAIN_BIDS",
        "data_type": "ELEVATOR_BID",
        "unit": "USD_PER_BUSHEL",
        "slugs": ["2886"],
        "commodity": "Wheat",
        "class_contains": ["hard red winter", "hrw"],
        "price_unit_allowed": {"$ per bushel", "dollars per bushel", "usd per bushel"},
        "sale_type_required": "bid",
        "row_text_any": [],
        "row_text_prefer": [],
        "allow_without_text_hit": True,
        "source_mapping_status": "STRICT",
        "proxy_for": None,
        "proxy_reason": None,
        "tos_status": "GO",
        "gatekeeper_cleared": True,
    },
    "SRW_WHEAT": {
        "document_target": "SRW_WHEAT",
        "source_id": "USDA_AMS_SRW_WHEAT_BID",
        "instrument": "SRW_WHEAT",
        "delivery_point": "US_SRW_GRAIN_BIDS",
        "data_type": "ELEVATOR_BID",
        "unit": "USD_PER_BUSHEL",
        "slugs": ["2787", "2851"],
        "commodity": "Wheat",
        "class_contains": ["soft red winter", "srw"],
        "price_unit_allowed": {"$ per bushel", "dollars per bushel", "usd per bushel"},
        "sale_type_required": "bid",
        "row_text_any": [],
        "row_text_prefer": [],
        "allow_without_text_hit": True,
        "source_mapping_status": "STRICT",
        "proxy_for": None,
        "proxy_reason": None,
        "tos_status": "GO",
        "gatekeeper_cleared": True,
    },
}


REQUIRED_OUTPUT_FIELDS = [
    "source_id",
    "instrument",
    "delivery_point",
    "timestamp",
    "data_type",
    "value",
    "unit",
    "basis_vs_futures",
    "futures_contract",
    "tos_status",
    "gatekeeper_cleared",
    "gatekeeper_id",
    "raw_source_url",
    "series_key",
    "document_target",
    "slug_id",
    "report_title",
    "commodity_raw",
    "source_mapping_status",
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
        s = str(x).strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def parse_mdy(s: Any) -> datetime:
    txt = norm_text(s)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            pass
    return datetime.min


def get_field(row: Dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
    lower_map = {str(k).lower(): k for k in row.keys()}
    for name in names:
        k = lower_map.get(name.lower())
        if k is not None:
            return row[k]
    return None


def class_matches(class_value: Any, patterns: List[str]) -> bool:
    if not patterns:
        return True
    c = lower(class_value)
    for p in patterns:
        p = p.lower()
        if p == "hrw" and ("hard red winter" in c or c == "hrw"):
            return True
        if p == "srw" and ("soft red winter" in c or c == "srw"):
            return True
        if p in c:
            return True
    return False


def compute_price_value(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    """
    Price cleaning math:
    1. If USDA avg_price exists:
           value = avg_price
    2. Else if price_min and price_max exist:
           value = (price_min + price_max) / 2
    3. Else if only price_min exists:
           value = price_min
    4. Else if only price_max exists:
           value = price_max
    """
    avg = parse_float(get_field(row, "avg_price", "Avg Price", "average_price"))
    pmin = parse_float(get_field(row, "price Min", "price_min", "price min"))
    pmax = parse_float(get_field(row, "price Max", "price_max", "price max"))

    if avg is not None:
        return avg, pmin, pmax, "avg_price"
    if pmin is not None and pmax is not None:
        return (pmin + pmax) / 2.0, pmin, pmax, "midpoint_price_min_max"
    if pmin is not None:
        return pmin, pmin, pmax, "price_min_only"
    if pmax is not None:
        return pmax, pmin, pmax, "price_max_only"
    return None, pmin, pmax, "missing_price"


def recursively_find_results(payload: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "results" and isinstance(value, list):
                for row in value:
                    if isinstance(row, dict):
                        yield row
            else:
                yield from recursively_find_results(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from recursively_find_results(item)


def row_text(row: Dict[str, Any]) -> str:
    fields = [
        "report_title", "market_location_name", "market_location_city", "market_location_state",
        "trade_loc", "trade Loc", "location_State", "location_City",
        "commodity", "class", "grade", "delivery_point", "quote_type",
        "sale Type", "sale_type", "freight", "delivery", "delivery_period", "desc",
    ]
    return " | ".join(norm_text(get_field(row, f)) for f in fields).lower()


def keyword_hits(text: str, keywords: List[str]) -> List[str]:
    return [kw for kw in keywords if kw.lower() in text]


def fetch_json(url: str, api_key: str, params: Optional[Dict[str, Any]] = None, retries: int = 3) -> Any:
    last_error = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params or {}, auth=(api_key, ""), timeout=60)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.8 * (2 ** attempt))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            time.sleep(0.8 * (2 ** attempt))
    raise RuntimeError(f"GET failed: {url} params={params} error={last_error}")


def save_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def fetch_slug(
    api_key: str,
    target_key: str,
    slug: str,
    begin_date: str,
    end_date: str,
    raw_dir: Path,
) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/reports/{slug}/Report%20Detail"
    params = {
        "report_begin_date": begin_date,
        "report_end_date": end_date,
        "format": "json",
    }
    fname = f"usda_step5_{target_key}_{slug}_Report_Detail_raw.json"
    try:
        payload = fetch_json(url, api_key=api_key, params=params)
        save_json(raw_dir / fname, payload)
        rows = list(recursively_find_results(payload))
        print(f"  fetched slug={slug:<5} file={fname:<70} results_rows={len(rows)}")
        return rows
    except Exception as exc:
        print(f"  [WARN] failed slug={slug}: {exc}")
        return []


def filter_normalize_row(row: Dict[str, Any], target_key: str, cfg: Dict[str, Any], gatekeeper_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
    commodity = norm_text(get_field(row, "commodity"))
    cls = norm_text(get_field(row, "class"))
    price_unit = norm_text(get_field(row, "price_unit", "price unit"))
    sale_type = norm_text(get_field(row, "sale Type", "sale_type", "sale type"))
    report_date = norm_text(get_field(row, "report_date"))
    slug_id = norm_text(get_field(row, "slug_id"))

    if lower(commodity) != lower(cfg["commodity"]):
        return None, "commodity_mismatch"

    if not class_matches(cls, cfg.get("class_contains", [])):
        return None, "class_mismatch"

    if lower(price_unit) not in cfg["price_unit_allowed"]:
        return None, "price_unit_mismatch"

    required_sale = cfg.get("sale_type_required")
    if required_sale is not None and lower(sale_type) != lower(required_sale):
        return None, "sale_type_mismatch"

    txt = row_text(row)
    hits_any = keyword_hits(txt, cfg.get("row_text_any", []))
    hits_prefer = keyword_hits(txt, cfg.get("row_text_prefer", []))
    if cfg.get("row_text_any") and not hits_any and not cfg.get("allow_without_text_hit", False):
        return None, "delivery_location_proxy_mismatch"

    value, price_low, price_high, price_method = compute_price_value(row)
    if value is None or value <= 0:
        return None, "invalid_price"

    if not report_date:
        return None, "missing_report_date"

    record = {
        "source_id": cfg["source_id"],
        "instrument": cfg["instrument"],
        "delivery_point": cfg["delivery_point"],
        "timestamp": report_date,
        "data_type": cfg["data_type"],
        "value": value,
        "unit": cfg["unit"],
        "price_low": price_low,
        "price_high": price_high,
        "price_method": price_method,
        "price_unit_raw": price_unit,
        "basis_vs_futures": None,
        "futures_contract": None,
        "tos_status": cfg["tos_status"],
        "gatekeeper_cleared": cfg["gatekeeper_cleared"],
        "gatekeeper_id": gatekeeper_id,
        "raw_source_url": f"{BASE_URL}/reports/{slug_id}/Report%20Detail" if slug_id else BASE_URL,

        "series_key": target_key,
        "document_target": cfg["document_target"],
        "source_mapping_status": cfg["source_mapping_status"],
        "proxy_for": cfg.get("proxy_for"),
        "proxy_reason": cfg.get("proxy_reason"),
        "proxy_keyword_hits": sorted(set(hits_any + hits_prefer)),

        "slug_id": slug_id,
        "slug_name": norm_text(get_field(row, "slug_name")),
        "report_title": norm_text(get_field(row, "report_title")),
        "report_date": report_date,
        "published_date": norm_text(get_field(row, "published_date")),
        "commodity_raw": commodity,
        "class_raw": cls,
        "grade": norm_text(get_field(row, "grade")),
        "protein": norm_text(get_field(row, "protein")),
        "delivery_point_raw": norm_text(get_field(row, "delivery_point")),
        "market_location_name": norm_text(get_field(row, "market_location_name")),
        "market_location_city": norm_text(get_field(row, "market_location_city")),
        "market_location_state": norm_text(get_field(row, "market_location_state")),
        "trade_loc": norm_text(get_field(row, "trade_loc", "trade Loc")),
        "location_state": norm_text(get_field(row, "location_State")),
        "location_city": norm_text(get_field(row, "location_City")),
        "quote_type": norm_text(get_field(row, "quote_type")),
        "sale_type": sale_type,
        "basis_unit": norm_text(get_field(row, "basis_unit")),
        "basis_min": get_field(row, "basis Min", "basis_min"),
        "basis_max": get_field(row, "basis Max", "basis_max"),
        "freight": norm_text(get_field(row, "freight")),
        "delivery_raw": norm_text(get_field(row, "delivery", "delivery_period")),
        "normalized_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return record, "ok"


def deduplicate(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for rec in records:
        key = (
            rec.get("series_key"),
            rec.get("slug_id"),
            rec.get("timestamp"),
            rec.get("report_title"),
            rec.get("commodity_raw"),
            rec.get("class_raw"),
            rec.get("grade"),
            rec.get("protein"),
            rec.get("delivery_point_raw"),
            rec.get("market_location_name"),
            rec.get("trade_loc"),
            rec.get("location_state"),
            rec.get("location_city"),
            rec.get("quote_type"),
            rec.get("sale_type"),
            rec.get("price_low"),
            rec.get("price_high"),
            rec.get("value"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(rec)
    return out


def limit_latest_per_target(records: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    out = []
    for target_key in TARGET_CONFIG:
        group = [r for r in records if r.get("series_key") == target_key]
        group.sort(
            key=lambda r: (
                parse_mdy(r.get("timestamp")),
                len(r.get("proxy_keyword_hits", [])),
                float(r.get("value", 0)),
                str(r.get("slug_id", "")),
            ),
            reverse=True,
        )
        out.extend(group[:limit])
    out.sort(key=lambda r: (r.get("series_key", ""), parse_mdy(r.get("timestamp")), str(r.get("slug_id", ""))))
    return out


def write_csv(path: Path, records: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "series_key", "document_target", "source_mapping_status", "proxy_for",
        "source_id", "instrument", "delivery_point", "timestamp", "data_type",
        "value", "unit", "price_low", "price_high", "price_method", "price_unit_raw",
        "slug_id", "slug_name", "report_title", "published_date",
        "commodity_raw", "class_raw", "grade", "protein", "delivery_point_raw",
        "market_location_name", "market_location_city", "market_location_state",
        "trade_loc", "location_state", "location_city", "quote_type", "sale_type",
        "basis_unit", "basis_min", "basis_max", "freight", "delivery_raw",
        "proxy_keyword_hits", "proxy_reason", "tos_status", "gatekeeper_cleared",
        "gatekeeper_id", "raw_source_url",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            row = {k: rec.get(k) for k in fieldnames}
            row["proxy_keyword_hits"] = "|".join(rec.get("proxy_keyword_hits", []))
            writer.writerow(row)


def validate_target(records: List[Dict[str, Any]], target_key: str, cfg: Dict[str, Any], reject_counts: Dict[str, int], limit: int) -> Dict[str, Any]:
    group = [r for r in records if r.get("series_key") == target_key]
    errors: List[str] = []
    warnings: List[str] = []

    if len(group) == 0:
        errors.append("No records found after filtering.")
    if len(group) > limit:
        errors.append(f"Record count exceeds limit_per_target={limit}.")

    for rec in group:
        missing = [f for f in REQUIRED_OUTPUT_FIELDS if f not in rec]
        if missing:
            errors.append(f"Missing required fields: {missing}")
            break

    invalid_value = sum(1 for r in group if not isinstance(r.get("value"), (int, float)) or r.get("value", 0) <= 0)
    invalid_unit = sum(1 for r in group if r.get("unit") != cfg["unit"])
    non_null_basis = sum(1 for r in group if r.get("basis_vs_futures") is not None)
    non_null_futures = sum(1 for r in group if r.get("futures_contract") is not None)
    wrong_commodity = sum(1 for r in group if lower(r.get("commodity_raw")) != lower(cfg["commodity"]))
    wrong_class = sum(1 for r in group if not class_matches(r.get("class_raw"), cfg.get("class_contains", [])))

    if invalid_value:
        errors.append(f"Invalid value count: {invalid_value}")
    if invalid_unit:
        errors.append(f"Invalid unit count: {invalid_unit}")
    if non_null_basis:
        errors.append(f"basis_vs_futures should be null in Step 5, count={non_null_basis}")
    if non_null_futures:
        errors.append(f"futures_contract should be null in Step 5, count={non_null_futures}")
    if wrong_commodity:
        errors.append(f"Commodity mismatch count: {wrong_commodity}")
    if wrong_class:
        errors.append(f"Class mismatch count: {wrong_class}")

    required_sale = cfg.get("sale_type_required")
    if required_sale is not None:
        wrong_sale = sum(1 for r in group if lower(r.get("sale_type")) != lower(required_sale))
        if wrong_sale:
            errors.append(f"sale_type must be {required_sale}, count={wrong_sale}")

    if cfg["source_mapping_status"] in {"PROXY", "FALLBACK"} and cfg.get("proxy_reason"):
        warnings.append(cfg["proxy_reason"])

    if target_key == "SOYBEAN_MEAL":
        sale_types = sorted(set(norm_text(r.get("sale_type")) for r in group))
        if sale_types and "bid" not in [s.lower() for s in sale_types]:
            warnings.append("Soybean meal records are not Bid in the fetched USDA feedstuff source; sale_type is preserved honestly.")

    latest = None
    latest_value = None
    if group:
        latest_rec = max(group, key=lambda r: parse_mdy(r.get("timestamp")))
        latest = latest_rec.get("timestamp")
        latest_value = latest_rec.get("value")

    return {
        "series_key": target_key,
        "document_target": cfg["document_target"],
        "source_id": cfg["source_id"],
        "instrument": cfg["instrument"],
        "delivery_point": cfg["delivery_point"],
        "source_mapping_status": cfg["source_mapping_status"],
        "proxy_for": cfg.get("proxy_for"),
        "record_count": len(group),
        "latest_timestamp": latest,
        "latest_value": latest_value,
        "expected_unit": cfg["unit"],
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "reject_counts": reject_counts,
    }


def build_validation(records: List[Dict[str, Any]], all_reject_counts: Dict[str, Dict[str, int]], limit: int) -> Dict[str, Any]:
    group_results = [
        validate_target(records, k, cfg, all_reject_counts.get(k, {}), limit)
        for k, cfg in TARGET_CONFIG.items()
    ]

    return {
        "validated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stage": "USDA_STEP5_UNIFIED_LIGHT_VALIDATION",
        "all_required_ok": all(g["ok"] for g in group_results),
        "total_records": len(records),
        "document_targets": [
            "CORN_DECATUR handled as CORN_DECATUR_PROXY",
            "CORN_GULF handled as CORN_GULF_PROXY",
            "SOYBEAN_MEAL strict",
            "HRW_WHEAT strict",
            "SRW_WHEAT strict",
            "CORN_COUNTRY_ELEVATOR_OK_FALLBACK included as additional corn fallback/provenance check"
        ],
        "corn_mapping_note": (
            "Strict Decatur/Gulf corn bid records were not found in prior USDA MARS broad search. "
            "Corn outputs are explicitly labeled as PROXY/FALLBACK and should not be represented as strict Decatur/Gulf data."
        ),
        "required_output_fields": REQUIRED_OUTPUT_FIELDS,
        "validation_rules": {
            "common": [
                "source_id/instrument/delivery_point/timestamp/data_type/value/unit required",
                "value must be numeric and > 0",
                "basis_vs_futures must be null before futures leg",
                "futures_contract must be null before futures leg",
                "raw_source_url, slug_id, report_title required for provenance",
            ],
            "corn": "commodity=Corn, sale_type=Bid, unit=USD_PER_BUSHEL, mapping disclosed as PROXY/FALLBACK",
            "soybean_meal": "commodity=Soybean Meal, unit=USD_PER_SHORT_TON, sale_type preserved honestly",
            "hrw_wheat": "commodity=Wheat, class=Hard Red Winter/HRW, sale_type=Bid, unit=USD_PER_BUSHEL",
            "srw_wheat": "commodity=Wheat, class=Soft Red Winter/SRW, sale_type=Bid, unit=USD_PER_BUSHEL",
        },
        "price_cleaning_logic": [
            "Prefer value = avg_price.",
            "If avg_price is missing, use value = (price_min + price_max) / 2.",
            "If only one side exists, use the available price_min or price_max.",
            "No scaling, z-score, or model transformation is applied."
        ],
        "group_results": group_results,
    }


def write_validation_txt(path: Path, validation: Dict[str, Any]) -> None:
    lines = []
    lines.append("USDA Step 5 Unified LIGHT Validation Report")
    lines.append("=" * 80)
    lines.append(f"Validated at: {validation['validated_at']}")
    lines.append(f"All required OK: {validation['all_required_ok']}")
    lines.append(f"Total records: {validation['total_records']}")
    lines.append("")
    lines.append("Corn mapping note:")
    lines.append(f"  {validation['corn_mapping_note']}")
    lines.append("")
    lines.append("Price cleaning logic:")
    for item in validation["price_cleaning_logic"]:
        lines.append(f"  - {item}")
    lines.append("")
    lines.append("Group results:")
    for g in validation["group_results"]:
        status = "PASS" if g["ok"] else "WARN"
        lines.append(
            f"{status} | {g['series_key']:<34} | doc_target={g['document_target']:<16} "
            f"| count={g['record_count']:4d} | latest={g['latest_timestamp']} "
            f"| value={g['latest_value']} | unit={g['expected_unit']} "
            f"| mapping={g['source_mapping_status']}"
        )
        if g["errors"]:
            lines.append(f"      errors: {g['errors']}")
        if g["warnings"]:
            lines.append(f"      warnings: {g['warnings']}")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_target_outputs(base_dir: Path, target_key: str, records: List[Dict[str, Any]], validation_result: Dict[str, Any]) -> Dict[str, str]:
    tdir = base_dir / "targets" / target_key
    tdir.mkdir(parents=True, exist_ok=True)

    jsonl_path = tdir / f"{target_key}_records.jsonl"
    csv_path = tdir / f"{target_key}_records.csv"
    normalized_path = tdir / f"{target_key}_normalized.json"
    validation_path = tdir / f"{target_key}_validation_report.json"

    write_jsonl(jsonl_path, records)
    write_csv(csv_path, records)
    save_json(normalized_path, {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "USDA_AMS_MARS",
        "stage": "step5_unified_light_per_target_normalized",
        "normalization_status": "COMPLETED_IN_FETCHER",
        "series_key": target_key,
        "records": records,
    })
    save_json(validation_path, validation_result)

    return {
        "jsonl": str(jsonl_path),
        "csv": str(csv_path),
        "normalized_json": str(normalized_path),
        "validation_json": str(validation_path),
    }



def commodity_group_for_record(rec: Dict[str, Any]) -> str:
    """
    Split final outputs by project-level commodity group.
    Corn proxy/fallback targets are grouped together under CORN.
    HRW and SRW are kept separate because the document treats HRW/SRW wheat as distinct targets.
    """
    series_key = rec.get("series_key")
    if series_key in {"CORN_DECATUR_PROXY", "CORN_GULF_PROXY", "CORN_COUNTRY_ELEVATOR_OK_FALLBACK"}:
        return "CORN"
    if series_key == "SOYBEAN_MEAL":
        return "SOYBEAN_MEAL"
    if series_key == "HRW_WHEAT":
        return "HRW_WHEAT"
    if series_key == "SRW_WHEAT":
        return "SRW_WHEAT"
    return str(series_key or "UNKNOWN")


def validate_commodity_group(records: List[Dict[str, Any]], commodity_group: str, group_records: List[Dict[str, Any]], validation: Dict[str, Any]) -> Dict[str, Any]:
    included_series = sorted(set(r.get("series_key") for r in group_records))
    target_results = [g for g in validation["group_results"] if g["series_key"] in included_series]
    return {
        "validated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stage": "USDA_STEP5_COMMODITY_SPLIT_VALIDATION",
        "commodity_group": commodity_group,
        "record_count": len(group_records),
        "included_series_keys": included_series,
        "ok": all(g.get("ok", False) for g in target_results) and len(group_records) > 0,
        "normalization_status": "COMPLETED_IN_FETCHER",
        "target_validation_results": target_results,
        "notes": [
            "This is a commodity-split output generated from the unified USDA Step 5 light fetcher.",
            "Corn records are explicitly labeled PROXY/FALLBACK because strict Decatur/Gulf corn records were not found.",
            "Non-corn records are handled under the strict target rules defined in the script."
        ],
    }


def write_commodity_outputs(base_dir: Path, records: List[Dict[str, Any]], validation: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Primary final outputs: split by commodity group.
    """
    commodity_dir = base_dir / "commodities"
    commodity_dir.mkdir(parents=True, exist_ok=True)

    groups: Dict[str, List[Dict[str, Any]]] = {}
    for rec in records:
        groups.setdefault(commodity_group_for_record(rec), []).append(rec)

    outputs: Dict[str, Dict[str, str]] = {}
    for commodity_group, group_records in sorted(groups.items()):
        cdir = commodity_dir / commodity_group
        cdir.mkdir(parents=True, exist_ok=True)

        jsonl_path = cdir / f"usda_step5_{commodity_group}_normalized.jsonl"
        csv_path = cdir / f"usda_step5_{commodity_group}_normalized.csv"
        normalized_path = cdir / f"usda_step5_{commodity_group}_normalized.json"
        validation_path = cdir / f"usda_step5_{commodity_group}_validation_report.json"

        write_jsonl(jsonl_path, group_records)
        write_csv(csv_path, group_records)

        commodity_validation = validate_commodity_group(records, commodity_group, group_records, validation)
        save_json(normalized_path, {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source": "USDA_AMS_MARS",
            "stage": "step5_unified_light_commodity_split_normalized",
            "normalization_status": "COMPLETED_IN_FETCHER",
            "commodity_group": commodity_group,
            "records": group_records,
        })
        save_json(validation_path, commodity_validation)

        outputs[commodity_group] = {
            "normalized_json": str(normalized_path),
            "jsonl": str(jsonl_path),
            "csv": str(csv_path),
            "validation_json": str(validation_path),
        }

    return outputs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--limit-per-target", type=int, default=30)
    parser.add_argument("--out-dir", default=str(Path.home() / "usda_output" / "step5_unified_light"))
    parser.add_argument("--gatekeeper-id", default="LOCAL_PROTO")
    parser.add_argument("--api-key", default=os.getenv("USDA_MARS_API_KEY", "").strip())
    parser.add_argument("--srw-extra-slugs", default="", help="Optional comma-separated extra SRW slugs.")
    args = parser.parse_args()

    if args.srw_extra_slugs.strip():
        extra = [x.strip() for x in args.srw_extra_slugs.split(",") if x.strip()]
        TARGET_CONFIG["SRW_WHEAT"]["slugs"] = list(dict.fromkeys(TARGET_CONFIG["SRW_WHEAT"]["slugs"] + extra))

    out_dir = Path(args.out_dir)
    raw_dir = out_dir / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("USDA AMS / MARS Step 5 Unified Fetcher LIGHT")
    print("=" * 80)
    print(f"Output directory: {out_dir}")
    print(f"Raw directory:    {raw_dir}")
    print(f"Lookback days:    {args.days}")
    print(f"Limit per target: {args.limit_per_target}")
    print(f"API key provided: {'YES' if args.api_key else 'NO'}")
    print(f"Targets:          {', '.join(TARGET_CONFIG.keys())}")

    if not args.api_key:
        print("[ERROR] Missing USDA_MARS_API_KEY.")
        print('PowerShell example: $env:USDA_MARS_API_KEY="YOUR_KEY"')
        return 2

    end = datetime.now()
    begin = end - timedelta(days=args.days)
    begin_date = begin.strftime("%m/%d/%Y")
    end_date = end.strftime("%m/%d/%Y")
    print(f"Date window:      {begin_date} to {end_date}")

    all_records_raw: List[Dict[str, Any]] = []
    reject_counts: Dict[str, Dict[str, int]] = {k: {} for k in TARGET_CONFIG}

    for target_key, cfg in TARGET_CONFIG.items():
        print("\n" + "-" * 80)
        print(f"[TARGET] {target_key} | doc_target={cfg['document_target']} | slugs={cfg['slugs']} | mapping={cfg['source_mapping_status']}")
        print("-" * 80)

        rows_all: List[Dict[str, Any]] = []
        for slug in cfg["slugs"]:
            rows_all.extend(fetch_slug(args.api_key, target_key, slug, begin_date, end_date, raw_dir))

        print(f"  expanded results[] rows before filter: {len(rows_all)}")

        records_target: List[Dict[str, Any]] = []
        for row in rows_all:
            rec, reason = filter_normalize_row(row, target_key, cfg, args.gatekeeper_id)
            if rec is not None:
                records_target.append(rec)
            else:
                reject_counts[target_key][reason] = reject_counts[target_key].get(reason, 0) + 1

        records_target = deduplicate(records_target)
        records_target = limit_latest_per_target(records_target, args.limit_per_target)
        all_records_raw.extend(records_target)

        print(f"  valid records after filter + dedupe + limit: {len(records_target)}")
        if records_target:
            latest = max(records_target, key=lambda r: parse_mdy(r.get("timestamp")))
            print(
                f"  latest sample: date={latest['timestamp']} value={latest['value']} "
                f"unit={latest['unit']} title={latest['report_title']} "
                f"commodity={latest['commodity_raw']} class={latest['class_raw']} "
                f"sale={latest['sale_type']} mapping={latest['source_mapping_status']}"
            )
        else:
            print(f"  reject_counts: {reject_counts[target_key]}")

    all_records = deduplicate(all_records_raw)
    all_records = limit_latest_per_target(all_records, args.limit_per_target)

    validation = build_validation(all_records, reject_counts, args.limit_per_target)

    # Combined outputs
    jsonl_path = out_dir / "combined/usda_step5_grain_physical_prices_normalized.jsonl"
    csv_path = out_dir / "combined/usda_step5_grain_physical_prices_normalized.csv"
    normalized_json_path = out_dir / "combined/usda_step5_grain_physical_prices_normalized.json"
    validation_json_path = out_dir / "combined/usda_step5_grain_physical_prices_validation_report.json"
    validation_txt_path = out_dir / "combined/usda_step5_grain_physical_prices_validation_report.txt"

    write_jsonl(jsonl_path, all_records)
    write_csv(csv_path, all_records)

    # Per-target outputs
    per_target_outputs = {}
    validation_by_key = {g["series_key"]: g for g in validation["group_results"]}
    for target_key in TARGET_CONFIG:
        target_records = [r for r in all_records if r.get("series_key") == target_key]
        per_target_outputs[target_key] = write_target_outputs(out_dir, target_key, target_records, validation_by_key[target_key])

    commodity_outputs = write_commodity_outputs(out_dir, all_records, validation)

    normalized_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "USDA_AMS_MARS",
        "stage": "step5_unified_fetch_filter_clean_normalize_light",
        "normalization_status": "COMPLETED_IN_FETCHER",
        "document_targets": validation["document_targets"],
        "corn_mapping_note": validation["corn_mapping_note"],
        "records": all_records,
        "commodity_outputs": commodity_outputs,
        "per_target_outputs": per_target_outputs,
    }
    save_json(normalized_json_path, normalized_payload)
    save_json(validation_json_path, validation)
    write_validation_txt(validation_txt_path, validation)

    print("\n" + "=" * 80)
    print("USDA Step 5 Unified LIGHT Summary")
    print("=" * 80)
    for g in validation["group_results"]:
        status = "PASS" if g["ok"] else "WARN"
        print(
            f"{status} | {g['series_key']:<34} | count={g['record_count']:4d} "
            f"| latest={g['latest_timestamp']} | value={g['latest_value']} "
            f"| unit={g['expected_unit']} | mapping={g['source_mapping_status']}"
        )
        if g["errors"]:
            print(f"      errors: {g['errors']}")
        if g["warnings"]:
            print(f"      warnings: {g['warnings']}")

    print("\nSaved commodity-split normalized outputs under:")
    print(out_dir / "commodities")

    print("\nSaved combined normalized JSON:")
    print(normalized_json_path)
    print("\nSaved combined JSONL:")
    print(jsonl_path)
    print("\nSaved combined CSV:")
    print(csv_path)
    print("\nSaved validation JSON:")
    print(validation_json_path)
    print("\nSaved validation TXT:")
    print(validation_txt_path)
    print("\nSaved per-target outputs under:")
    print(out_dir / "targets")
    print("\nRaw API responses:")
    print(raw_dir)

    if validation["all_required_ok"]:
        print("\n[DONE] USDA Step 5 unified LIGHT completed and validated.")
        return 0

    print("\n[DONE WITH WARNINGS] Review validation report.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
