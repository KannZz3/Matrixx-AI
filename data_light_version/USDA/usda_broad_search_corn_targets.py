#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
USDA MARS Broad Search for CORN_DECATUR and CORN_GULF.

Purpose
-------
Continue expanding USDA MARS discovery for:
  1. CORN_DECATUR
  2. CORN_GULF

This script uses a broader search strategy over the USDA MARS reports index and fetched
Report Detail payloads. It searches titles, locations, and nested result rows for keywords:

Decatur
Illinois
Central Illinois
Interior Illinois
Processor
Country Elevator
Gulf
New Orleans
Louisiana
Export
Mississippi River
Barge
Terminal
Corn
Daily Grain Bids

What it does
------------
1. Loads existing raw JSON files from C:\Users\<YOU>\usda_output\raw.
2. Recursively expands nested results[] records.
3. Searches existing records for strict and loose CORN_DECATUR / CORN_GULF matches.
4. If USDA_MARS_API_KEY is set, fetches the USDA reports index.
5. Selects broader candidate report slugs based on report title / slug / metadata.
6. Fetches Report Detail for candidate slugs.
7. Saves new raw payloads.
8. Re-runs search over both old and new raw payloads.
9. Outputs candidate JSONL, CSV, and a human-readable report.

Authentication
--------------
PowerShell:
    $env:USDA_MARS_API_KEY="YOUR_KEY"

Run
---
    python usda_broad_search_corn_targets.py

Or:
    python usda_broad_search_corn_targets.py --max-slugs 1000 --days 60

If needed:
    python usda_broad_search_corn_targets.py --deep-all --max-slugs 2000
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2"

TARGETS = ["CORN_DECATUR", "CORN_GULF"]

BROAD_KEYWORDS = [
    "decatur",
    "illinois",
    "central illinois",
    "interior illinois",
    "processor",
    "processors",
    "country elevator",
    "country elevators",
    "gulf",
    "new orleans",
    "louisiana",
    "export",
    "mississippi river",
    "barge",
    "terminal",
    "terminals",
    "corn",
    "daily grain bids",
    "grain bids",
]

DECATUR_STRICT = [
    "decatur",
]

DECATUR_LOOSE = [
    "illinois",
    "central illinois",
    "interior illinois",
    "processor",
    "processors",
    "country elevator",
    "country elevators",
    "illinois river",
]

GULF_STRICT = [
    "gulf",
    "new orleans",
    "louisiana gulf",
    "export elevator",
    "export elevators",
]

GULF_LOOSE = [
    "louisiana",
    "mississippi river",
    "barge",
    "barge loading",
    "terminal",
    "terminals",
    "river",
    "export",
]


def norm_text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def lower(x: Any) -> str:
    return norm_text(x).lower()


def safe_filename(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s))
    return s.strip("_")[:120] or "unknown"


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


def get_field(row: Dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
    lower_map = {str(k).lower(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None:
            return row[key]
    return None


def compute_price_value(row: Dict[str, Any]) -> Optional[float]:
    avg = parse_float(get_field(row, "avg_price", "Avg Price", "average_price"))
    if avg is not None:
        return avg

    pmin = parse_float(get_field(row, "price Min", "price_min", "price min"))
    pmax = parse_float(get_field(row, "price Max", "price_max", "price max"))

    if pmin is not None and pmax is not None:
        return (pmin + pmax) / 2.0
    if pmin is not None:
        return pmin
    if pmax is not None:
        return pmax
    return None


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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


def flatten_text(obj: Any, max_chars: int = 30000) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False)
    except Exception:
        s = str(obj)
    s = s[:max_chars]
    return s.lower()


def text_hit(text: str, keywords: List[str]) -> List[str]:
    return [kw for kw in keywords if kw in text]


def date_to_mdy(date_obj: datetime) -> str:
    return date_obj.strftime("%m/%d/%Y")


def fetch_json(
    url: str,
    api_key: str,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
    sleep: float = 0.75,
) -> Any:
    auth = (api_key, "")
    last_error = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params or {}, auth=auth, timeout=60)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep * (2 ** attempt))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            time.sleep(sleep * (2 ** attempt))
    raise RuntimeError(f"Failed GET {url} params={params}: {last_error}")


def get_reports_index(api_key: str) -> List[Dict[str, Any]]:
    payload = fetch_json(f"{BASE_URL}/reports", api_key=api_key)
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("results", "data", "reports"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def report_slug_id(row: Dict[str, Any]) -> Optional[str]:
    for key in ("slug_id", "slugId", "id", "slug"):
        if key in row and row[key] not in (None, ""):
            return str(row[key])
    return None


def report_title_text(row: Dict[str, Any]) -> str:
    fields = [
        "slug_id", "slug_name", "report_title", "title", "name",
        "office_name", "office_city", "office_state",
        "market_location_name", "market_location_city", "market_location_state",
        "market_type", "commodity", "grp", "cat", "category"
    ]
    parts = [norm_text(row.get(k)) for k in fields if row.get(k) not in (None, "")]
    if not parts:
        parts = [json.dumps(row, ensure_ascii=False)[:2000]]
    return " | ".join(parts).lower()


def select_candidate_slugs(
    reports: List[Dict[str, Any]],
    max_slugs: int,
    deep_all: bool = False,
) -> List[Tuple[str, int, str, List[str]]]:
    """
    Return list of (slug_id, score, title_text, hits)
    """
    candidates = []
    seen = set()

    for row in reports:
        sid = report_slug_id(row)
        if not sid or sid in seen:
            continue

        text = report_title_text(row)
        hits = text_hit(text, BROAD_KEYWORDS)

        # score title/location hits; corn + grain bids are important but not sufficient
        score = 0
        for h in hits:
            if h in ("corn", "daily grain bids", "grain bids"):
                score += 10
            elif h in ("decatur", "gulf", "new orleans", "louisiana", "illinois"):
                score += 25
            elif h in ("central illinois", "interior illinois", "mississippi river", "barge", "terminal", "export"):
                score += 18
            else:
                score += 8

        # broad report titles like "Daily Grain Bids"
        if "grain" in text and ("bid" in text or "bids" in text):
            score += 15
        if "corn" in text:
            score += 15

        if deep_all:
            score = max(score, 1)

        if score > 0:
            seen.add(sid)
            candidates.append((sid, score, text[:500], hits))

    candidates.sort(key=lambda x: (-x[1], int(x[0]) if str(x[0]).isdigit() else 999999))
    return candidates[:max_slugs]


def fetch_report_detail_for_slug(
    api_key: str,
    slug: str,
    begin_date: str,
    end_date: str,
    out_raw_dir: Path,
) -> List[Path]:
    """
    Fetch both direct and section-based Report Detail payloads because USDA MARS
    endpoint behavior can differ by report.
    """
    saved = []

    attempts = [
        (
            f"{BASE_URL}/reports/{slug}",
            {
                "report_begin_date": begin_date,
                "report_end_date": end_date,
                "format": "json",
            },
            f"usda_mars_broad_{slug}_direct_raw.json",
        ),
        (
            f"{BASE_URL}/reports/{slug}/Report%20Detail",
            {
                "report_begin_date": begin_date,
                "report_end_date": end_date,
                "format": "json",
            },
            f"usda_mars_broad_{slug}_Report_Detail_raw.json",
        ),
    ]

    for url, params, fname in attempts:
        try:
            payload = fetch_json(url, api_key=api_key, params=params)
            path = out_raw_dir / fname
            save_json(path, payload)
            saved.append(path)
            rows = list(recursively_find_results(payload))
            print(f"  fetched slug={slug} rows={len(rows):5d} file={fname}")
        except Exception as exc:
            print(f"  [WARN] failed slug={slug} url={url}: {exc}")

    return saved


def row_combined_text(row: Dict[str, Any]) -> str:
    fields = [
        "report_title", "market_location_name", "market_location_city", "market_location_state",
        "office_name", "office_city", "office_state",
        "trade_loc", "trade Loc", "location_State", "location_City",
        "commodity", "class", "grade", "delivery_point", "quote_type", "sale Type", "sale_type",
        "freight", "trans_mode", "desc", "application"
    ]
    return " | ".join(norm_text(get_field(row, f)) for f in fields).lower()


def classify_candidate(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    commodity = lower(get_field(row, "commodity"))
    if commodity != "corn":
        return []

    price_unit = lower(get_field(row, "price_unit", "price unit"))
    sale_type = lower(get_field(row, "sale Type", "sale_type", "sale type"))
    value = compute_price_value(row)
    text = row_combined_text(row)

    if value is None:
        return []

    base = {
        "report_date": norm_text(get_field(row, "report_date")),
        "published_date": norm_text(get_field(row, "published_date")),
        "slug_id": get_field(row, "slug_id"),
        "slug_name": norm_text(get_field(row, "slug_name")),
        "report_title": norm_text(get_field(row, "report_title")),
        "market_location_name": norm_text(get_field(row, "market_location_name")),
        "market_location_city": norm_text(get_field(row, "market_location_city")),
        "market_location_state": norm_text(get_field(row, "market_location_state")),
        "trade_loc": norm_text(get_field(row, "trade_loc", "trade Loc")),
        "location_state": norm_text(get_field(row, "location_State")),
        "location_city": norm_text(get_field(row, "location_City")),
        "commodity": norm_text(get_field(row, "commodity")),
        "class": norm_text(get_field(row, "class")),
        "grade": norm_text(get_field(row, "grade")),
        "delivery_point": norm_text(get_field(row, "delivery_point")),
        "quote_type": norm_text(get_field(row, "quote_type")),
        "sale_type": norm_text(get_field(row, "sale Type", "sale_type", "sale type")),
        "basis_unit": norm_text(get_field(row, "basis_unit")),
        "basis_min": get_field(row, "basis Min", "basis_min"),
        "basis_max": get_field(row, "basis Max", "basis_max"),
        "price_unit": norm_text(get_field(row, "price_unit", "price unit")),
        "price_min": get_field(row, "price Min", "price_min"),
        "price_max": get_field(row, "price Max", "price_max"),
        "avg_price": get_field(row, "avg_price"),
        "value": value,
        "unit_normalized": "USD_PER_BUSHEL" if price_unit in {"$ per bushel", "dollars per bushel", "usd per bushel"} else None,
        "row_text_excerpt": text[:1200],
    }

    candidates = []

    # Common validity checks for grain bid records
    valid_bid_price = (
        price_unit in {"$ per bushel", "dollars per bushel", "usd per bushel"}
        and sale_type == "bid"
    )

    # Decatur strict and loose
    decatur_hits = text_hit(text, DECATUR_STRICT)
    decatur_loose_hits = text_hit(text, DECATUR_LOOSE)
    if decatur_hits and valid_bid_price:
        rec = dict(base)
        rec.update({
            "target": "CORN_DECATUR",
            "match_type": "STRICT",
            "match_hits": decatur_hits,
            "score": 100 + 10 * len(decatur_hits) + 3 * len(decatur_loose_hits),
        })
        candidates.append(rec)
    elif decatur_loose_hits and valid_bid_price:
        rec = dict(base)
        rec.update({
            "target": "CORN_DECATUR",
            "match_type": "LOOSE",
            "match_hits": decatur_loose_hits,
            "score": 55 + 3 * len(decatur_loose_hits),
        })
        candidates.append(rec)

    # Gulf strict and loose
    gulf_hits = text_hit(text, GULF_STRICT)
    gulf_loose_hits = text_hit(text, GULF_LOOSE)
    if gulf_hits and valid_bid_price:
        rec = dict(base)
        rec.update({
            "target": "CORN_GULF",
            "match_type": "STRICT",
            "match_hits": gulf_hits,
            "score": 100 + 10 * len(gulf_hits) + 3 * len(gulf_loose_hits),
        })
        candidates.append(rec)
    elif gulf_loose_hits and valid_bid_price:
        rec = dict(base)
        rec.update({
            "target": "CORN_GULF",
            "match_type": "LOOSE",
            "match_hits": gulf_loose_hits,
            "score": 55 + 3 * len(gulf_loose_hits),
        })
        candidates.append(rec)

    return candidates


def scan_raw_dir(raw_dir: Path) -> Tuple[List[Dict[str, Any]], int, int]:
    candidates: List[Dict[str, Any]] = []
    files_with_results = 0
    total_rows = 0

    for path in sorted(raw_dir.glob("*.json")):
        try:
            payload = load_json(path)
        except Exception:
            continue

        rows = list(recursively_find_results(payload))
        if rows:
            files_with_results += 1

        for row in rows:
            total_rows += 1
            for rec in classify_candidate(row):
                rec["raw_file"] = path.name
                candidates.append(rec)

    # de-dup likely duplicates from FULL and Report Detail raw files
    seen = set()
    deduped = []
    for rec in candidates:
        key = (
            rec.get("target"),
            rec.get("match_type"),
            rec.get("slug_id"),
            rec.get("report_date"),
            rec.get("report_title"),
            rec.get("market_location_name"),
            rec.get("trade_loc"),
            rec.get("location_state"),
            rec.get("location_city"),
            rec.get("delivery_point"),
            rec.get("price_min"),
            rec.get("price_max"),
            rec.get("avg_price"),
        )
        if key not in seen:
            seen.add(key)
            deduped.append(rec)

    deduped.sort(key=lambda r: (-int(r.get("score", 0)), r.get("target", ""), r.get("report_date", ""), str(r.get("slug_id", ""))))
    return deduped, files_with_results, total_rows


def write_outputs(out_dir: Path, candidates: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = out_dir / "corn_decatur_gulf_candidates.jsonl"
    csv_path = out_dir / "corn_decatur_gulf_candidates.csv"
    summary_path = out_dir / "corn_decatur_gulf_summary.json"
    report_path = out_dir / "corn_decatur_gulf_report.txt"

    with jsonl_path.open("w", encoding="utf-8") as f:
        for rec in candidates:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    fieldnames = [
        "target", "match_type", "score", "match_hits",
        "report_date", "published_date", "slug_id", "slug_name", "report_title",
        "market_location_name", "market_location_city", "market_location_state",
        "trade_loc", "location_state", "location_city",
        "commodity", "class", "grade", "delivery_point",
        "quote_type", "sale_type", "basis_unit", "basis_min", "basis_max",
        "price_unit", "price_min", "price_max", "avg_price", "value",
        "unit_normalized", "raw_file",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in candidates:
            row = {k: rec.get(k) for k in fieldnames}
            row["match_hits"] = "|".join(rec.get("match_hits", []))
            writer.writerow(row)

    by_target = {t: {"total": 0, "strict": 0, "loose": 0} for t in TARGETS}
    top_by_target = {t: [] for t in TARGETS}
    for rec in candidates:
        t = rec["target"]
        by_target[t]["total"] += 1
        if rec["match_type"] == "STRICT":
            by_target[t]["strict"] += 1
        else:
            by_target[t]["loose"] += 1
    for t in TARGETS:
        top_by_target[t] = [r for r in candidates if r["target"] == t][:20]

    summary = {
        **meta,
        "target_counts": by_target,
        "top_by_target": top_by_target,
        "output_files": {
            "jsonl": str(jsonl_path),
            "csv": str(csv_path),
            "summary": str(summary_path),
            "report": str(report_path),
        }
    }
    save_json(summary_path, summary)

    lines = []
    lines.append("USDA Broad Search Report: CORN_DECATUR / CORN_GULF")
    lines.append("=" * 80)
    lines.append(f"Generated at: {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    lines.append(f"Raw dir: {meta.get('raw_dir')}")
    lines.append(f"Files scanned: {meta.get('raw_files_scanned')}")
    lines.append(f"Files with results[]: {meta.get('files_with_results')}")
    lines.append(f"Scanned results[] rows: {meta.get('scanned_result_rows')}")
    lines.append(f"API key used: {meta.get('api_key_used')}")
    lines.append(f"Reports index rows: {meta.get('reports_index_rows')}")
    lines.append(f"Candidate slugs fetched: {meta.get('candidate_slugs_fetched')}")
    lines.append("")

    for t in TARGETS:
        c = by_target[t]
        status = "FOUND_STRICT" if c["strict"] > 0 else ("FOUND_LOOSE_ONLY" if c["loose"] > 0 else "NOT_FOUND")
        lines.append(f"{t}: {status} | total={c['total']} strict={c['strict']} loose={c['loose']}")
        for rec in top_by_target[t][:10]:
            lines.append(
                f"  score={rec['score']:3d} {rec['match_type']:<6} "
                f"date={rec.get('report_date')} value={rec.get('value')} "
                f"slug={rec.get('slug_id')} title={rec.get('report_title')} "
                f"loc={rec.get('market_location_name')} trade={rec.get('trade_loc')} "
                f"delivery={rec.get('delivery_point')} hits={rec.get('match_hits')}"
            )
        lines.append("")

    lines.append("Output files:")
    lines.append(str(jsonl_path))
    lines.append(str(csv_path))
    lines.append(str(summary_path))
    lines.append(str(report_path))

    report_path.write_text("\n".join(lines), encoding="utf-8")

    print("\n" + "=" * 80)
    print("USDA Broad Search Summary")
    print("=" * 80)
    for t in TARGETS:
        c = by_target[t]
        status = "FOUND_STRICT" if c["strict"] > 0 else ("FOUND_LOOSE_ONLY" if c["loose"] > 0 else "NOT_FOUND")
        print(f"{t:<14} | {status:<16} | total={c['total']:5d} strict={c['strict']:5d} loose={c['loose']:5d}")
        for rec in top_by_target[t][:5]:
            print(
                f"   score={rec['score']:3d} {rec['match_type']:<6} "
                f"value={rec.get('value')} date={rec.get('report_date')} slug={rec.get('slug_id')} "
                f"title={rec.get('report_title')} hits={rec.get('match_hits')}"
            )

    print("\nSaved outputs:")
    print(report_path)
    print(summary_path)
    print(csv_path)
    print(jsonl_path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default=str(Path.home() / "usda_output" / "raw"))
    parser.add_argument("--out-dir", default=str(Path.home() / "usda_output" / "corn_broad_search"))
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--max-slugs", type=int, default=1000)
    parser.add_argument("--deep-all", action="store_true")
    parser.add_argument("--skip-api", action="store_true", help="Only scan existing raw files.")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    broad_raw_dir = raw_dir / "_broad_search_new"
    raw_dir.mkdir(parents=True, exist_ok=True)
    broad_raw_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("USDA MARS Broad Search: CORN_DECATUR / CORN_GULF")
    print("=" * 80)
    print(f"Raw dir:     {raw_dir}")
    print(f"New raw dir: {broad_raw_dir}")
    print(f"Out dir:     {out_dir}")
    print(f"Days:        {args.days}")
    print(f"Max slugs:   {args.max_slugs}")
    print(f"Deep all:    {args.deep_all}")

    api_key = os.getenv("USDA_MARS_API_KEY", "").strip()
    reports_index_rows = 0
    candidate_slugs_fetched = 0

    if not args.skip_api and api_key:
        print("\n[API] Fetching USDA reports index...")
        try:
            reports = get_reports_index(api_key)
            reports_index_rows = len(reports)
            print(f"[API] Reports index rows: {reports_index_rows}")

            candidates = select_candidate_slugs(reports, max_slugs=args.max_slugs, deep_all=args.deep_all)
            print(f"[API] Broad candidate slugs selected: {len(candidates)}")

            # Save candidate slug list for debugging
            with (out_dir / "broad_candidate_slugs.csv").open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["slug_id", "score", "hits", "text"])
                for sid, score, text, hits in candidates:
                    writer.writerow([sid, score, "|".join(hits), text])

            end = datetime.now()
            begin = end - timedelta(days=args.days)
            begin_date = date_to_mdy(begin)
            end_date = date_to_mdy(end)

            print(f"[API] Fetching Report Detail payloads from {begin_date} to {end_date}...")
            for i, (sid, score, text, hits) in enumerate(candidates, 1):
                print(f"[{i:04d}/{len(candidates):04d}] slug={sid} score={score} hits={hits[:5]}")
                saved = fetch_report_detail_for_slug(
                    api_key=api_key,
                    slug=sid,
                    begin_date=begin_date,
                    end_date=end_date,
                    out_raw_dir=broad_raw_dir,
                )
                candidate_slugs_fetched += 1 if saved else 0

        except Exception as exc:
            print(f"[WARN] API broad fetch failed: {exc}")
            print("Continuing with existing raw file scan only.")
    else:
        print("\n[API] Skipped. Set USDA_MARS_API_KEY or remove --skip-api to fetch more reports.")

    print("\n[SCAN] Scanning raw files recursively...")
    # Scan both root raw files and new broad raw files because broad_raw_dir is nested under raw_dir.
    candidates, files_with_results, scanned_rows = scan_raw_dir(raw_dir)

    meta = {
        "raw_dir": str(raw_dir),
        "out_dir": str(out_dir),
        "raw_files_scanned": len(list(raw_dir.rglob("*.json"))),
        "files_with_results": files_with_results,
        "scanned_result_rows": scanned_rows,
        "api_key_used": bool(api_key and not args.skip_api),
        "reports_index_rows": reports_index_rows,
        "candidate_slugs_fetched": candidate_slugs_fetched,
        "days": args.days,
        "max_slugs": args.max_slugs,
        "deep_all": args.deep_all,
        "broad_keywords": BROAD_KEYWORDS,
        "decatur_strict": DECATUR_STRICT,
        "decatur_loose": DECATUR_LOOSE,
        "gulf_strict": GULF_STRICT,
        "gulf_loose": GULF_LOOSE,
    }

    write_outputs(out_dir, candidates, meta)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
