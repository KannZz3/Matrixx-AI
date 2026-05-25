#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Baker Hughes Step 7 Rig Count Local Parser LIGHT v4

Purpose
-------
Complete Step 7 using the local Baker Hughes Excel file:

    05-22-2026 North_America Rig_Count Report.xlsx
    ↓
    parse NAM Weekly long-table data
    ↓
    filter Country = UNITED STATES
    ↓
    aggregate Rig Count Value by US_PublishDate and DrillFor
    ↓
    compute oil / gas / misc / total rig counts
    ↓
    compute week-over-week deltas
    ↓
    flag energy scarcity trigger when oil/gas/total rigs fall by 10 or more WoW
    ↓
    output normalized JSON / JSONL / CSV + latest payload + validation report

No API key is required.
No web request is made in v4.

Default input search:
    1. --local-file if provided
    2. current working directory / "05-22-2026 North_America Rig_Count Report.xlsx"
    3. ~/Downloads / "05-22-2026 North_America Rig_Count Report.xlsx"
    4. /mnt/data / "05-22-2026 North_America Rig_Count Report.xlsx" when running inside ChatGPT sandbox

Default output:
    C:\Users\<YOU>\baker_hughes_output\step7_rig_count_light_v4

Default retention:
    Most recent 260 weekly records, approximately 5 years.

Run:
    python baker_hughes_step7_rig_count_light_v4.py

Optional:
    python baker_hughes_step7_rig_count_light_v4.py --local-file "C:\Users\78432\Downloads\05-22-2026 North_America Rig_Count Report.xlsx"

Dependencies:
    pandas
    openpyxl

If missing:
    pip install pandas openpyxl

Step 7 trigger rule
-------------------
    oil_rigs_delta_1w <= -10   -> oil scarcity trigger
    gas_rigs_delta_1w <= -10   -> gas scarcity trigger
    total_rigs_delta_1w <= -10 -> total scarcity trigger
"""

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import pandas as pd
except Exception:
    pd = None


DEFAULT_FILE_NAME = "05-22-2026 North_America Rig_Count Report.xlsx"
DEFAULT_OUT_DIR = Path.home() / "baker_hughes_output" / "step7_rig_count_light_v4"
TARGET_TRIGGER_DROP = -10
DEFAULT_MAX_STALE_DAYS = 21
DEFAULT_LIMIT_WEEKS = 260

SHEET_NAME = "NAM Weekly"
REQUIRED_COLUMNS = [
    "Country",
    "DrillFor",
    "US_PublishDate",
    "Rig Count Value",
]


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
        "source_id", "instrument", "timestamp", "data_type",
        "oil_rigs", "gas_rigs", "misc_rigs", "total_rigs",
        "oil_rigs_delta_1w", "gas_rigs_delta_1w", "misc_rigs_delta_1w", "total_rigs_delta_1w",
        "energy_scarcity_trigger", "oil_scarcity_trigger", "gas_scarcity_trigger", "total_scarcity_trigger",
        "directional_bias", "confidence", "trigger_reason",
        "unit", "frequency", "tos_status", "gatekeeper_cleared", "gatekeeper_id",
        "raw_source_url", "raw_file_name", "source_sheet", "normalized_at",
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
            out = {}
            for k in keys:
                v = row.get(k)
                out[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
            writer.writerow(out)


def write_validation_txt(path: Path, report: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    lines = []
    lines.append("Baker Hughes Step 7 Rig Count Validation Report v4")
    lines.append("=" * 88)
    lines.append(f"Validated at: {report.get('validated_at')}")
    lines.append(f"All required OK: {report.get('all_required_ok')}")
    lines.append(f"Local file: {report.get('local_file')}")
    lines.append(f"Sheet: {report.get('source_sheet')}")
    lines.append(f"Record count: {report.get('record_count')}")
    lines.append(f"Latest date: {report.get('latest_date')}")
    lines.append("")
    if report.get("latest"):
        latest = report["latest"]
        lines.append("Latest:")
        lines.append(f"  oil_rigs={latest.get('oil_rigs')} delta={latest.get('oil_rigs_delta_1w')}")
        lines.append(f"  gas_rigs={latest.get('gas_rigs')} delta={latest.get('gas_rigs_delta_1w')}")
        lines.append(f"  misc_rigs={latest.get('misc_rigs')} delta={latest.get('misc_rigs_delta_1w')}")
        lines.append(f"  total_rigs={latest.get('total_rigs')} delta={latest.get('total_rigs_delta_1w')}")
        lines.append(f"  energy_scarcity_trigger={latest.get('energy_scarcity_trigger')}")
        lines.append(f"  trigger_reason={latest.get('trigger_reason')}")
        lines.append("")
    if report.get("errors"):
        lines.append("Errors:")
        for e in report["errors"]:
            lines.append(f"  - {e}")
    if report.get("warnings"):
        lines.append("Warnings:")
        for w in report["warnings"]:
            lines.append(f"  - {w}")
    path.write_text("\n".join(lines), encoding="utf-8")


def require_dependencies() -> None:
    if pd is None:
        print("[ERROR] Missing Python packages: pandas, openpyxl")
        print("Install with:")
        print("    pip install pandas openpyxl")
        raise SystemExit(2)


def resolve_local_file(local_file: str) -> Path:
    candidates = []

    if local_file:
        candidates.append(Path(local_file))

    candidates.append(Path.cwd() / DEFAULT_FILE_NAME)
    candidates.append(Path.home() / "Downloads" / DEFAULT_FILE_NAME)
    candidates.append(Path("/mnt/data") / DEFAULT_FILE_NAME)

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                return p
        except Exception:
            pass

    msg = [
        "Could not find the Baker Hughes local Excel file.",
        "Tried:",
    ]
    for p in candidates:
        msg.append(f"  - {p}")
    msg.append("")
    msg.append("Use:")
    msg.append(f'  python baker_hughes_step7_rig_count_light_v4.py --local-file "C:\\Users\\78432\\Downloads\\{DEFAULT_FILE_NAME}"')
    raise FileNotFoundError("\n".join(msg))


def normalize_drillfor(x: Any) -> str:
    s = str(x).strip().lower()
    if s == "oil":
        return "Oil"
    if s == "gas":
        return "Gas"
    if "misc" in s:
        return "Miscellaneous"
    if "geo" in s:
        return "Miscellaneous"
    return str(x).strip()


def parse_nam_weekly_long_table(local_file: Path, debug_dir: Path) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Parse the Baker Hughes NAM Weekly long-table structure.

    Expected sheet:
      NAM Weekly

    Expected columns:
      Country
      DrillFor
      US_PublishDate
      Rig Count Value

    Aggregation:
      Country == UNITED STATES
      DrillFor in Oil/Gas/Miscellaneous
      group by US_PublishDate and DrillFor
      sum Rig Count Value
    """
    ensure_dir(debug_dir)

    # Header row is row 11 in Excel, zero-index header=10.
    # This is the actual table header for the uploaded 05-22-2026 workbook.
    df = pd.read_excel(local_file, sheet_name=SHEET_NAME, header=10, engine="openpyxl")

    # Save raw column preview for audit.
    preview_path = debug_dir / "nam_weekly_preview.csv"
    df.head(100).to_csv(preview_path, index=False)

    cols = [str(c).strip() for c in df.columns]
    df.columns = cols

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"NAM Weekly sheet is missing required columns: {missing_cols}. Columns seen: {cols}")

    work = df.copy()
    work["Country"] = work["Country"].astype(str).str.strip().str.upper()
    work["DrillFor"] = work["DrillFor"].map(normalize_drillfor)
    work["US_PublishDate"] = pd.to_datetime(work["US_PublishDate"], errors="coerce")
    work["Rig Count Value"] = pd.to_numeric(work["Rig Count Value"], errors="coerce")

    before_filter_rows = len(work)

    work = work[
        (work["Country"] == "UNITED STATES")
        & (work["DrillFor"].isin(["Oil", "Gas", "Miscellaneous"]))
        & (work["US_PublishDate"].notna())
        & (work["Rig Count Value"].notna())
    ].copy()

    work["timestamp"] = work["US_PublishDate"].dt.date.astype(str)

    grouped_long = (
        work.groupby(["timestamp", "DrillFor"], as_index=False)["Rig Count Value"]
        .sum()
        .sort_values(["timestamp", "DrillFor"])
    )

    grouped_long.to_csv(debug_dir / "nam_weekly_us_drillfor_grouped_long.csv", index=False)

    wide = (
        grouped_long.pivot(index="timestamp", columns="DrillFor", values="Rig Count Value")
        .fillna(0)
        .reset_index()
        .sort_values("timestamp")
    )

    # Ensure columns exist.
    for c in ["Oil", "Gas", "Miscellaneous"]:
        if c not in wide.columns:
            wide[c] = 0

    records = []
    for _, row in wide.iterrows():
        oil = int(round(float(row["Oil"])))
        gas = int(round(float(row["Gas"])))
        misc = int(round(float(row["Miscellaneous"])))
        total = oil + gas + misc
        records.append({
            "timestamp": str(row["timestamp"]),
            "oil_rigs": oil,
            "gas_rigs": gas,
            "misc_rigs": misc,
            "total_rigs": total,
            "source_sheet": SHEET_NAME,
        })

    metadata = {
        "parser": "NAM_WEEKLY_LONG_TABLE_V4",
        "source_sheet": SHEET_NAME,
        "header_row_zero_indexed": 10,
        "required_columns": REQUIRED_COLUMNS,
        "columns_seen": cols,
        "rows_before_filter": before_filter_rows,
        "rows_after_us_oil_gas_misc_filter": len(work),
        "weekly_record_count": len(records),
        "earliest_date": records[0]["timestamp"] if records else None,
        "latest_date": records[-1]["timestamp"] if records else None,
        "drillfor_values_seen_after_normalization": sorted(work["DrillFor"].dropna().unique().tolist()),
        "country_filter": "UNITED STATES",
        "aggregation": "sum Rig Count Value by US_PublishDate and DrillFor",
    }

    return records, metadata


def add_deltas_and_triggers(
    rows: List[Dict[str, Any]],
    raw_source_url: str,
    raw_file_name: str,
    gatekeeper_id: str,
) -> List[Dict[str, Any]]:
    rows = sorted(rows, key=lambda r: r["timestamp"])
    out = []

    for i, row in enumerate(rows):
        prev = rows[i - 1] if i > 0 else None

        oil_delta = row["oil_rigs"] - prev["oil_rigs"] if prev else None
        gas_delta = row["gas_rigs"] - prev["gas_rigs"] if prev else None
        misc_delta = row["misc_rigs"] - prev["misc_rigs"] if prev else None
        total_delta = row["total_rigs"] - prev["total_rigs"] if prev else None

        oil_trigger = oil_delta is not None and oil_delta <= TARGET_TRIGGER_DROP
        gas_trigger = gas_delta is not None and gas_delta <= TARGET_TRIGGER_DROP
        total_trigger = total_delta is not None and total_delta <= TARGET_TRIGGER_DROP
        energy_trigger = oil_trigger or gas_trigger or total_trigger

        reasons = []
        if oil_trigger:
            reasons.append(f"Oil rigs fell by {abs(oil_delta)} WoW.")
        if gas_trigger:
            reasons.append(f"Gas rigs fell by {abs(gas_delta)} WoW.")
        if total_trigger:
            reasons.append(f"Total rigs fell by {abs(total_delta)} WoW.")
        if not reasons and prev is None:
            reasons.append("Insufficient previous week history to compute WoW trigger.")
        elif not reasons:
            reasons.append("No oil/gas/total rig category fell by 10 or more WoW.")

        confidence = "medium" if energy_trigger else "low"
        directional_bias = "bullish_energy_supply_risk" if energy_trigger else "neutral_energy_supply_signal"

        rec = {
            "source_id": "BAKER_HUGHES_RIG_COUNT_WEEKLY",
            "instrument": "US_RIG_COUNT",
            "timestamp": row["timestamp"],
            "data_type": "RIG_COUNT",

            "oil_rigs": row["oil_rigs"],
            "gas_rigs": row["gas_rigs"],
            "misc_rigs": row["misc_rigs"],
            "total_rigs": row["total_rigs"],

            "oil_rigs_delta_1w": oil_delta,
            "gas_rigs_delta_1w": gas_delta,
            "misc_rigs_delta_1w": misc_delta,
            "total_rigs_delta_1w": total_delta,

            "energy_scarcity_trigger": energy_trigger,
            "oil_scarcity_trigger": oil_trigger,
            "gas_scarcity_trigger": gas_trigger,
            "total_scarcity_trigger": total_trigger,
            "trigger_components": {
                "oil_scarcity_trigger": oil_trigger,
                "gas_scarcity_trigger": gas_trigger,
                "total_scarcity_trigger": total_trigger,
            },
            "trigger_threshold": TARGET_TRIGGER_DROP,
            "trigger_reason": " ".join(reasons),

            "directional_bias": directional_bias,
            "confidence": confidence,
            "unit": "RIG_COUNT",
            "frequency": "WEEKLY",

            "tos_status": "GO",
            "gatekeeper_cleared": True,
            "gatekeeper_id": gatekeeper_id,
            "raw_source_url": raw_source_url,
            "raw_file_name": raw_file_name,
            "source_sheet": row.get("source_sheet"),
            "normalized_at": now_utc(),
        }
        out.append(rec)

    return out


def validate_records(
    records: List[Dict[str, Any]],
    local_file: Path,
    parse_meta: Dict[str, Any],
    max_stale_days: int,
) -> Dict[str, Any]:
    errors = []
    warnings = []

    latest = records[-1] if records else None

    if not records:
        errors.append("No normalized rig count records produced.")
    if len(records) < 2:
        errors.append("At least two weekly records are required to compute WoW deltas.")

    # Sheet identity hard check.
    if parse_meta.get("source_sheet") != SHEET_NAME:
        errors.append(f"Selected sheet must be {SHEET_NAME}, observed={parse_meta.get('source_sheet')}")
    if parse_meta.get("parser") != "NAM_WEEKLY_LONG_TABLE_V4":
        errors.append("Parser identity is not NAM_WEEKLY_LONG_TABLE_V4.")
    if parse_meta.get("rows_after_us_oil_gas_misc_filter", 0) <= 0:
        errors.append("No United States Oil/Gas/Miscellaneous NAM Weekly rows after filter.")

    # Data row checks.
    seen_dates = set()
    for r in records:
        ts = r.get("timestamp")
        if not ts:
            errors.append("Missing timestamp in normalized record.")
            break
        if ts in seen_dates:
            errors.append(f"Duplicate timestamp in normalized records: {ts}")
            break
        seen_dates.add(ts)

        for field in ["oil_rigs", "gas_rigs", "misc_rigs", "total_rigs"]:
            val = r.get(field)
            if not isinstance(val, int) or val < 0:
                errors.append(f"{field} must be a non-negative integer at {ts}; observed={val}")
                break

        if r.get("total_rigs") != r.get("oil_rigs", 0) + r.get("gas_rigs", 0) + r.get("misc_rigs", 0):
            errors.append(f"total_rigs must equal oil_rigs + gas_rigs + misc_rigs at {ts}")
            break

        if r.get("energy_scarcity_trigger") != (
            bool(r.get("oil_scarcity_trigger")) or bool(r.get("gas_scarcity_trigger")) or bool(r.get("total_scarcity_trigger"))
        ):
            errors.append(f"energy_scarcity_trigger inconsistent with components at {ts}")
            break

        for delta_field, trigger_field in [
            ("oil_rigs_delta_1w", "oil_scarcity_trigger"),
            ("gas_rigs_delta_1w", "gas_scarcity_trigger"),
            ("total_rigs_delta_1w", "total_scarcity_trigger"),
        ]:
            delta = r.get(delta_field)
            expected = delta is not None and delta <= TARGET_TRIGGER_DROP
            if r.get(trigger_field) != expected:
                errors.append(f"{trigger_field} inconsistent with {delta_field} at {ts}")
                break

    # Weekly frequency hard check.
    if len(records) >= 2:
        dates = [datetime.fromisoformat(r["timestamp"]).date() for r in records]
        gaps = [(dates[i] - dates[i - 1]).days for i in range(1, len(dates))]
        recent_gaps = gaps[-52:] if len(gaps) >= 52 else gaps
        weekly_ok_ratio = sum(1 for g in recent_gaps if 5 <= g <= 10) / len(recent_gaps)
        latest_gap = gaps[-1]

        if weekly_ok_ratio < 0.80:
            errors.append(f"Weekly frequency check failed: only {weekly_ok_ratio:.2%} recent gaps are 5-10 days.")
        if not (5 <= latest_gap <= 10):
            errors.append(f"Latest date gap must be 5-10 days, observed={latest_gap} days.")

    # Latest hard checks.
    if latest:
        latest_date = datetime.fromisoformat(latest["timestamp"]).date()
        lag_days = (datetime.now().date() - latest_date).days
        if lag_days > max_stale_days:
            errors.append(f"Latest rig count date is {lag_days} days old, exceeding max_stale_days={max_stale_days}.")

        for field in ["oil_rigs_delta_1w", "gas_rigs_delta_1w", "total_rigs_delta_1w"]:
            if latest.get(field) is None:
                errors.append(f"Latest record missing computed delta: {field}")

        # Reasonable U.S. range sanity check. It is not a financial model, just a parser guard.
        if not (100 <= latest.get("total_rigs", 0) <= 1500):
            errors.append(f"Latest total_rigs looks implausible for U.S. weekly rig count: {latest.get('total_rigs')}")

    return {
        "validated_at": now_utc(),
        "stage": "BAKER_HUGHES_STEP7_RIG_COUNT_VALIDATION_LIGHT_V4",
        "all_required_ok": len(errors) == 0,
        "local_file": str(local_file),
        "source_sheet": SHEET_NAME,
        "record_count": len(records),
        "earliest_date": records[0]["timestamp"] if records else None,
        "latest_date": latest.get("timestamp") if latest else None,
        "latest": latest,
        "parse_metadata": parse_meta,
        "trigger_rule": "oil/gas/total rig count delta <= -10 triggers energy_scarcity_trigger",
        "validation_rules": {
            "local_only": "No web request is made in v4.",
            "sheet_identity": "Must parse NAM Weekly long-table sheet.",
            "country_filter": "Country == UNITED STATES.",
            "aggregation": "Sum Rig Count Value by US_PublishDate and DrillFor.",
            "required_fields": ["timestamp", "oil_rigs", "gas_rigs", "misc_rigs", "total_rigs"],
            "total_consistency": "total_rigs must equal oil_rigs + gas_rigs + misc_rigs.",
            "minimum_history": "At least two weekly records required for WoW delta.",
            "weekly_frequency": "Recent date gaps should mostly be 5-10 days and latest gap must be 5-10 days.",
            "trigger_logic": "Each scarcity trigger must equal delta <= -10.",
            "freshness": f"Latest date must be within {max_stale_days} days.",
            "retention": f"Most recent {DEFAULT_LIMIT_WEEKS} weekly records by default.",
        },
        "errors": errors,
        "warnings": warnings,
    }


def main() -> int:
    require_dependencies()

    parser = argparse.ArgumentParser()
    parser.add_argument("--local-file", default="", help=f"Local Baker Hughes Excel file. Default searches for {DEFAULT_FILE_NAME}.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--max-stale-days", type=int, default=DEFAULT_MAX_STALE_DAYS)
    parser.add_argument("--limit-weeks", type=int, default=DEFAULT_LIMIT_WEEKS)
    parser.add_argument("--gatekeeper-id", default="LOCAL_PROTO")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    raw_dir = out_dir / "raw"
    combined_dir = out_dir / "combined"
    debug_dir = out_dir / "debug"
    ensure_dir(raw_dir)
    ensure_dir(combined_dir)
    ensure_dir(debug_dir)

    print("=" * 88)
    print("Baker Hughes Step 7 Rig Count Local Parser LIGHT v4")
    print("=" * 88)
    print(f"Local file arg:   {args.local_file if args.local_file else 'AUTO_SEARCH'}")
    print(f"Output directory: {out_dir}")
    print(f"Raw directory:    {raw_dir}")
    print(f"Limit weeks:      {args.limit_weeks}")
    print(f"Max stale days:   {args.max_stale_days}")
    print(f"Gatekeeper ID:    {args.gatekeeper_id}")
    print("API key required: NO")
    print("Web request made: NO")

    local_file = resolve_local_file(args.local_file)
    print(f"\n[LOCAL] Using Baker Hughes file:")
    print(f"        {local_file}")

    raw_copy = raw_dir / local_file.name
    if local_file.resolve() != raw_copy.resolve():
        shutil.copy2(local_file, raw_copy)

    save_json(raw_dir / "baker_hughes_raw_file_metadata.json", {
        "loaded_at": now_utc(),
        "mode": "LOCAL_FILE_ONLY",
        "local_file": str(local_file),
        "raw_copy": str(raw_copy),
        "bytes": local_file.stat().st_size,
    })

    print("\n[PARSE] Parsing NAM Weekly long table...")
    records_raw, parse_meta = parse_nam_weekly_long_table(local_file, debug_dir)
    save_json(raw_dir / "baker_hughes_parse_metadata.json", parse_meta)

    print(f"[PARSE] Source sheet: {parse_meta.get('source_sheet')}")
    print(f"[PARSE] Rows after U.S. Oil/Gas/Misc filter: {parse_meta.get('rows_after_us_oil_gas_misc_filter')}")
    print(f"[PARSE] Weekly records before limit: {len(records_raw)}")
    print(f"[PARSE] Date range before limit: {parse_meta.get('earliest_date')} to {parse_meta.get('latest_date')}")

    records_before_limit = len(records_raw)
    if args.limit_weeks is not None and args.limit_weeks > 0 and len(records_raw) > args.limit_weeks:
        records_raw = records_raw[-args.limit_weeks:]

    print(f"[PARSE] Records retained for normalized output: {len(records_raw)}")

    normalized = add_deltas_and_triggers(
        records_raw,
        raw_source_url=f"LOCAL_FILE:{local_file}",
        raw_file_name=local_file.name,
        gatekeeper_id=args.gatekeeper_id,
    )

    validation = validate_records(normalized, local_file, parse_meta, args.max_stale_days)
    validation["retention_policy"] = {
        "limit_weeks": args.limit_weeks,
        "records_before_limit": records_before_limit,
        "records_retained": len(normalized),
        "description": "Validation is performed on the retained most-recent weekly records."
    }

    combined_json = combined_dir / "baker_hughes_rig_count_normalized.json"
    combined_jsonl = combined_dir / "baker_hughes_rig_count_normalized.jsonl"
    combined_csv = combined_dir / "baker_hughes_rig_count_normalized.csv"
    latest_json = combined_dir / "baker_hughes_rig_count_latest.json"
    latest_csv = combined_dir / "baker_hughes_rig_count_latest.csv"
    validation_json = combined_dir / "baker_hughes_rig_count_validation_report.json"
    validation_txt = combined_dir / "baker_hughes_rig_count_validation_report.txt"

    latest = normalized[-1] if normalized else None

    save_json(combined_json, {
        "generated_at": now_utc(),
        "source": "BAKER_HUGHES_RIG_COUNT",
        "stage": "step7_rig_count_local_parse_normalize_trigger_light_v4",
        "local_file": str(local_file),
        "normalization_status": "COMPLETED",
        "parser": "NAM_WEEKLY_LONG_TABLE_V4",
        "retention_policy": validation["retention_policy"],
        "records": normalized,
        "latest": latest,
    })
    write_jsonl(combined_jsonl, normalized)
    write_csv(combined_csv, normalized)
    save_json(latest_json, {"generated_at": now_utc(), "latest": latest})
    write_csv(latest_csv, [latest] if latest else [])
    save_json(validation_json, validation)
    write_validation_txt(validation_txt, validation)

    print("\n" + "=" * 88)
    print("BAKER HUGHES STEP 7 v4 SUMMARY")
    print("=" * 88)

    if latest:
        print(f"Latest date:              {latest.get('timestamp')}")
        print(f"Oil rigs:                 {latest.get('oil_rigs')} | delta_1w={latest.get('oil_rigs_delta_1w')}")
        print(f"Gas rigs:                 {latest.get('gas_rigs')} | delta_1w={latest.get('gas_rigs_delta_1w')}")
        print(f"Misc rigs:                {latest.get('misc_rigs')} | delta_1w={latest.get('misc_rigs_delta_1w')}")
        print(f"Total rigs:               {latest.get('total_rigs')} | delta_1w={latest.get('total_rigs_delta_1w')}")
        print(f"Energy scarcity trigger:  {latest.get('energy_scarcity_trigger')}")
        print(f"Trigger reason:           {latest.get('trigger_reason')}")

    print(f"Records retained:         {len(normalized)} / {records_before_limit} parsed")
    print(f"Retention policy:         most recent {args.limit_weeks} weeks")
    print(f"\nValidation: {'PASS' if validation['all_required_ok'] else 'FAIL'}")
    if validation["errors"]:
        print("Errors:")
        for e in validation["errors"]:
            print(f"  - {e}")
    if validation["warnings"]:
        print("Warnings:")
        for w in validation["warnings"]:
            print(f"  - {w}")

    print("\nSaved outputs:")
    print(combined_json)
    print(combined_jsonl)
    print(combined_csv)
    print(latest_json)
    print(latest_csv)
    print(validation_json)
    print(validation_txt)
    print("\nRaw/debug files:")
    print(raw_dir)
    print(debug_dir)

    if validation["all_required_ok"]:
        print("\n[DONE] Baker Hughes Step 7 local rig count layer completed and validated.")
        return 0

    print("\n[DONE WITH FAILURES] Review validation report and debug files.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
