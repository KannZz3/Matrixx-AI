#!/usr/bin/env python3
"""
Normalize EIA Physical Energy Price Fetch Output for Metrixx BASIS SENTIMENT Service

Purpose
-------
Read the fetch-layer output file:
    eia_output/eia_fetch_ready_for_normalize.json

Then convert each EIA date-value observation into the document-required normalized
JSON envelope for the BASIS SENTIMENT service.

This script performs ONLY the Normalize + Validation layer. It does not calculate
basis yet because futures settlement data is not available in Step 4.

Usage
-----
From the same terminal/location where you ran eia_physical_price_fetcher.py:

    python normalize_eia.py

Optional:

    python normalize_eia.py --input eia_output/eia_fetch_ready_for_normalize.json --out-dir eia_output
    python normalize_eia.py --gatekeeper-id LOCAL_PROTO
    python normalize_eia.py --no-strict

Outputs
-------
- eia_output/eia_energy_physical_prices_normalized.jsonl
- eia_output/eia_energy_physical_prices_normalized.json
- eia_output/eia_normalization_validation_report.json

Normalized JSON envelope example
--------------------------------
{
  "source_id": "EIA_WTI_DAILY",
  "instrument": "WTI_CRUDE",
  "delivery_point": "CUSHING_OK",
  "timestamp": "2026-05-18",
  "data_type": "SPOT_PRICE",
  "value": 112.25,
  "unit": "USD_PER_BBL",
  "basis_vs_futures": null,
  "futures_contract": null,
  "tos_status": "GO",
  "gatekeeper_cleared": true,
  "gatekeeper_id": "LOCAL_PROTO",
  "raw_source_url": "https://www.eia.gov/dnav/pet/hist/RWTCD.htm",
  "series_key": "WTI_CRUDE",
  "series_id": "PET.RWTC.D",
  "normalized_at": "2026-05-25T00:00:00+00:00"
}
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# -----------------------------------------------------------------------------
# 1. Expected config for final validation
# -----------------------------------------------------------------------------
# These values mirror the fetch script and are used to ensure the normalize-ready
# file still carries the correct semantic mapping before output is written.

EXPECTED_SERIES: Dict[str, Dict[str, Any]] = {
    "WTI_CRUDE": {
        "source_id": "EIA_WTI_DAILY",
        "instrument": "WTI_CRUDE",
        "delivery_point": "CUSHING_OK",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_BBL",
        "expected_min": 0.01,
        "expected_max": 300.0,
        "required": True,
    },
    "BRENT_CRUDE": {
        "source_id": "EIA_BRENT_DAILY",
        "instrument": "BRENT_CRUDE",
        "delivery_point": "EUROPE",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_BBL",
        "expected_min": 0.01,
        "expected_max": 300.0,
        "required": True,
    },
    "HENRY_HUB_NG": {
        "source_id": "EIA_HENRY_HUB_DAILY",
        "instrument": "HENRY_HUB_NG",
        "delivery_point": "HENRY_HUB_LA",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_MMBTU",
        "expected_min": 0.01,
        "expected_max": 100.0,
        "required": True,
    },
    "RBOB_GASOLINE_LA": {
        "source_id": "EIA_RBOB_GASOLINE_LA_DAILY",
        "instrument": "RBOB_GASOLINE",
        "delivery_point": "LOS_ANGELES",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_GAL",
        "expected_min": 0.01,
        "expected_max": 20.0,
        "required": True,
    },
    "HEATING_OIL_NYH": {
        "source_id": "EIA_HEATING_OIL_NYH_DAILY",
        "instrument": "HEATING_OIL",
        "delivery_point": "NEW_YORK_HARBOR",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_GAL",
        "expected_min": 0.01,
        "expected_max": 20.0,
        "required": True,
    },
    "JET_FUEL_USGC": {
        "source_id": "EIA_JET_FUEL_USGC_DAILY",
        "instrument": "JET_FUEL",
        "delivery_point": "US_GULF_COAST",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_GAL",
        "expected_min": 0.01,
        "expected_max": 20.0,
        "required": True,
    },
}

REQUIRED_NORMALIZED_FIELDS = [
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
]


# -----------------------------------------------------------------------------
# 2. Data structures
# -----------------------------------------------------------------------------

@dataclass
class GroupValidationResult:
    series_key: str
    source_id: str
    instrument: str
    delivery_point: str
    unit: str
    ok: bool
    input_observation_count: int
    output_record_count: int
    latest_timestamp: Optional[str]
    latest_value: Optional[float]
    errors: List[str]
    warnings: List[str]


# -----------------------------------------------------------------------------
# 3. Basic helpers
# -----------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value in {"", "--", "-", "NA", "N/A", "W"}:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_groups(payload: Any) -> List[Dict[str, Any]]:
    """
    Expected fetch-ready structure:
        {"groups": [ ... ]}

    Also supports a raw list of groups for robustness.
    """
    if isinstance(payload, dict) and isinstance(payload.get("groups"), list):
        return payload["groups"]
    if isinstance(payload, list):
        return payload
    raise ValueError(
        "Input file must contain a top-level 'groups' list or be a list of groups. "
        "Make sure you are using eia_fetch_ready_for_normalize.json."
    )


# -----------------------------------------------------------------------------
# 4. Core normalization logic
# -----------------------------------------------------------------------------

def normalize_one_observation(
    group: Dict[str, Any],
    obs: Dict[str, Any],
    gatekeeper_id: str,
    normalized_at: str,
) -> Optional[Dict[str, Any]]:
    """
    Convert one fetch-ready observation into the BASIS normalized JSON envelope.

    Mathematical/data transformation logic:
    - timestamp_out = timestamp_in
    - value_out = float(value_in)
    - No price scaling is applied.
    - No basis is calculated in Step 4, so basis_vs_futures = None.
    - No futures contract is available in Step 4, so futures_contract = None.
    """
    timestamp = obs.get("timestamp") or obs.get("date") or obs.get("period")
    value = coerce_float(obs.get("value"))

    if timestamp is None or value is None:
        return None

    if value <= 0:
        return None

    return {
        "source_id": group.get("source_id"),
        "instrument": group.get("instrument"),
        "delivery_point": group.get("delivery_point"),
        "timestamp": str(timestamp),
        "data_type": group.get("data_type", "SPOT_PRICE"),
        "value": value,
        "unit": group.get("unit"),
        "basis_vs_futures": None,
        "futures_contract": None,
        "tos_status": group.get("tos_status", "GO"),
        "gatekeeper_cleared": bool(group.get("gatekeeper_cleared", True)),
        "gatekeeper_id": gatekeeper_id,
        "raw_source_url": group.get("raw_source_url"),
        # Extra useful provenance fields. These do not conflict with the envelope.
        "series_key": group.get("series_key"),
        "series_id": group.get("series_id"),
        "label": group.get("label"),
        "normalized_at": normalized_at,
    }


def validate_group_before_normalize(group: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    series_key = group.get("series_key")
    if not series_key:
        errors.append("Group missing series_key.")
        return errors, warnings

    expected = EXPECTED_SERIES.get(series_key)
    if expected is None:
        warnings.append(f"Unexpected series_key not listed in EXPECTED_SERIES: {series_key}")
    else:
        for field in ["source_id", "instrument", "delivery_point", "data_type", "unit"]:
            if group.get(field) != expected[field]:
                errors.append(
                    f"Mapping mismatch for {field}: got {group.get(field)!r}, "
                    f"expected {expected[field]!r}"
                )

    for field in [
        "source_id",
        "instrument",
        "delivery_point",
        "data_type",
        "unit",
        "tos_status",
        "gatekeeper_cleared",
        "raw_source_url",
        "observations",
    ]:
        if field not in group:
            errors.append(f"Group missing required field: {field}")

    if "observations" in group and not isinstance(group["observations"], list):
        errors.append("Group field 'observations' must be a list.")

    return errors, warnings


def validate_normalized_records_for_group(
    group: Dict[str, Any],
    records: List[Dict[str, Any]],
    pre_errors: List[str],
    pre_warnings: List[str],
) -> GroupValidationResult:
    series_key = str(group.get("series_key", "UNKNOWN"))
    expected = EXPECTED_SERIES.get(series_key, {})

    errors = list(pre_errors)
    warnings = list(pre_warnings)

    input_observations = group.get("observations", [])
    input_count = len(input_observations) if isinstance(input_observations, list) else 0
    output_count = len(records)

    if input_count == 0:
        errors.append("No input observations available for this group.")
    if output_count == 0:
        errors.append("No normalized records produced for this group.")
    if output_count != input_count:
        warnings.append(
            f"Output count differs from input count: input={input_count}, output={output_count}. "
            "This can happen if missing/non-positive values were dropped."
        )

    timestamps = set()
    latest_timestamp: Optional[str] = None
    latest_value: Optional[float] = None

    for idx, record in enumerate(records):
        for field in REQUIRED_NORMALIZED_FIELDS:
            if field not in record:
                errors.append(f"Record {idx} missing normalized field: {field}")

        # Required semantic values.
        if expected:
            for field in ["source_id", "instrument", "delivery_point", "data_type", "unit"]:
                if record.get(field) != expected[field]:
                    errors.append(
                        f"Record {idx} {field} mismatch: got {record.get(field)!r}, "
                        f"expected {expected[field]!r}"
                    )

        if record.get("tos_status") != "GO":
            errors.append(f"Record {idx} tos_status should be GO, got {record.get('tos_status')!r}")

        if record.get("gatekeeper_cleared") is not True:
            errors.append(f"Record {idx} gatekeeper_cleared should be true.")

        if record.get("basis_vs_futures") is not None:
            errors.append("basis_vs_futures must be null in Step 4 before CME futures leg is added.")

        if record.get("futures_contract") is not None:
            errors.append("futures_contract must be null in Step 4 before CME futures leg is added.")

        timestamp = record.get("timestamp")
        if not timestamp:
            errors.append(f"Record {idx} missing timestamp value.")
        elif timestamp in timestamps:
            warnings.append(f"Duplicate timestamp in normalized output: {timestamp}")
        else:
            timestamps.add(timestamp)

        value = coerce_float(record.get("value"))
        if value is None:
            errors.append(f"Record {idx} has non-numeric value: {record.get('value')!r}")
        elif value <= 0:
            errors.append(f"Record {idx} has non-positive value: {value}")
        else:
            if expected:
                min_v = float(expected["expected_min"])
                max_v = float(expected["expected_max"])
                if value < min_v or value > max_v:
                    warnings.append(
                        f"Record {idx} value outside sanity range: {timestamp}={value}; "
                        f"expected [{min_v}, {max_v}]"
                    )
            latest_timestamp = str(timestamp)
            latest_value = float(value)

    return GroupValidationResult(
        series_key=series_key,
        source_id=str(group.get("source_id", "")),
        instrument=str(group.get("instrument", "")),
        delivery_point=str(group.get("delivery_point", "")),
        unit=str(group.get("unit", "")),
        ok=len(errors) == 0,
        input_observation_count=input_count,
        output_record_count=output_count,
        latest_timestamp=latest_timestamp,
        latest_value=latest_value,
        errors=errors,
        warnings=warnings,
    )


def normalize_groups(
    groups: List[Dict[str, Any]],
    gatekeeper_id: str,
) -> Tuple[List[Dict[str, Any]], List[GroupValidationResult]]:
    normalized_at = utc_now_iso()
    all_records: List[Dict[str, Any]] = []
    validation_results: List[GroupValidationResult] = []

    for group in groups:
        pre_errors, pre_warnings = validate_group_before_normalize(group)
        observations = group.get("observations", []) if isinstance(group.get("observations", []), list) else []

        records: List[Dict[str, Any]] = []
        if not pre_errors:
            for obs in observations:
                record = normalize_one_observation(
                    group=group,
                    obs=obs,
                    gatekeeper_id=gatekeeper_id,
                    normalized_at=normalized_at,
                )
                if record is not None:
                    records.append(record)

            # Sort records by instrument/delivery/timestamp for stable output.
            records.sort(key=lambda r: (r.get("instrument", ""), r.get("delivery_point", ""), r.get("timestamp", "")))

        result = validate_normalized_records_for_group(group, records, pre_errors, pre_warnings)
        validation_results.append(result)
        all_records.extend(records)

    all_records.sort(key=lambda r: (r.get("instrument", ""), r.get("delivery_point", ""), r.get("timestamp", "")))
    return all_records, validation_results


def validate_required_groups(results: List[GroupValidationResult]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    result_by_key = {r.series_key: r for r in results}

    for series_key, expected in EXPECTED_SERIES.items():
        if not expected.get("required", False):
            continue
        if series_key not in result_by_key:
            errors.append(f"Required series missing from normalize input/output: {series_key}")
        elif not result_by_key[series_key].ok:
            errors.append(f"Required series failed normalization validation: {series_key}")

    return len(errors) == 0, errors


# -----------------------------------------------------------------------------
# 5. CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize EIA fetch-ready data into BASIS SENTIMENT JSON envelope."
    )
    parser.add_argument(
        "--input",
        default=str(Path("eia_output") / "eia_fetch_ready_for_normalize.json"),
        help="Input fetch-ready JSON. Default: eia_output/eia_fetch_ready_for_normalize.json",
    )
    parser.add_argument(
        "--out-dir",
        default="eia_output",
        help="Output directory. Default: eia_output",
    )
    parser.add_argument(
        "--gatekeeper-id",
        default="LOCAL_PROTO",
        help="Gatekeeper identifier written into normalized records. Default: LOCAL_PROTO",
    )
    parser.add_argument(
        "--no-strict",
        action="store_true",
        help="Do not exit with non-zero status if validation fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    input_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()

    print("=" * 80)
    print("EIA Physical Energy Price Normalizer")
    print("=" * 80)
    print(f"Input file: {input_path}")
    print(f"Output directory: {out_dir}")
    print(f"Gatekeeper ID: {args.gatekeeper_id}")
    print()

    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path}", file=sys.stderr)
        print(
            "Make sure you already ran eia_physical_price_fetcher.py and generated "
            "eia_output/eia_fetch_ready_for_normalize.json.",
            file=sys.stderr,
        )
        return 2

    try:
        payload = load_json(input_path)
        groups = extract_groups(payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Could not load/parse input file: {exc}", file=sys.stderr)
        return 2

    print(f"Groups found: {len(groups)}")

    normalized_records, validation_results = normalize_groups(
        groups=groups,
        gatekeeper_id=args.gatekeeper_id,
    )

    all_required_ok, required_errors = validate_required_groups(validation_results)
    normalized_at = utc_now_iso()

    normalized_jsonl_path = out_dir / "eia_energy_physical_prices_normalized.jsonl"
    normalized_json_path = out_dir / "eia_energy_physical_prices_normalized.json"
    validation_report_path = out_dir / "eia_normalization_validation_report.json"

    normalized_payload = {
        "normalized_at": normalized_at,
        "source_input": str(input_path),
        "record_count": len(normalized_records),
        "purpose": "Normalized EIA physical price records for BASIS SENTIMENT Step 4 physical leg.",
        "records": normalized_records,
    }

    validation_report = {
        "normalized_at": normalized_at,
        "source_input": str(input_path),
        "all_required_ok": all_required_ok,
        "required_errors": required_errors,
        "total_normalized_records": len(normalized_records),
        "group_results": [asdict(r) for r in validation_results],
        "notes": [
            "basis_vs_futures is intentionally null in Step 4 because CME futures settlements have not been joined yet.",
            "futures_contract is intentionally null in Step 4 because futures leg is not available yet.",
            "No price scaling is applied during normalization; values are only coerced to float and assigned to the standard JSON envelope.",
        ],
    }

    save_jsonl(normalized_jsonl_path, normalized_records)
    save_json(normalized_json_path, normalized_payload)
    save_json(validation_report_path, validation_report)

    print()
    print("=" * 80)
    print("NORMALIZATION SUMMARY")
    print("=" * 80)
    for result in validation_results:
        status = "PASS" if result.ok else "FAIL"
        print(
            f"{status:4s} | {result.series_key:18s} | "
            f"input={result.input_observation_count:4d} | "
            f"output={result.output_record_count:4d} | "
            f"latest={result.latest_timestamp} | value={result.latest_value} | unit={result.unit}"
        )
        for warning in result.warnings[:5]:
            print(f"      WARN | {warning}")
        if len(result.warnings) > 5:
            print(f"      WARN | ... {len(result.warnings) - 5} more warnings")
        for error in result.errors:
            print(f"      ERROR | {error}")

    if required_errors:
        print("\nRequired-series errors:")
        for error in required_errors:
            print(f"- {error}")

    print("\nSaved normalized JSONL:")
    print(normalized_jsonl_path)
    print("\nSaved normalized JSON:")
    print(normalized_json_path)
    print("\nSaved normalization validation report:")
    print(validation_report_path)

    if normalized_records:
        print("\nSample normalized record:")
        print(json.dumps(normalized_records[-1], ensure_ascii=False, indent=2))

    if not all_required_ok and not args.no_strict:
        print("\n[ERROR] One or more required EIA groups failed normalization validation.", file=sys.stderr)
        return 1

    print("\n[DONE] EIA normalization layer completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
