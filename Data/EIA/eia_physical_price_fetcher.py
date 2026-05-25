#!/usr/bin/env python3
"""
EIA Physical Energy Price Fetcher for Metrixx BASIS SENTIMENT Service

Purpose
-------
Fetch the EIA physical energy price series required by the project document:
WTI, Brent, Henry Hub, RBOB, Heating Oil, and Jet Fuel.

This script ONLY does the Fetch + Parse + Validation layer.
It saves data in a shape that is ready for the next Normalize step.

Usage
-----
1) Install dependency:
   pip install requests

2) Set API key:
   Windows CMD:
      set EIA_API_KEY=your_key_here
   PowerShell:
      $env:EIA_API_KEY="your_key_here"
   Mac/Linux:
      export EIA_API_KEY="your_key_here"

3) Run:
   python eia_physical_price_fetcher.py --length 30

Optional:
   python eia_physical_price_fetcher.py --api-key YOUR_KEY --length 60 --out-dir eia_output

Outputs
-------
- raw_eia_<series_key>.json
- eia_fetch_ready_for_normalize.json
- eia_fetch_validation_report.json

The file eia_fetch_ready_for_normalize.json is the direct input for the next Normalize step.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ImportError as exc:  # pragma: no cover
    print("Missing dependency: requests. Install with: pip install requests", file=sys.stderr)
    raise exc


# -----------------------------------------------------------------------------
# 1. Series configuration
# -----------------------------------------------------------------------------
# Notes:
# - The project document asks for WTI, Brent, Henry Hub, RBOB, Heating Oil,
#   and Jet Fuel physical price data.
# - EIA v2 supports the legacy /seriesid/{series_id} route.
# - The internal fields below are intentionally close to the document's JSON
#   envelope, but this fetcher stops before full normalization.

EIA_SERIES: Dict[str, Dict[str, Any]] = {
    "WTI_CRUDE": {
        "series_id": "PET.RWTC.D",
        "source_id": "EIA_WTI_DAILY",
        "instrument": "WTI_CRUDE",
        "delivery_point": "CUSHING_OK",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_BBL",
        "label": "Cushing, OK WTI Spot Price FOB",
        "raw_source_url": "https://www.eia.gov/dnav/pet/hist/RWTCD.htm",
        "expected_min": 0.01,
        "expected_max": 300.0,
        "required": True,
    },
    "BRENT_CRUDE": {
        "series_id": "PET.RBRTE.D",
        "source_id": "EIA_BRENT_DAILY",
        "instrument": "BRENT_CRUDE",
        "delivery_point": "EUROPE",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_BBL",
        "label": "Europe Brent Spot Price FOB",
        "raw_source_url": "https://www.eia.gov/dnav/pet/hist/RBRTED.htm",
        "expected_min": 0.01,
        "expected_max": 300.0,
        "required": True,
    },
    "HENRY_HUB_NG": {
        "series_id": "NG.RNGWHHD.D",
        "source_id": "EIA_HENRY_HUB_DAILY",
        "instrument": "HENRY_HUB_NG",
        "delivery_point": "HENRY_HUB_LA",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_MMBTU",
        "label": "Henry Hub Natural Gas Spot Price",
        "raw_source_url": "https://www.eia.gov/dnav/ng/hist/RNGWHHDD.htm",
        "expected_min": 0.01,
        "expected_max": 100.0,
        "required": True,
    },
    "RBOB_GASOLINE_LA": {
        "series_id": "PET.EER_EPMRR_PF4_Y05LA_DPG.D",
        "source_id": "EIA_RBOB_GASOLINE_LA_DAILY",
        "instrument": "RBOB_GASOLINE",
        "delivery_point": "LOS_ANGELES",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_GAL",
        "label": "Los Angeles Reformulated RBOB Regular Gasoline Spot Price",
        "raw_source_url": "https://www.eia.gov/dnav/pet/hist/EER_EPMRR_PF4_Y05LA_DPGD.htm",
        "expected_min": 0.01,
        "expected_max": 20.0,
        "required": True,
    },
    "HEATING_OIL_NYH": {
        "series_id": "PET.EER_EPD2F_PF4_Y35NY_DPG.D",
        "source_id": "EIA_HEATING_OIL_NYH_DAILY",
        "instrument": "HEATING_OIL",
        "delivery_point": "NEW_YORK_HARBOR",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_GAL",
        "label": "New York Harbor No. 2 Heating Oil Spot Price FOB",
        "raw_source_url": "https://www.eia.gov/dnav/pet/hist/EER_EPD2F_PF4_Y35NY_DPGD.htm",
        "expected_min": 0.01,
        "expected_max": 20.0,
        "required": True,
    },
    "JET_FUEL_USGC": {
        "series_id": "PET.EER_EPJK_PF4_RGC_DPG.D",
        "source_id": "EIA_JET_FUEL_USGC_DAILY",
        "instrument": "JET_FUEL",
        "delivery_point": "US_GULF_COAST",
        "data_type": "SPOT_PRICE",
        "unit": "USD_PER_GAL",
        "label": "U.S. Gulf Coast Kerosene-Type Jet Fuel Spot Price FOB",
        "raw_source_url": "https://www.eia.gov/dnav/pet/hist/EER_EPJK_PF4_RGC_DPGD.htm",
        "expected_min": 0.01,
        "expected_max": 20.0,
        "required": True,
    },
}


# -----------------------------------------------------------------------------
# 2. Data structures
# -----------------------------------------------------------------------------

@dataclass
class ValidationResult:
    series_key: str
    source_id: str
    series_id: str
    instrument: str
    delivery_point: str
    unit: str
    ok: bool
    observation_count: int
    latest_timestamp: Optional[str]
    latest_value: Optional[float]
    errors: List[str]
    warnings: List[str]


# -----------------------------------------------------------------------------
# 3. EIA API fetch
# -----------------------------------------------------------------------------

def fetch_eia_series(
    api_key: str,
    series_id: str,
    length: int,
    max_retries: int = 3,
    backoff_seconds: float = 1.5,
) -> Dict[str, Any]:
    """Fetch one EIA legacy series through EIA API v2 /seriesid route."""
    url = f"https://api.eia.gov/v2/seriesid/{series_id}"
    params = {"api_key": api_key, "length": int(length)}

    last_error: Optional[BaseException] = None

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)

            # Retry common temporary/rate-limit failures.
            if response.status_code in {429, 500, 502, 503, 504}:
                wait = backoff_seconds * (2 ** attempt)
                print(
                    f"[WARN] HTTP {response.status_code} for {series_id}; "
                    f"retrying in {wait:.1f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        except Exception as exc:  # noqa: BLE001 - CLI script should report all failures
            last_error = exc
            wait = backoff_seconds * (2 ** attempt)
            print(
                f"[WARN] Fetch attempt {attempt + 1}/{max_retries} failed for {series_id}: {exc}; "
                f"retrying in {wait:.1f}s...",
                file=sys.stderr,
            )
            time.sleep(wait)

    raise RuntimeError(f"EIA fetch failed for {series_id}: {last_error}")


# -----------------------------------------------------------------------------
# 4. Parse raw EIA payload into normalize-ready observations
# -----------------------------------------------------------------------------

def _coerce_float(value: Any) -> Optional[float]:
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


def parse_eia_seriesid_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert EIA v2 /seriesid payload into a simple list:
    [{"timestamp": "YYYY-MM-DD", "value": float}, ...]
    """
    rows = payload.get("response", {}).get("data", [])
    observations: List[Dict[str, Any]] = []

    for row in rows:
        timestamp = row.get("period") or row.get("date")
        value = _coerce_float(row.get("value"))

        if timestamp is None or value is None:
            continue

        observations.append({"timestamp": str(timestamp), "value": value})

    # Sort ascending for downstream normalization/calculation.
    observations.sort(key=lambda x: x["timestamp"])
    return observations


# -----------------------------------------------------------------------------
# 5. Validation: every required data group is checked before Normalize step
# -----------------------------------------------------------------------------

def validate_observations(
    series_key: str,
    meta: Dict[str, Any],
    observations: List[Dict[str, Any]],
) -> ValidationResult:
    errors: List[str] = []
    warnings: List[str] = []

    required_fields = [
        "series_id",
        "source_id",
        "instrument",
        "delivery_point",
        "data_type",
        "unit",
        "raw_source_url",
    ]

    for field in required_fields:
        if not meta.get(field):
            errors.append(f"Missing config field: {field}")

    if not observations:
        errors.append("No valid observations parsed from EIA response.")

    seen_timestamps = set()
    values: List[float] = []

    for idx, obs in enumerate(observations):
        timestamp = obs.get("timestamp")
        value = obs.get("value")

        if not timestamp:
            errors.append(f"Observation {idx} missing timestamp.")
            continue

        if timestamp in seen_timestamps:
            warnings.append(f"Duplicate timestamp found: {timestamp}")
        seen_timestamps.add(timestamp)

        if not isinstance(value, (float, int)):
            errors.append(f"Observation {idx} has non-numeric value: {value}")
            continue

        value_f = float(value)
        values.append(value_f)

        expected_min = float(meta["expected_min"])
        expected_max = float(meta["expected_max"])
        if value_f < expected_min or value_f > expected_max:
            warnings.append(
                f"Value outside sanity range for {series_key}: {timestamp}={value_f}; "
                f"expected [{expected_min}, {expected_max}]"
            )

    latest_timestamp = observations[-1]["timestamp"] if observations else None
    latest_value = float(observations[-1]["value"]) if observations else None

    ok = len(errors) == 0

    return ValidationResult(
        series_key=series_key,
        source_id=meta.get("source_id", ""),
        series_id=meta.get("series_id", ""),
        instrument=meta.get("instrument", ""),
        delivery_point=meta.get("delivery_point", ""),
        unit=meta.get("unit", ""),
        ok=ok,
        observation_count=len(observations),
        latest_timestamp=latest_timestamp,
        latest_value=latest_value,
        errors=errors,
        warnings=warnings,
    )


def validate_all_required_series(results: List[ValidationResult]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    result_by_key = {r.series_key: r for r in results}

    for series_key, meta in EIA_SERIES.items():
        if not meta.get("required", False):
            continue
        if series_key not in result_by_key:
            errors.append(f"Required series was not attempted: {series_key}")
            continue
        if not result_by_key[series_key].ok:
            errors.append(f"Required series failed validation: {series_key}")

    return len(errors) == 0, errors


# -----------------------------------------------------------------------------
# 6. Save normalize-ready payload
# -----------------------------------------------------------------------------

def make_normalize_ready_group(
    series_key: str,
    meta: Dict[str, Any],
    observations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Shape returned here is intentionally simple for the next normalize step.
    The next file can iterate each group and emit the document-required JSON envelope.
    """
    return {
        "series_key": series_key,
        "series_id": meta["series_id"],
        "source_id": meta["source_id"],
        "instrument": meta["instrument"],
        "delivery_point": meta["delivery_point"],
        "data_type": meta["data_type"],
        "unit": meta["unit"],
        "label": meta["label"],
        "tos_status": "GO",
        "gatekeeper_cleared": True,
        "raw_source_url": meta["raw_source_url"],
        "observations": observations,
    }


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# -----------------------------------------------------------------------------
# 7. CLI entrypoint
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch EIA physical energy prices required for BASIS SENTIMENT service."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("EIA_API_KEY", ""),
        help="EIA API key. Defaults to EIA_API_KEY environment variable.",
    )
    parser.add_argument(
        "--length",
        type=int,
        default=30,
        help="Number of latest observations to fetch per series. Default: 30.",
    )
    parser.add_argument(
        "--out-dir",
        default="eia_output",
        help="Output directory. Default: eia_output.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Max retries per series. Default: 3.",
    )
    parser.add_argument(
        "--no-strict",
        action="store_true",
        help="Do not exit with non-zero status if validation fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.api_key:
        print(
            "[ERROR] Missing EIA API key. Set EIA_API_KEY or pass --api-key.",
            file=sys.stderr,
        )
        return 2

    out_dir = Path(args.out_dir).resolve()
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("EIA Physical Energy Price Fetcher")
    print("=" * 80)
    print(f"Output directory: {out_dir}")
    print(f"Observations per series: {args.length}")
    print(f"Series count: {len(EIA_SERIES)}")
    print()

    normalize_ready_groups: List[Dict[str, Any]] = []
    validation_results: List[ValidationResult] = []

    for series_key, meta in EIA_SERIES.items():
        series_id = meta["series_id"]
        print(f"[FETCH] {series_key} | {series_id} | {meta['label']}")

        try:
            raw_payload = fetch_eia_series(
                api_key=args.api_key,
                series_id=series_id,
                length=args.length,
                max_retries=args.max_retries,
            )
            save_json(raw_dir / f"raw_eia_{series_key.lower()}.json", raw_payload)

            observations = parse_eia_seriesid_payload(raw_payload)
            validation = validate_observations(series_key, meta, observations)
            validation_results.append(validation)

            if validation.ok:
                normalize_ready_groups.append(
                    make_normalize_ready_group(series_key, meta, observations)
                )
                print(
                    f"  OK | count={validation.observation_count} | "
                    f"latest={validation.latest_timestamp} value={validation.latest_value} {validation.unit}"
                )
                if validation.warnings:
                    for warning in validation.warnings:
                        print(f"  WARN | {warning}")
            else:
                print(f"  FAIL | {series_key}")
                for error in validation.errors:
                    print(f"    ERROR | {error}")
                for warning in validation.warnings:
                    print(f"    WARN  | {warning}")

        except Exception as exc:  # noqa: BLE001 - command-line fetcher should continue
            validation = ValidationResult(
                series_key=series_key,
                source_id=meta.get("source_id", ""),
                series_id=series_id,
                instrument=meta.get("instrument", ""),
                delivery_point=meta.get("delivery_point", ""),
                unit=meta.get("unit", ""),
                ok=False,
                observation_count=0,
                latest_timestamp=None,
                latest_value=None,
                errors=[f"Fetch or parse exception: {exc}"],
                warnings=[],
            )
            validation_results.append(validation)
            print(f"  FAIL | {series_key}: {exc}")

    all_required_ok, required_errors = validate_all_required_series(validation_results)

    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    normalize_ready_payload = {
        "fetched_at": fetched_at,
        "source": "EIA_API_V2_SERIESID",
        "purpose": "Input for next Normalize step in BASIS SENTIMENT EIA service.",
        "groups": normalize_ready_groups,
    }

    validation_report = {
        "fetched_at": fetched_at,
        "all_required_ok": all_required_ok,
        "required_errors": required_errors,
        "series_results": [asdict(r) for r in validation_results],
    }

    normalize_ready_path = out_dir / "eia_fetch_ready_for_normalize.json"
    validation_report_path = out_dir / "eia_fetch_validation_report.json"

    save_json(normalize_ready_path, normalize_ready_payload)
    save_json(validation_report_path, validation_report)

    print()
    print("=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    for result in validation_results:
        status = "PASS" if result.ok else "FAIL"
        print(
            f"{status:4s} | {result.series_key:18s} | "
            f"count={result.observation_count:4d} | "
            f"latest={result.latest_timestamp} | value={result.latest_value} | unit={result.unit}"
        )

    if required_errors:
        print("\nRequired-series errors:")
        for error in required_errors:
            print(f"- {error}")

    print("\nSaved normalize-ready file:")
    print(normalize_ready_path)
    print("\nSaved validation report:")
    print(validation_report_path)
    print("\nRaw API responses saved under:")
    print(raw_dir)

    if not all_required_ok and not args.no_strict:
        print("\n[ERROR] One or more required EIA data groups failed validation.", file=sys.stderr)
        return 1

    print("\n[DONE] EIA fetch layer completed. Use eia_fetch_ready_for_normalize.json next.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
