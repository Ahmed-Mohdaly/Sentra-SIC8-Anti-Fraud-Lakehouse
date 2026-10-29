#!/usr/bin/env python3
"""Safely enrich historical transaction CSVs with merchant coordinates and payment classifications.

Preserves original data intact without overwriting source files.
Applies 'unknown' to historical transaction_entry_method per project data contract.
Resolves merchant city/state against city_reference.csv with comprehensive data-quality metrics.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from synthetic_streaming.classification_and_geo import (
    CityResolver,
    normalize_city_state,
    sample_entry_method,
    validate_coordinates,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("enrich_historical")


def enrich_historical_dataset(
    input_file: Path,
    output_file: Path,
    city_ref_file: Path,
    limit: Optional[int] = None,
    randomize_entry_method: bool = True,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """Read historical transaction dataset, resolve city coordinates, and write enriched dataset safely."""
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if input_file.resolve() == output_file.resolve():
        raise ValueError("Output file cannot overwrite the original input file. Choose a different output destination.")

    import random
    rng = random.Random(seed) if seed is not None else random.Random()

    # Initialize city resolver
    resolver = CityResolver()
    loaded_cities = resolver.load_from_csv(city_ref_file)
    logger.info(f"Loaded {loaded_cities} reference cities from {city_ref_file}")

    total_rows = 0
    matched_count = 0
    unmatched_count = 0
    online_count = 0
    missing_geo_count = 0
    invalid_coord_count = 0
    unmatched_cities_counter = Counter()
    entry_method_counter = Counter()

    with open(input_file, "r", encoding="utf-8-sig") as f_in, open(output_file, "w", encoding="utf-8", newline="") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        header = next(reader, None)
        if not header:
            raise ValueError(f"Input file {input_file} is empty.")

        # Determine column indices from header
        header_map = {col.strip(): idx for idx, col in enumerate(header)}
        city_idx = header_map.get("Merchant City")
        state_idx = header_map.get("Merchant State")
        use_chip_idx = header_map.get("Use Chip")
        mcc_idx = header_map.get("MCC")

        if city_idx is None:
            raise ValueError(f"Missing 'Merchant City' column in {input_file}")

        # Enriched output header
        out_header = list(header) + [
            "Transaction Entry Method",
            "Merchant Latitude",
            "Merchant Longitude",
            "Coordinate Source",
            "Coordinate Precision",
        ]
        writer.writerow(out_header)

        for row in reader:
            if not row or len(row) < len(header):
                continue

            total_rows += 1
            city = row[city_idx].strip() if city_idx < len(row) else ""
            state = row[state_idx].strip() if state_idx is not None and state_idx < len(row) else ""
            use_chip = row[use_chip_idx].strip() if use_chip_idx is not None and use_chip_idx < len(row) else "Chip Transaction"

            mcc_val = 5411
            if mcc_idx is not None and mcc_idx < len(row):
                try:
                    mcc_val = int(float(row[mcc_idx].strip()))
                except (ValueError, TypeError):
                    mcc_val = 5411

            # Assign entry method: either realistically sampled or preserved as unknown
            if randomize_entry_method:
                entry_method = sample_entry_method(
                    use_chip=use_chip,
                    source_system="HISTORICAL",
                    mcc=mcc_val,
                    rng=rng,
                )
            else:
                entry_method = "unknown"

            entry_method_counter[entry_method] += 1

            m_lat: Optional[float] = None
            m_lon: Optional[float] = None
            coord_source = ""
            coord_precision = ""

            if not city:
                missing_geo_count += 1
                coord_source = "missing_city"
                coord_precision = "none"
            elif city.upper() == "ONLINE":
                online_count += 1
                coord_source = "online_transaction"
                coord_precision = "none"
            else:
                m_lat, m_lon = resolver.resolve(city, state)
                if m_lat is not None and m_lon is not None:
                    # Validate coordinate bounds and co-occurrence
                    if validate_coordinates(m_lat, m_lon):
                        matched_count += 1
                        coord_source = "US Census Gazetteer & GeoNames"
                        coord_precision = "city_center"
                    else:
                        invalid_coord_count += 1
                        m_lat, m_lon = None, None
                        coord_source = "invalid_coordinates"
                        coord_precision = "none"
                else:
                    unmatched_count += 1
                    unmatched_cities_counter[f"{city}, {state}"] += 1
                    coord_source = "unmatched_city"
                    coord_precision = "none"

            lat_str = f"{m_lat:.6f}" if m_lat is not None else ""
            lon_str = f"{m_lon:.6f}" if m_lon is not None else ""

            enriched_row = list(row) + [
                entry_method,
                lat_str,
                lon_str,
                coord_source,
                coord_precision,
            ]
            writer.writerow(enriched_row)

            if limit and total_rows >= limit:
                break

    match_rate = (matched_count / total_rows * 100.0) if total_rows > 0 else 0.0
    unmatched_rate = (unmatched_count / total_rows * 100.0) if total_rows > 0 else 0.0
    online_rate = (online_count / total_rows * 100.0) if total_rows > 0 else 0.0
    unknown_rate = (entry_method_counter.get("unknown", 0) / total_rows * 100.0) if total_rows > 0 else 0.0

    report = {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "total_rows_processed": total_rows,
        "matched_with_coordinates": matched_count,
        "match_rate_percentage": round(match_rate, 2),
        "online_transactions_null_coords": online_count,
        "online_rate_percentage": round(online_rate, 2),
        "unmatched_cities": unmatched_count,
        "unmatched_rate_percentage": round(unmatched_rate, 2),
        "missing_city_or_state": missing_geo_count,
        "invalid_coordinates_detected": invalid_coord_count,
        "entry_method_distribution": dict(entry_method_counter),
        "entry_method_unknown_rate_percentage": round(unknown_rate, 2),
        "top_unmatched_cities": [
            {"city_state": k, "occurrences": v}
            for k, v in unmatched_cities_counter.most_common(10)
        ],
    }
    return report


def main():
    parser = argparse.ArgumentParser(description="Enrich historical transaction data with city coordinates safely.")
    parser.add_argument("--input-file", type=str, default="data/User0_credit_card_transactions.csv", help="Input historical CSV")
    parser.add_argument("--output-file", type=str, default=None, help="Output enriched CSV destination (defaults to enriched_<input>)")
    parser.add_argument("--city-reference", type=str, default="data/city_reference.csv", help="Reference cities CSV file")
    parser.add_argument("--limit", type=int, default=None, help="Optional row processing limit for verification")
    parser.add_argument("--report-json", type=str, default=None, help="Optional path to write JSON summary report")
    parser.add_argument("--preserve-unknown", action="store_true", help="Preserve 'unknown' for transaction_entry_method instead of realistic random assignment")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducible sampling")
    args = parser.parse_args()

    input_path = Path(args.input_file)
    output_path = Path(args.output_file) if args.output_file else input_path.parent / f"enriched_{input_path.name}"
    ref_path = Path(args.city_reference)

    logger.info(f"Enriching historical transactions: {input_path} -> {output_path}")
    report = enrich_historical_dataset(
        input_path,
        output_path,
        ref_path,
        limit=args.limit,
        randomize_entry_method=not args.preserve_unknown,
        seed=args.seed,
    )

    print("\n" + "=" * 70)
    print("HISTORICAL DATA ENRICHMENT QUALITY REPORT")
    print("=" * 70)
    print(f"Input Dataset:                   {report['input_file']}")
    print(f"Output Dataset:                  {report['output_file']}")
    print(f"Total Rows Processed:            {report['total_rows_processed']:,}")
    print(f"Successfully Matched to City:    {report['matched_with_coordinates']:,} ({report['match_rate_percentage']}%)")
    print(f"Online Transactions (null geo):  {report['online_transactions_null_coords']:,} ({report['online_rate_percentage']}%)")
    print(f"Unmatched Cities:                {report['unmatched_cities']:,} ({report['unmatched_rate_percentage']}%)")
    print(f"Missing City/State:              {report['missing_city_or_state']:,}")
    print(f"Invalid Coordinates:             {report['invalid_coordinates_detected']}")
    print(f"Entry Method Unknown Rate:       {report['entry_method_unknown_rate_percentage']}%")
    print(f"Entry Method Distribution:       {report['entry_method_distribution']}")
    print("-" * 70)
    if report["top_unmatched_cities"]:
        print("Top Unmatched Cities:")
        for item in report["top_unmatched_cities"][:5]:
            print(f"  - {item['city_state']}: {item['occurrences']} occurrences")
    print("=" * 70 + "\n")

    if args.report_json:
        with open(args.report_json, "w", encoding="utf-8") as f_rep:
            json.dump(report, f_rep, indent=2)
        logger.info(f"Report saved to {args.report_json}")


if __name__ == "__main__":
    main()
