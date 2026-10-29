#!/usr/bin/env python3
"""Database initialization and reference dataset seeding CLI."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

# Ensure src is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.database import create_db_engine, init_db, seed_reference_data


def main():
    parser = argparse.ArgumentParser(description="Initialize and seed reference datasets into relational database")
    parser.add_argument("--database-url", type=str, help="Database connection URL (defaults to DATABASE_URL or config)")
    parser.add_argument("--data-dir", type=str, default="data", help="Path to directory containing reference CSV files")
    parser.add_argument("--recreate", action="store_true", help="Drop and recreate existing reference tables before seeding")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logger = logging.getLogger("init_db")

    config = GeneratorConfig.load()
    db_url = args.database_url or config.database_url
    data_dir = Path(args.data_dir or config.baseline_data_dir)

    logger.info(f"Connecting to database: {config.to_safe_dict().get('database_url')}")
    engine = create_db_engine(db_url)

    logger.info("Initializing schema...")
    init_db(engine)

    logger.info(f"Seeding reference data from {data_dir} (recreate={args.recreate})...")
    counts = seed_reference_data(engine, data_dir=data_dir, recreate=args.recreate)

    logger.info("Database initialization and reference seeding complete!")
    logger.info(f"Records seeded: {counts}")


if __name__ == "__main__":
    main()
