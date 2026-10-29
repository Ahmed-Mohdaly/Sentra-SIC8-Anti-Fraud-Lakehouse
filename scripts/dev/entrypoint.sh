#!/usr/bin/env bash
set -e

# Entrypoint router supporting container roles in Docker and cloud container platforms
case "$1" in
  api|generator-api)
    shift
    exec python /app/scripts/run_generator.py api "$@"
    ;;
  worker|generator-worker)
    shift
    exec python /app/scripts/run_generator.py worker "$@"
    ;;
  all)
    shift
    exec python /app/scripts/run_generator.py all "$@"
    ;;
  init-db|seed-db)
    shift
    exec python /app/scripts/init_db.py "$@"
    ;;
  profile)
    shift
    exec python /app/scripts/profile_data.py "$@"
    ;;
  load-test)
    shift
    exec python /app/scripts/load_test.py "$@"
    ;;
  extractor|ingest)
    shift
    exec python /app/src/ingestion/extractor.py "$@"
    ;;
  batch-reference|reference-extractor)
    shift
    exec python /app/src/ingestion/batch_reference_extractor.py "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
