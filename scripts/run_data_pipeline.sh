#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DB_PATH="$ROOT_DIR/data/forbidden_company.db"

python3 -m jobs.init_db --db "$DB_PATH" --schema "$ROOT_DIR/db/schema.sql"
python3 -m jobs.import_csv_to_db --db "$DB_PATH" --csv "$ROOT_DIR/data/source-intake-round1-jobsites.csv"
python3 -m backend.export_companies_json --db "$DB_PATH" --output "$ROOT_DIR/data/companies.json" --include-pending

echo "[DONE] pipeline finished"
