#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TODAY="$(date +%F)"

python3 -m collectors.collect_liepin \
  --output-csv "$ROOT_DIR/data/source-intake-liepin-$TODAY.csv" \
  --merge-csv "$ROOT_DIR/data/source-intake-round1-jobsites.csv" \
  --db "$ROOT_DIR/data/forbidden_company.db" \
  --collector "scheduled-liepin" \
  --limit 100 \
  --skip-no-evidence
