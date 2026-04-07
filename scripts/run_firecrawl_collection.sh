#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TODAY="$(date +%F)"

python3 -m collectors.collect_firecrawl \
  --input-urls "$ROOT_DIR/data/firecrawl-urls.txt" \
  --output-csv "$ROOT_DIR/data/source-intake-firecrawl-$TODAY.csv" \
  --merge-csv "$ROOT_DIR/data/source-intake-round1-jobsites.csv" \
  --db "$ROOT_DIR/data/forbidden_company.db" \
  --collector "scheduled-firecrawl" \
  --source-platform "猎聘" \
  --limit 100 \
  --skip-no-evidence
