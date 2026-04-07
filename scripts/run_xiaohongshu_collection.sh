#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TODAY="$(date +%F)"

python3 -m collectors.collect_xiaohongshu \
  --input-urls "$ROOT_DIR/data/xiaohongshu-urls.txt" \
  --output-csv "$ROOT_DIR/data/source-intake-xiaohongshu-$TODAY.csv" \
  --merge-csv "$ROOT_DIR/data/source-intake-round1-jobsites.csv" \
  --db "$ROOT_DIR/data/forbidden_company.db" \
  --collector "scheduled-xiaohongshu" \
  --limit 100 \
  --comment-limit 0 \
  --skip-no-evidence
