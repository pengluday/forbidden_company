#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DB_PATH="$ROOT_DIR/data/forbidden_company.db"
ADMIN_PORT="${ADMIN_PORT:-8787}"
PUBLIC_PORT="${PUBLIC_PORT:-8081}"

python3 -m jobs.init_db --db "$DB_PATH" --schema "$ROOT_DIR/db/schema.sql"
python3 -m backend.export_companies_json --db "$DB_PATH" --output "$ROOT_DIR/data/companies.json" --include-pending

python3 -m backend.admin_server --host 127.0.0.1 --port "$ADMIN_PORT" &
ADMIN_PID=$!

python3 -m http.server "$PUBLIC_PORT" --directory "$ROOT_DIR/frontend" &
PUBLIC_PID=$!

cleanup() {
  kill "$ADMIN_PID" "$PUBLIC_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[READY] public:  http://127.0.0.1:${PUBLIC_PORT}/"
echo "[READY] admin:   http://127.0.0.1:${ADMIN_PORT}/admin/"
echo "[READY] ctrl+c to stop"

wait
