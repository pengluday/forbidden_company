#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from backend.company_export import build_company_records


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Export frontend companies.json from SQLite tables.')
    p.add_argument('--db', default='data/forbidden_company.db')
    p.add_argument('--output', default='data/companies.json')
    p.add_argument('--include-pending', action='store_true', help='Include collected rows not yet promoted to verified table.')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    try:
        result = build_company_records(conn, include_pending=args.include_pending)

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

        print(f'[DONE] Exported {len(result)} companies to {output_path}')
    finally:
        conn.close()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
