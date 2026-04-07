#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Initialize SQLite database schema.')
    p.add_argument('--db', default='data/forbidden_company.db')
    p.add_argument('--schema', default='db/schema.sql')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    schema_path = Path(args.schema)

    if not schema_path.exists():
        raise SystemExit(f'Schema not found: {schema_path}')

    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = schema_path.read_text(encoding='utf-8')

    conn = sqlite3.connect(db_path)
    try:
      conn.executescript(schema_sql)
      conn.commit()
    finally:
      conn.close()

    print(f'[DONE] Initialized DB: {db_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
