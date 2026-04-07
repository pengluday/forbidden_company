#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3


SQL = '''
INSERT INTO company_products (
  company_name, product_name, product_category, product_url, confidence, source_note
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(company_name, product_name) DO UPDATE SET
  product_category=excluded.product_category,
  product_url=excluded.product_url,
  confidence=excluded.confidence,
  source_note=excluded.source_note,
  updated_at=CURRENT_TIMESTAMP
'''


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Upsert company-product mapping.')
    p.add_argument('--db', default='data/forbidden_company.db')
    p.add_argument('--company', required=True)
    p.add_argument('--product', required=True)
    p.add_argument('--category', default='')
    p.add_argument('--url', default='')
    p.add_argument('--confidence', default='unverified', choices=['unverified', 'partial', 'verified'])
    p.add_argument('--note', default='manual input')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        conn.execute(
            SQL,
            (args.company, args.product, args.category, args.url, args.confidence, args.note),
        )
        conn.commit()
    finally:
        conn.close()

    print(f'[DONE] Upserted product mapping: {args.company} -> {args.product}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
