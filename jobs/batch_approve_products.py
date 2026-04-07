#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Batch approve pending product submissions from CSV.')
    p.add_argument('--db', default='data/forbidden_company.db')
    p.add_argument('--csv', required=True, help='CSV with submission_id,reviewer,note')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f'CSV not found: {csv_path}')

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    approved = 0

    try:
        with csv_path.open('r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                submission_id = (row.get('submission_id') or '').strip()
                reviewer = (row.get('reviewer') or '').strip()
                note = (row.get('note') or '').strip()
                if not submission_id or not reviewer:
                    continue

                pending = conn.execute(
                    '''
                    SELECT id, company_name, product_name, product_category, product_url, source_note
                    FROM pending_product_submissions
                    WHERE id = ? AND review_status = 'pending'
                    ''',
                    (submission_id,),
                ).fetchone()
                if pending is None:
                    continue

                conn.execute(
                    '''
                    INSERT INTO company_products (
                      company_name, product_name, product_category, product_url, confidence, source_note
                    ) VALUES (?, ?, ?, ?, 'verified', ?)
                    ON CONFLICT(company_name, product_name) DO UPDATE SET
                      product_category=excluded.product_category,
                      product_url=excluded.product_url,
                      confidence=excluded.confidence,
                      source_note=excluded.source_note,
                      updated_at=CURRENT_TIMESTAMP
                    ''',
                    (
                        pending['company_name'],
                        pending['product_name'],
                        pending['product_category'],
                        pending['product_url'],
                        pending['source_note'] or note,
                    ),
                )
                conn.execute(
                    '''
                    UPDATE pending_product_submissions
                    SET review_status = 'approved',
                        reviewed_by = ?,
                        reviewed_note = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    ''',
                    (reviewer, note, submission_id),
                )
                approved += 1
        conn.commit()
    finally:
        conn.close()

    print(f'[DONE] Approved {approved} product submissions')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
